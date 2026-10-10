// p1.go -- the file you edit for Project 1.
//
// The file has three parts, one for each part of the project.
// Read project1-desc.pdf for what each part must do.
//
// Rules:
//   - Do not change the names of the functions in this file.
//   - Do not change the arguments they take or what they return.
//   - Do not change the fields inside SeqConfig, LocalConfig, or
//     DistConfig.
//   - You may add your own helper functions and types anywhere.
//   - Do not modify sealed.go or yours_test.go.
//
// After you finish a part, you can run it by hand:
//   go run . -mode seq
//   go run . -mode local -n 4
//   go run . -mode worker -addr localhost:9001
//   go run . -mode coord  -workers localhost:9001,localhost:9002

package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	// You will need more packages as you go. Add them here when you need
	// them. Likely: "encoding/gob", "errors", "net", "net/rpc", "sync",
	// "sync/atomic".
)

/* Part 1 -- Sequential SGD (20 points). See project1-desc.pdf, Part 1. */

// SeqConfig holds the inputs to RunSeq. Do not add or remove fields.
type SeqConfig struct {
	Seed      int64
	Steps     int
	BatchSize int
	LR        float64

	// OnStep may be set by the test code. If it is not nil, call it
	// after every step with a fresh copy of the weights.
	OnStep func(step int, w []float64)
}

// RunSeq trains for cfg.Steps steps and returns the final weights.
func RunSeq(cfg SeqConfig) []float64 {
	// Write your code here.
	// See project1-desc.pdf, Part 1 for what this function must do.
	w := Init(cfg.Seed)
	b := NewBatcher(cfg.Seed, cfg.BatchSize)
	for step := 0; step < cfg.Steps; step++ {
		batch := b.Next(step)
		g := Grad(w, batch)
		for i := range w {
			w[i] -= (cfg.LR * g[i]) / float64(cfg.BatchSize)
		}
		if cfg.OnStep != nil {
			copyArr := make([]float64, len(w))
			copy(copyArr, w)
			cfg.OnStep(step, copyArr)
		}
	}

	return w
}

/* Part 2 -- Parallel local trainer (15 points). See project1-desc.pdf, Part 2. */

// LocalConfig holds the inputs to RunLocal. NRanks must divide
// BatchSize. Do not add or remove fields.
type LocalConfig struct {
	Seed      int64
	Steps     int
	BatchSize int
	LR        float64
	NRanks    int
}

// RunLocal trains using NRanks goroutines and returns the final weights.
func RunLocal(cfg LocalConfig) []float64 {
	// Write your code here.
	// See project1-desc.pdf, Part 2. Sum partial gradients in rank order so
	// two runs with the same input return the same result, bit for bit.
	if cfg.NRanks <= 0 {
		panic("NRanks must be positive")
	}

	if cfg.BatchSize%cfg.NRanks != 0 {
		panic("BatchSize must be divisible by NRanks")
	}

	resultChannel := make(chan []float64, 1)
	gradChannels := make([]chan []float64, cfg.NRanks)
	totalChannels := make([]chan []float64, cfg.NRanks)

	for rank := 0; rank < cfg.NRanks; rank++ {
		gradChannels[rank] = make(chan []float64)
		totalChannels[rank] = make(chan []float64)
	}

	var wg sync.WaitGroup
	wg.Add(cfg.NRanks)

	for rank := 0; rank < cfg.NRanks; rank++ {

		go func(r int) {
			defer wg.Done()
			rankW := Init(cfg.Seed)
			b := NewBatcher(cfg.Seed, cfg.BatchSize)

			for step := 0; step < cfg.Steps; step++ {
				batch := b.Next(step)
				sliceSize := cfg.BatchSize / cfg.NRanks
				rankBatch := batch[r*sliceSize : (r+1)*sliceSize]
				partialGrad := Grad(rankW, rankBatch)

				// rank 0 summing and sending back results
				var totalGrad []float64
				if r == 0 {
					totalGrad = make([]float64, len(partialGrad))
					copy(totalGrad, partialGrad)

					for src := 1; src < cfg.NRanks; src++ {
						partial := <-gradChannels[src]
						for i := range totalGrad {
							totalGrad[i] += partial[i]
						}
					}

					for dest := 1; dest < cfg.NRanks; dest++ {
						totalCopy := make([]float64, len(totalGrad))
						copy(totalCopy, totalGrad)
						totalChannels[dest] <- totalCopy
					}
				} else {
					gradChannels[r] <- partialGrad
					totalGrad = <-totalChannels[r]
				}

				for i := range rankW {
					rankW[i] -= (cfg.LR * totalGrad[i]) / float64(cfg.BatchSize)
				}
			}
			if r == 0 {
				resultChannel <- rankW
			}
		}(rank)

	}

	wg.Wait()
	return <-resultChannel
}

/* Part 3 -- Coordinator and workers over RPC (65 points). */

// Worker is the RPC service. You register it under the name "Worker"
// so clients call "Worker.Grad" and "Worker.Ping".
type Worker struct{}

// Grad returns the summed gradient for one slice of a batch.
func (Worker) Grad(args GradArgs, reply *GradReply) error {
	// Write your code here.
	// See project1-desc.pdf, Part 3.1.

	// check for
	if len(args.W) != D {
		return fmt.Errorf("Worker.Grad: len(w) = %d, want %d", len(args.W), D)
	}
	if len(args.Batch) == 0 {
		return fmt.Errorf("Worker.Grad: empty batch")
	}

	reply.Grad = Grad(args.W, args.Batch)
	return nil
}

// Ping is a health check. Leave it as it is.
func (Worker) Ping(_ PingArgs, reply *PingReply) error {
	reply.OK = true
	return nil
}

// RunWorker starts a worker on addr and returns its actual address
// plus a stop function that shuts it down.
func RunWorker(addr string) (actualAddr string, stop func(), err error) {
	// Write your code here.
	// See project1-desc.pdf, Part 3.1.
	server := rpc.NewServer()

	if err := server.RegisterName("Worker", Worker{}); err != nil {
		return "", nil, fmt.Errorf("RunWorker: %w", err)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return "", nil, fmt.Errorf("RunWorker: %w", err)
	}

	var stopOnce sync.Once
	stop = func() {
		stopOnce.Do(func() { _ = listener.Close() })
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()

	return listener.Addr().String(), stop, nil

}

// DistConfig holds the inputs to RunCoordinator. Do not add or remove
// fields.
type DistConfig struct {
	Seed      int64
	Steps     int
	BatchSize int
	LR        float64
	NSlices   int
	Workers   []string
	Timeout   time.Duration
	OnStep    func(step int, w []float64)
}

type workerState struct {
	addr  string
	alive bool
	busy  bool
}

type sliceResult struct {
	workerIndex int
	sliceIndex  int
	grad        []float64
	err         error
}

// RunCoordinator drives training end to end and returns the final
// weights.
func RunCoordinator(cfg DistConfig) []float64 {
	// Write your code here.
	// See project1-desc.pdf, Part 3.2 and Part 3.3.
	// You are free to add helper types and helper functions.
	if cfg.NSlices <= 0 {
		panic("RunCoordinator: invalid Nslices")
	}

	if cfg.BatchSize%cfg.NSlices != 0 {
		panic("RunCoordinator: BatchSize must be divisible by Nslices")
	}
	if len(cfg.Workers) == 0 {
		panic("RunCoordinator: no workers")
	}

	w := Init(cfg.Seed)
	batcher := NewBatcher(cfg.Seed, cfg.BatchSize)
	sliceSize := cfg.BatchSize / cfg.NSlices

	workers := make([]workerState, len(cfg.Workers))
	for i, addr := range cfg.Workers {
		workers[i] = workerState{
			addr:  addr,
			alive: true,
			busy:  false,
		}
	}

	for step := 0; step < cfg.Steps; step++ {
		batch := batcher.Next(step)
		results := make(chan sliceResult, cfg.NSlices)
		partialGrads := make([][]float64, cfg.NSlices)

		launch := func(workerIndex int, sliceIndex int) {
			start := sliceIndex * sliceSize
			end := start + sliceSize

			workerAddr := workers[workerIndex].addr

			weightCopy := make([]float64, len(w))
			copy(weightCopy, w)

			args := GradArgs{
				Step:       step,
				SliceIndex: sliceIndex,
				W:          weightCopy,
				Batch:      batch[start:end],
			}

			workers[workerIndex].busy = true

			go func(workerIndex int, addr string, args GradArgs) {
				client, err := rpc.Dial("tcp", addr)
				if err != nil {
					results <- sliceResult{
						workerIndex: workerIndex,
						sliceIndex:  args.SliceIndex,
						err:         err,
					}
					return
				}
				defer client.Close()

				var reply GradReply
				err = client.Call("Worker.Grad", args, &reply)

				results <- sliceResult{
					workerIndex: workerIndex,
					sliceIndex:  args.SliceIndex,
					grad:        reply.Grad,
					err:         err,
				}
			}(workerIndex, workerAddr, args)
		}

		nextSlice := 0
		retrySlices := make([]int, 0)

		getNextSlice := func() (int, bool) {
			if len(retrySlices) > 0 {
				sliceIndex := retrySlices[0]
				retrySlices = retrySlices[1:]
				return sliceIndex, true
			}

			if nextSlice < cfg.NSlices {
				sliceIndex := nextSlice
				nextSlice++
				return sliceIndex, true
			}
		
			return 0, false
		}

		dispatch := func() {
			for workerIndex := range workers {
				if !workers[workerIndex].alive || workers[workerIndex].busy {
					continue
				}

				sliceIndex, available := getNextSlice()
				if !available {
					return
				}

				launch(workerIndex, sliceIndex)
			}
		}

		dispatch()

		completed := 0
		for completed < cfg.NSlices {
			finalResult := <-results

			workerIndex := finalResult.workerIndex
			workers[workerIndex].busy = false

			if finalResult.err != nil || len(finalResult.grad) != D {
				// temp error
				workers[workerIndex].alive = false
				retrySlices = append(retrySlices, sliceIndex)
			} else {
				partialGrads[finalResult.sliceIndex] = finalResult.grad
				completed++
			}
			
			dispatch()
			

			if nextSlice < cfg.NSlices && workers[workerIndex].alive {
				launch(workerIndex, nextSlice)
				nextSlice++
			}
		}

		fullGrad := make([]float64, D)
		for sliceIndex := 0; sliceIndex < cfg.NSlices; sliceIndex++ {
			for i := range D; {
				fullGrad[i] += partialGrads[sliceIndex][i]
			}
		}

		for i := range w {
			w[i] -= cfg.LR * fullGrad[i] / float64(cfg.BatchSize)
		}

		if cfg.OnStep != nil {
			weightCopy := make([]float64, len(w))
			copy(weightCopy, w)
			cfg.OnStep(step, weightCopy)
		}
	}

	return w
}

/* main -- one binary, four modes. Leave this as it is. */

func main() {
	mode := flag.String("mode", "", "seq | local | worker | coord")
	seed := flag.Int64("seed", 2026, "seed")
	steps := flag.Int("steps", 300, "training steps")
	bs := flag.Int("bs", 64, "batch size")
	lr := flag.Float64("lr", 0.02, "learning rate")
	n := flag.Int("n", 4, "number of ranks (local) or slices (coord)")
	addr := flag.String("addr", "localhost:0", "worker listen address")
	workers := flag.String("workers", "", "comma-separated host:port list (coord)")
	timeout := flag.Duration("timeout", 500*time.Millisecond, "per-attempt timeout")
	flag.Parse()

	switch *mode {
	case "seq":
		eval := NewBatcher(*seed, 1024).Next(0)
		w := RunSeq(SeqConfig{Seed: *seed, Steps: *steps, BatchSize: *bs, LR: *lr})
		fmt.Printf("final loss = %.6f\n", Loss(w, eval))
	case "local":
		eval := NewBatcher(*seed, 1024).Next(0)
		w := RunLocal(LocalConfig{Seed: *seed, Steps: *steps,
			BatchSize: *bs, LR: *lr, NRanks: *n})
		fmt.Printf("final loss = %.6f  (%d ranks)\n", Loss(w, eval), *n)
	case "worker":
		actual, stop, err := RunWorker(*addr)
		if err != nil {
			log.Fatalf("worker start: %v", err)
		}
		fmt.Printf("worker listening on %s\n", actual)
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		stop()
	case "coord":
		if *workers == "" {
			log.Fatal("coord: provide -workers")
		}
		addrs := strings.Split(*workers, ",")
		eval := NewBatcher(*seed, 1024).Next(0)
		w := RunCoordinator(DistConfig{
			Seed: *seed, Steps: *steps, BatchSize: *bs, LR: *lr,
			NSlices: *n, Workers: addrs, Timeout: *timeout,
		})
		fmt.Printf("final loss = %.6f\n", Loss(w, eval))
	default:
		fmt.Fprintln(os.Stderr, "usage: p1 -mode {seq|local|worker|coord} [flags]")
		flag.PrintDefaults()
		os.Exit(2)
	}
}
