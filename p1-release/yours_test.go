// Your test file. Nothing here is graded. Write your own tests as you
// go. Delete or change the examples below.
//
// Useful commands:
//
//	go test .                    // run every test in this folder
//	go test -run TestSeq .       // run only tests whose name has TestSeq
//	go test -v .                 // print each test name as it runs
//	go test -race .              // catch data races (needed for Parts 2 and 3)
//
// A good habit: when you find a bug, add a test that would have
// caught it. Then you never chase the same bug twice.
package main

import (
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"
)

// A smoke test. When RunSeq is implemented, this passes: the returned
// slice has the right length. It does not check that the model
// actually trains.
func TestExampleSeq(t *testing.T) {
	cfg := SeqConfig{Seed: 1, Steps: 5, BatchSize: 32, LR: 0.01}
	w := RunSeq(cfg)
	if len(w) != D {
		t.Fatalf("RunSeq returned %d weights, want %d", len(w), D)
	}
}

func TestSeqValues(t *testing.T) {
	cfg := SeqConfig{Seed: 1, Steps: 5, BatchSize: 32, LR: 0.01}
	x_w := RunSeq(cfg)
	y_w := RunSeq(cfg)

	for i := range x_w {
		if x_w[i] != y_w[i] {
			t.Fatalf("RunSeq returned different weights: x_w[%d]=%v, y_w[%d]=%v", i, x_w[i], i, y_w[i])
		}
	}
}

func TestSeqLoss(t *testing.T) {
	evalBatch := NewBatcher(1, 1024).Next(0)
	initialLoss := Loss(Init(1), evalBatch)

	cfg := SeqConfig{
		Seed:      1,
		Steps:     300,
		BatchSize: 64,
		LR:        0.01,
	}
	w := RunSeq(cfg)
	finalLoss := Loss(w, evalBatch)

	if finalLoss > initialLoss/50 {
		t.Fatalf(
			"loss went from %v to %v; want at least 50x reduction",
			initialLoss,
			finalLoss,
		)
	}
}

// Template for Part 2. Uncomment when RunLocal works.

func TestExampleLocal(t *testing.T) {
	cfg := LocalConfig{Seed: 1, Steps: 5, BatchSize: 32, LR: 0.01, NRanks: 4}
	w := RunLocal(cfg)
	if len(w) != D {
		t.Fatalf("RunLocal returned %d weights, want %d", len(w), D)
	}
}

func TestLocalValues(t *testing.T) {
	cfg := LocalConfig{Seed: 1, Steps: 5, BatchSize: 32, LR: 0.01, NRanks: 4}
	x_w := RunLocal(cfg)
	y_w := RunLocal(cfg)

	for i := range x_w {
		if x_w[i] != y_w[i] {
			t.Fatalf("RunLocal returned different weights: x_w[%d]=%v, y_w[%d]=%v", i, x_w[i], i, y_w[i])
		}
	}
}

func TestLocalDiffRanks(t *testing.T) {
	for n := 1; n <= 8; n *= 2 {
		cfg := LocalConfig{Seed: 1, Steps: 5, BatchSize: 32, LR: 0.01, NRanks: n}
		x_w := RunLocal(cfg)
		y_w := RunLocal(cfg)

		for i := range x_w {
			if x_w[i] != y_w[i] {
				t.Fatalf("RunLocal returned different weights: x_w[%d]=%v, y_w[%d]=%v", i, x_w[i], i, y_w[i])
			}
		}
	}

}

func TestLocalLoss(t *testing.T) {
	const seed = int64(1)

	evalBatch := NewBatcher(seed, 1024).Next(0)
	initialLoss := Loss(Init(seed), evalBatch)

	for _, ranks := range []int{1, 2, 4, 8} {
		cfg := LocalConfig{
			Seed:      seed,
			Steps:     300,
			BatchSize: 64,
			LR:        0.02,
			NRanks:    ranks,
		}

		w := RunLocal(cfg)
		finalLoss := Loss(w, evalBatch)

		if finalLoss >= initialLoss {
			t.Errorf(
				"%d ranks: loss did not decrease: initial=%v final=%v",
				ranks,
				initialLoss,
				finalLoss,
			)
		}
	}
}

func referenceLocal(cfg LocalConfig) []float64 {
	w := Init(cfg.Seed)
	batcher := NewBatcher(cfg.Seed, cfg.BatchSize)
	sliceSize := cfg.BatchSize / cfg.NRanks

	for step := 0; step < cfg.Steps; step++ {
		batch := batcher.Next(step)
		fullGrad := make([]float64, D)

		for rank := 0; rank < cfg.NRanks; rank++ {
			start := rank * sliceSize
			end := start + sliceSize
			partial := Grad(w, batch[start:end])

			for i := 0; i < D; i++ {
				fullGrad[i] += partial[i]
			}
		}

		for i := range w {
			w[i] -= cfg.LR * fullGrad[i] / float64(cfg.BatchSize)
		}
	}

	return w
}

func TestLocalReferenceRank4(t *testing.T) {
	cfg := LocalConfig{
		Seed:      1,
		Steps:     50,
		BatchSize: 64,
		LR:        0.02,
		NRanks:    4,
	}

	got := RunLocal(cfg)
	want := referenceLocal(cfg)

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf(
				"weight[%d] = %v, want %v", i, got[i], want[i],
			)
		}
	}
}

// Template for Part 3. Uncomment when RunWorker works. Starts one
// worker and shuts it down.

func TestExampleWorker(t *testing.T) {
	addr, stop, err := RunWorker("localhost:0")
	if err != nil {
		t.Fatalf("RunWorker: %v", err)
	}
	defer stop()
	if addr == "" {
		t.Fatal("RunWorker returned an empty address")
	}
}

func startTestWorkers(t *testing.T, count int) []string {
	t.Helper()

	addresses := make([]string, count)

	for i := 0; i < count; i++ {
		addr, stop, err := RunWorker("localhost:0")
		if err != nil {
			t.Fatalf("start worker %d: %v", i, err)
		}

		addresses[i] = addr
		t.Cleanup(stop)
	}

	return addresses
}

func requireSameWeights(t *testing.T, got, want []float64) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d weights, want %d", len(got), len(want))
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf(
				"weight[%d] = %v, want %v",
				i,
				got[i],
				want[i],
			)
		}
	}
}

func TestCoordinatorHealthy(t *testing.T) {
	const seed = int64(1)

	workers := startTestWorkers(t, 4)

	cfg := DistConfig{
		Seed:      seed,
		Steps:     300,
		BatchSize: 64,
		LR:        0.02,
		NSlices:   4,
		Workers:   workers,
		Timeout:   500 * time.Millisecond,
	}

	first := RunCoordinator(cfg)
	second := RunCoordinator(cfg)

	// Section 4: two runs must agree bit-for-bit.
	requireSameWeights(t, second, first)

	// Section 4: loss must drop by at least 20x.
	evalBatch := NewBatcher(seed, 1024).Next(0)
	initialLoss := Loss(Init(seed), evalBatch)
	finalLoss := Loss(first, evalBatch)

	if finalLoss > initialLoss/20 {
		t.Fatalf(
			"loss went from %v to %v; want at least 20x reduction",
			initialLoss,
			finalLoss,
		)
	}

	// Stronger check: distributed and local slice-ordered reductions
	// should produce identical weights.
	want := RunLocal(LocalConfig{
		Seed:      cfg.Seed,
		Steps:     cfg.Steps,
		BatchSize: cfg.BatchSize,
		LR:        cfg.LR,
		NRanks:    cfg.NSlices,
	})
	requireSameWeights(t, first, want)
}

func startControllableTestWorkers(t *testing.T, count int) ([]string, []func()) {
	t.Helper()

	addresses := make([]string, count)
	stops := make([]func(), count)
	for i := 0; i < count; i++ {
		addr, stop, err := RunWorker("localhost:0")
		if err != nil {
			t.Fatalf("start worker %d: %v", i, err)
		}
		addresses[i] = addr
		stops[i] = stop
		t.Cleanup(stop)
	}
	return addresses, stops
}

func TestCoordinatorCrashMatchesHealthy(t *testing.T) {
	addresses, stops := startControllableTestWorkers(t, 3)
	cfg := DistConfig{
		Seed:      9,
		Steps:     60,
		BatchSize: 64,
		LR:        0.02,
		NSlices:   4,
		Workers:   addresses,
		Timeout:   100 * time.Millisecond,
	}
	cfg.OnStep = func(step int, _ []float64) {
		if step == 5 {
			stops[0]()
		}
	}

	got := RunCoordinator(cfg)
	want := referenceLocal(LocalConfig{
		Seed:      cfg.Seed,
		Steps:     cfg.Steps,
		BatchSize: cfg.BatchSize,
		LR:        cfg.LR,
		NRanks:    cfg.NSlices,
	})
	requireSameWeights(t, got, want)
}

type slowTestWorker struct {
	delay time.Duration
}

func (w slowTestWorker) Grad(args GradArgs, reply *GradReply) error {
	time.Sleep(w.delay)
	return (Worker{}).Grad(args, reply)
}

func (slowTestWorker) Ping(_ PingArgs, reply *PingReply) error {
	reply.OK = true
	return nil
}

func startSlowTestWorker(t *testing.T, delay time.Duration) string {
	t.Helper()

	server := rpc.NewServer()
	if err := server.RegisterName("Worker", slowTestWorker{delay: delay}); err != nil {
		t.Fatalf("register slow worker: %v", err)
	}
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen for slow worker: %v", err)
	}

	var stopOnce sync.Once
	t.Cleanup(func() {
		stopOnce.Do(func() { _ = listener.Close() })
	})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()

	return listener.Addr().String()
}

func TestCoordinatorStragglerMatchesHealthy(t *testing.T) {
	slowAddress := startSlowTestWorker(t, 250*time.Millisecond)
	fastAddresses := startTestWorkers(t, 2)
	cfg := DistConfig{
		Seed:      11,
		Steps:     80,
		BatchSize: 64,
		LR:        0.02,
		NSlices:   4,
		Workers:   append([]string{slowAddress}, fastAddresses...),
		Timeout:   50 * time.Millisecond,
	}

	got := RunCoordinator(cfg)
	want := referenceLocal(LocalConfig{
		Seed:      cfg.Seed,
		Steps:     cfg.Steps,
		BatchSize: cfg.BatchSize,
		LR:        cfg.LR,
		NRanks:    cfg.NSlices,
	})
	requireSameWeights(t, got, want)
}
