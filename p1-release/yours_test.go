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

import "testing"

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

// Template for Part 3. Uncomment when RunWorker works. Starts one
// worker and shuts it down.

// func TestExampleWorker(t *testing.T) {
// 	addr, stop, err := RunWorker("localhost:0")
// 	if err != nil {
// 		t.Fatalf("RunWorker: %v", err)
// 	}
// 	defer stop()
// 	if addr == "" {
// 		t.Fatal("RunWorker returned an empty address")
// 	}
// }
