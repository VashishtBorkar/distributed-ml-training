/*
Your test file.  Nothing here is graded.  Write your own tests as you
go.  Delete or change the examples below.

Useful commands:

    go test .                      run every test in this folder
    go test -run TestSmoke .       run only tests whose name has TestSmoke
    go test -v .                   print each test name as it runs
    go test -race .                catch data races (needed for Part 2)

A good habit: when you find a bug, add a test that would have caught
it.  Then you never chase the same bug twice.
*/

package main

import (
	"net"
	"net/rpc"
	"testing"
	"time"
)

/*
A smoke test.  Starts a single KVServer (no backup), writes a key,
reads it back, and checks the value.  Passes once Part 1 is done.
*/
func TestSmokeSingleServer(t *testing.T) {
	addr := "127.0.0.1:21001"

	kv := MakeKVServer(addr, "")
	defer kv.Kill()

	srv := rpc.NewServer()
	if err := srv.Register(kv); err != nil {
		t.Fatalf("register: %v", err)
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	go srv.Accept(lis)

	time.Sleep(100 * time.Millisecond)

	ck := MakeClerk(addr)
	ck.Put("hello", "world")
	if got := ck.Get("hello"); got != "world" {
		t.Fatalf("Get(hello) = %q, want %q", got, "world")
	}
}

/*
Template for Part 2.  Uncomment and fill in once ForwardOp works.

func TestSmokePrimaryBackup(t *testing.T) {
    backupAddr  := "127.0.0.1:21011"
    primaryAddr := "127.0.0.1:21012"

    backup := MakeKVServer(backupAddr, "")
    ... register, listen ...

    primary := MakeKVServer(primaryAddr, backupAddr)
    ... register, listen ...

    ck := MakeClerk(primaryAddr)
    ck.Put("k", "v")
    // Reach into the backup directly and check its state matches.
}
*/
