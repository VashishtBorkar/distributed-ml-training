package main

import (
	"fmt"
	"net"
	"net/rpc"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Use independent RPC servers and ephemeral ports so tests do not share services
// or rely on a startup sleep. Cleanup also closes accepted connections.
func startTestService(t *testing.T, service interface{}) string {
	t.Helper()
	server := rpc.NewServer()
	if err := server.RegisterName("KVServer", service); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var connections []net.Conn
	stopped := false
	t.Cleanup(func() {
		listener.Close()
		mu.Lock()
		defer mu.Unlock()
		stopped = true
		for _, conn := range connections {
			conn.Close()
		}
	})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			if stopped {
				conn.Close()
				mu.Unlock()
				return
			}
			connections = append(connections, conn)
			mu.Unlock()
			go server.ServeConn(conn)
		}
	}()
	return listener.Addr().String()
}

func testClerk(t *testing.T, addr string) *Clerk {
	t.Helper()
	ck := MakeClerk(addr)
	t.Cleanup(func() {
		ck.mu.Lock()
		defer ck.mu.Unlock()
		if ck.client != nil {
			ck.client.Close()
		}
	})
	return ck
}

func readTestKey(t *testing.T, kv *KVServer, key string, xid Xid) string {
	t.Helper()
	var reply GetReply
	if err := kv.Get(GetArgs{Key: key, Xid: xid}, &reply); err != nil || reply.Err != OK {
		t.Fatalf("Get(%q): error=%v reply=%+v", key, err, reply)
	}
	return reply.Value
}

func TestSmokeSingleServer(t *testing.T) {
	kv := MakeKVServer("", "")
	addr := startTestService(t, kv)
	t.Cleanup(kv.Kill)
	ck := testClerk(t, addr)
	if got := ck.Get("missing"); got != "" {
		t.Fatalf("missing key = %q", got)
	}
	ck.Put("hello", "world")
	for i := 0; i < 2; i++ {
		if got := ck.Get("hello"); got != "world" {
			t.Fatalf("Get(hello) = %q", got)
		}
	}
	ck.Put("hello", "updated")
	if got := ck.Get("hello"); got != "updated" {
		t.Fatalf("overwrite = %q", got)
	}
}

func TestDuplicatePutAndGet(t *testing.T) {
	kv := MakeKVServer("", "")
	defer kv.Kill()
	first := PutAppendArgs{Key: "k", Value: "old", Op: OpPut, Xid: 1}
	var reply PutAppendReply
	if err := kv.Put(first, &reply); err != nil || reply.Err != OK {
		t.Fatalf("first Put: %v %+v", err, reply)
	}
	if got := readTestKey(t, kv, "k", 2); got != "old" {
		t.Fatal(got)
	}
	if err := kv.Put(PutAppendArgs{Key: "k", Value: "new", Op: OpPut, Xid: 3}, &reply); err != nil || reply.Err != OK {
		t.Fatalf("second Put: %v %+v", err, reply)
	}
	reply = PutAppendReply{}
	if err := kv.Put(first, &reply); err != nil || reply.Err != OK {
		t.Fatalf("duplicate Put: %v %+v", err, reply)
	}
	if got := readTestKey(t, kv, "k", 2); got != "old" {
		t.Fatalf("retried Get changed reply: %q", got)
	}
	if got := readTestKey(t, kv, "k", 4); got != "new" {
		t.Fatalf("duplicate Put overwrote newer write: %q", got)
	}
}

func TestDuplicateForwardOp(t *testing.T) {
	backup := MakeKVServer("", "")
	defer backup.Kill()
	first := ForwardArgs{Key: "k", Value: "old", Op: OpPut, Xid: 1}
	operations := []ForwardArgs{first, {Key: "k", Value: "new", Op: OpPut, Xid: 2}, first}
	for _, args := range operations {
		var reply ForwardReply
		if err := backup.ForwardOp(args, &reply); err != nil || reply.Err != OK {
			t.Fatalf("ForwardOp(%+v): %v %+v", args, err, reply)
		}
	}
	if got := readTestKey(t, backup, "k", 3); got != "new" {
		t.Fatalf("duplicate forward overwrote newer write: %q", got)
	}
}

func TestPrimaryBackupConcurrentClerks(t *testing.T) {
	backup := MakeKVServer("", "")
	backupAddr := startTestService(t, backup)
	primary := MakeKVServer("", backupAddr)
	primaryAddr := startTestService(t, primary)
	t.Cleanup(backup.Kill)
	t.Cleanup(primary.Kill)
	var wg sync.WaitGroup
	const clients, writes = 4, 20
	for i := 0; i < clients; i++ {
		ck := testClerk(t, primaryAddr)
		wg.Add(1)
		go func(client int) {
			defer wg.Done()
			for j := 0; j < writes; j++ {
				key := fmt.Sprintf("client-%d-key-%d", client, j)
				ck.Put(key, key)
				if got := ck.Get(key); got != key {
					t.Errorf("Get(%q) = %q", key, got)
				}
				// A shared key also checks that both servers agree on write ordering.
				ck.Put("shared", key)
			}
		}(i)
	}
	wg.Wait()
	primary.mu.Lock()
	defer primary.mu.Unlock()
	backup.mu.Lock()
	defer backup.mu.Unlock()
	if len(primary.mp) != clients*writes+1 {
		t.Fatalf("stored %d keys, want %d", len(primary.mp), clients*writes+1)
	}
	if !reflect.DeepEqual(primary.mp, backup.mp) {
		t.Fatal("primary and backup stores differ after concurrent writes")
	}
}

// The first forwarding call can delay or lose its acknowledgement. Later calls
// still reach the real backup, allowing duplicate handling to be exercised.
type controlledTestBackup struct {
	kv        *KVServer
	entered   chan ForwardArgs
	release   chan struct{}
	once      sync.Once
	loseReply bool
}

func (b *controlledTestBackup) ForwardOp(args ForwardArgs, reply *ForwardReply) error {
	first := false
	b.once.Do(func() { first = true })
	if first {
		b.entered <- args
		<-b.release
	}
	err := b.kv.ForwardOp(args, reply)
	if first && b.loseReply {
		return fmt.Errorf("simulated lost acknowledgement after applying write")
	}
	return err
}

func pendingTestPut(kv *KVServer, args PutAppendArgs) <-chan PutAppendReply {
	done := make(chan PutAppendReply, 1)
	go func() {
		var reply PutAppendReply
		if err := kv.Put(args, &reply); err != nil {
			reply.Err = ErrNetwork
		}
		done <- reply
	}()
	return done
}

func TestPutWaitsForBackupAck(t *testing.T) {
	backup := &controlledTestBackup{kv: MakeKVServer("", ""), entered: make(chan ForwardArgs, 1), release: make(chan struct{})}
	primary := MakeKVServer("", startTestService(t, backup))
	t.Cleanup(backup.kv.Kill)
	t.Cleanup(primary.Kill)
	var release sync.Once
	unblock := func() { release.Do(func() { close(backup.release) }) }
	t.Cleanup(unblock)
	args := PutAppendArgs{Key: "k", Value: "v", Op: OpPut, Xid: 1}
	done := pendingTestPut(primary, args)
	select {
	case forwarded := <-backup.entered:
		if forwarded.Xid != args.Xid || forwarded.Key != args.Key || forwarded.Value != args.Value || forwarded.Op != args.Op {
			t.Fatalf("forwarded wrong request: %+v", forwarded)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("primary did not forward write")
	}
	select {
	case reply := <-done:
		t.Fatalf("Put returned before backup acknowledged: %+v", reply)
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	select {
	case reply := <-done:
		if reply.Err != OK {
			t.Fatalf("Put reply: %+v", reply)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Put did not finish after acknowledgement")
	}
	if got := readTestKey(t, backup.kv, "k", 2); got != "v" {
		t.Fatalf("acknowledged write missing on backup: %q", got)
	}
}

func TestPutRetriesLostBackupAck(t *testing.T) {
	backup := &controlledTestBackup{kv: MakeKVServer("", ""), entered: make(chan ForwardArgs, 1), release: make(chan struct{}), loseReply: true}
	close(backup.release)
	primary := MakeKVServer("", startTestService(t, backup))
	t.Cleanup(backup.kv.Kill)
	t.Cleanup(primary.Kill)
	done := pendingTestPut(primary, PutAppendArgs{Key: "k", Value: "v", Op: OpPut, Xid: 10})
	select {
	case reply := <-done:
		if reply.Err != OK {
			t.Fatalf("Put reply: %+v", reply)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("Put did not recover from lost backup acknowledgement")
	}
	if got := readTestKey(t, backup.kv, "k", 11); got != "v" {
		t.Fatalf("backup value = %q", got)
	}
	backup.kv.mu.Lock()
	defer backup.kv.mu.Unlock()
	if len(backup.kv.putReplies) != 1 {
		t.Fatalf("retry used a new Xid: %d cached writes", len(backup.kv.putReplies))
	}
}

func TestKillStopsPendingPut(t *testing.T) {
	backup := &controlledTestBackup{kv: MakeKVServer("", ""), entered: make(chan ForwardArgs, 1), release: make(chan struct{})}
	primary := MakeKVServer("", startTestService(t, backup))
	t.Cleanup(backup.kv.Kill)
	t.Cleanup(primary.Kill)
	t.Cleanup(func() { close(backup.release) })
	done := pendingTestPut(primary, PutAppendArgs{Key: "k", Value: "v", Op: OpPut, Xid: 1})
	select {
	case <-backup.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("primary did not forward write")
	}
	primary.Kill()
	select {
	case reply := <-done:
		if reply.Err != ErrNetwork {
			t.Fatalf("killed primary returned %+v", reply)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Kill did not stop pending Put")
	}
}

func TestKilledServerRejectsOperations(t *testing.T) {
	kv := MakeKVServer("", "")
	kv.Kill()
	var get GetReply
	var put PutAppendReply
	var forward ForwardReply
	if err := kv.Get(GetArgs{Key: "k", Xid: 1}, &get); err != nil || get.Err != ErrNetwork {
		t.Fatalf("Get: %v %+v", err, get)
	}
	if err := kv.Put(PutAppendArgs{Key: "k", Value: "v", Xid: 2}, &put); err != nil || put.Err != ErrNetwork {
		t.Fatalf("Put: %v %+v", err, put)
	}
	if err := kv.ForwardOp(ForwardArgs{Key: "k", Value: "v", Xid: 3}, &forward); err != nil || forward.Err != ErrNetwork {
		t.Fatalf("ForwardOp: %v %+v", err, forward)
	}
}
