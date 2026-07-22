package idgen

import (
	"strings"
	"sync"
	"testing"
)

// m01 · p2 时间戳(41)+节点号(10)+序列号(12) 三段式 ID + 时钟回拨拒绝

func TestM01P2NextIDUniqueAndMonotonic(t *testing.T) {
	n, err := NewNode(7)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	seen := make(map[int64]bool)
	var last int64
	for i := 0; i < 5000; i++ {
		id, err := n.NextID()
		if err != nil {
			t.Fatalf("NextID: %v", err)
		}
		if seen[id] {
			t.Fatalf("duplicate id %d at iteration %d", id, i)
		}
		seen[id] = true
		if id <= last {
			t.Fatalf("id not monotonic: prev=%d cur=%d", last, id)
		}
		last = id
	}
}

func TestM01P2NextIDConcurrentUnique(t *testing.T) {
	n, err := NewNode(3)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	const goroutines = 20
	const perGoroutine = 500
	ids := make(chan int64, goroutines*perGoroutine)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				id, err := n.NextID()
				if err != nil {
					t.Errorf("NextID: %v", err)
					return
				}
				ids <- id
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := make(map[int64]bool, goroutines*perGoroutine)
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate id %d under concurrency", id)
		}
		seen[id] = true
	}
}

func TestM01P2NextIDClockRollbackRejected(t *testing.T) {
	n, err := NewNode(1)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	tick := int64(1_800_000_000_000) // 任意固定毫秒时间戳，具体值不重要
	n.nowFunc = func() int64 { return tick }
	if _, err := n.NextID(); err != nil {
		t.Fatalf("first NextID should succeed: %v", err)
	}
	n.nowFunc = func() int64 { return tick - 50 } // 模拟时钟回拨 50ms
	_, err = n.NextID()
	if err == nil {
		t.Fatal("expected error when clock moves backwards, got nil")
	}
	if !strings.Contains(err.Error(), "clock moved backwards") {
		t.Fatalf("error should mention clock rollback, got: %v", err)
	}
}

func TestM01P2NewNodeRejectsOutOfRangeID(t *testing.T) {
	if _, err := NewNode(-1); err == nil {
		t.Fatal("expected error for negative nodeID")
	}
	if _, err := NewNode(maxNodeID + 1); err == nil {
		t.Fatal("expected error for nodeID beyond 10-bit range")
	}
}
