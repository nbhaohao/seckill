package overload

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestM05P2BurstThenRefillsCappedAtCapacity(t *testing.T) {
	now, advance := FakeClock(time.Unix(0, 0))
	bucket := NewTokenBucket(5, 10, now)

	allowed := 0
	for i := 0; i < 10; i++ {
		if !bucket.Allow() {
			break
		}
		allowed++
	}
	if allowed != 5 {
		t.Fatalf("expected the burst to exhaust exactly capacity=5 tokens, allowed=%d", allowed)
	}
	if bucket.Allow() {
		t.Fatalf("bucket should be empty immediately after the burst, but Allow returned true")
	}
	t.Logf("burst: allowed=%d then exhausted; tokens=%.2f", allowed, bucket.Tokens())

	advance(300 * time.Millisecond)
	tokens := bucket.Tokens()
	if tokens < 2.95 || tokens > 3.05 {
		t.Fatalf("expected ~3 tokens after 300ms at 10 tokens/sec, got %.4f", tokens)
	}
	t.Logf("300ms after the burst at refill=10/s: tokens=%.4f", tokens)

	advance(10 * time.Second)
	tokens = bucket.Tokens()
	if tokens != 5 {
		t.Fatalf("expected tokens capped at capacity=5 after a long idle period, got %.4f", tokens)
	}
	t.Logf("after a 10s idle period: tokens=%.4f (capped at capacity, not overflowed)", tokens)
}

func TestM05P2AllowIsConcurrencySafe(t *testing.T) {
	now, _ := FakeClock(time.Unix(0, 0)) // frozen clock: no refill happens mid-test
	bucket := NewTokenBucket(100, 10, now)

	var wg sync.WaitGroup
	var allowedCount int64
	const goroutines = 200
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if bucket.Allow() {
				atomic.AddInt64(&allowedCount, 1)
			}
		}()
	}
	wg.Wait()

	if allowedCount != 100 {
		t.Fatalf("expected exactly capacity=100 admitted under a frozen clock with %d racers, got %d", goroutines, allowedCount)
	}
	t.Logf("%d concurrent Allow() calls against a frozen capacity=100 bucket: allowed=%d", goroutines, allowedCount)
}
