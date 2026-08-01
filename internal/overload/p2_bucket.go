package overload

import (
	"sync"
	"time"
)

// TokenBucket is m05 p2. AI will implement it in two slices during p2.
//  1. Splitting construction (fixed capacity, refill rate, clock) from the
//     mutable token count keeps configuration and state independent; a nil
//     clock defaulting to time.Now keeps production callers simple while
//     tests still inject a fake one.
//  2. Refilling on every call — instead of a background ticker — means Allow
//     has no goroutine to leak and no drift between wall time and a scheduler
//     tick. The mutex is required because Allow is called concurrently from
//     many request goroutines and they must all see one consistent count,
//     not race on it.
type TokenBucket struct {
	mu              sync.Mutex
	capacity        float64
	refillPerSecond float64
	tokens          float64
	last            time.Time
	now             func() time.Time
}

// NewTokenBucket is m05 p2 S1. AI will implement it during p2.
func NewTokenBucket(capacity int, refillPerSecond float64, now func() time.Time) *TokenBucket {
	if now == nil {
		now = time.Now
	}
	return &TokenBucket{
		capacity:        float64(capacity),
		refillPerSecond: refillPerSecond,
		tokens:          float64(capacity),
		last:            now(),
		now:             now,
	}
}

// Allow is m05 p2 S2. AI will implement it during p2.
func (b *TokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	b.tokens += now.Sub(b.last).Seconds() * b.refillPerSecond
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.last = now

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// Tokens is m05 p2 S2. AI will implement it during p2.
func (b *TokenBucket) Tokens() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	tokens := b.tokens + b.now().Sub(b.last).Seconds()*b.refillPerSecond
	if tokens > b.capacity {
		tokens = b.capacity
	}
	return tokens
}
