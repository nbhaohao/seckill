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
	panic("TODO: phase p2") // AI 将在 p2 S1 按上面的 why 边界实现。
}

// Allow is m05 p2 S2. AI will implement it during p2.
func (b *TokenBucket) Allow() bool {
	panic("TODO: phase p2") // AI 将在 p2 S2 按上面的 why 边界实现。
}

// Tokens is m05 p2 S2. AI will implement it during p2.
func (b *TokenBucket) Tokens() float64 {
	panic("TODO: phase p2") // AI 将在 p2 S2 按上面的 why 边界实现。
}
