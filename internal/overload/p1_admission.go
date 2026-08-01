package overload

import (
	"context"
	"sync"
	"time"
)

// Admission is m05 p1. AI will implement it in two slices during p1.
//  1. Bounded concurrency slots cap how much work the system has promised to
//     do at once; that is a separate door from the rate limiter in p2.
//     Failing fast within a wait budget when the door is full beats letting a
//     request queue for unbounded resources and dragging p99 down with it.
//  2. A slot must be returned on every exit path — success, failure, or ctx
//     cancellation. Leaking one slot permanently shrinks capacity by one,
//     with no visible cause besides a slowly rising InFlight.
type Admission struct {
	slots      chan struct{}
	waitBudget time.Duration

	mu        sync.Mutex
	capacity  int
	inFlight  int
	accepted  int64
	rejected  int64
	waitTotal time.Duration
}

// NewAdmission is m05 p1 S1. AI will implement it during p1.
func NewAdmission(capacity int, waitBudget time.Duration) *Admission {
	return &Admission{
		slots:      make(chan struct{}, capacity),
		waitBudget: waitBudget,
		capacity:   capacity,
	}
}

// Acquire is m05 p1 S1. AI will implement it during p1.
func (a *Admission) Acquire(ctx context.Context) error {
	select {
	case a.slots <- struct{}{}:
		a.mu.Lock()
		a.inFlight++
		a.accepted++
		a.mu.Unlock()
		return nil
	case <-time.After(a.waitBudget):
		a.mu.Lock()
		a.rejected++
		a.mu.Unlock()
		return ErrAdmissionFull
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release is m05 p1 S2. AI will implement it during p1.
func (a *Admission) Release() {
	panic("TODO: phase p1") // AI 将在 p1 S2 按上面的 why 边界实现。
}

// Stats is m05 p1 S2. AI will implement it during p1.
func (a *Admission) Stats() AdmissionStats {
	panic("TODO: phase p1") // AI 将在 p1 S2 按上面的 why 边界实现。
}
