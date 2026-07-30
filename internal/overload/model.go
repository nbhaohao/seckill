// Package overload holds m05's admission control, rate limiting, circuit
// breaking, and graceful shutdown primitives for the seckill entry path.
//
// 已就位（AI 生成）：本文件是类型 / 错误 / 可观察结构的样板，不是教学点；
// FakeClock 同样是样板，p1-p4 的红测试与之后的参考实现共用它，不是某个 phase 的答案。
package overload

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrAdmissionFull   = errors.New("overload: admission slots exhausted")
	ErrRateLimited     = errors.New("overload: rate limited")
	ErrBreakerOpen     = errors.New("overload: circuit breaker open")
	ErrShutdownTimeout = errors.New("overload: shutdown deadline exceeded")
)

// AdmissionStats is p1's observable snapshot: how many concurrent slots exist,
// how many are in use right now, and the running accept/reject/wait totals.
type AdmissionStats struct {
	Capacity  int
	InFlight  int
	Accepted  int64
	Rejected  int64
	WaitTotal time.Duration
}

// BreakerState is p3's three-state circuit breaker state machine.
type BreakerState int

const (
	StateClosed BreakerState = iota
	StateOpen
	StateHalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// BreakerPolicy configures p3's breaker: how many consecutive failures trip
// it, how long it stays cold before probing, how many concurrent probes the
// half-open state allows, and which clock it reads (nil falls back to
// time.Now so production callers do not have to think about it).
type BreakerPolicy struct {
	FailureThreshold int
	OpenFor          time.Duration
	HalfOpenProbes   int
	Now              func() time.Time
}

// DegradedPayload is p3's degraded response body. Degraded=true may only
// ever appear on a read-path response; the write path has no such thing as a
// degraded success, so its Degraded field must stay false.
type DegradedPayload struct {
	Error      string `json:"error,omitempty"`
	Degraded   bool   `json:"degraded,omitempty"`
	RetryAfter int    `json:"retry_after,omitempty"` // seconds
}

// ShutdownStep is one named step in p4's graceful shutdown sequence.
type ShutdownStep struct {
	Name string
	Fn   func(context.Context) error
}

// ShutdownReport is p4's observable outcome of Shutdown: which steps actually
// completed (in the order they completed), which step failed or never got a
// chance to start, the error behind that, and the total wall time spent.
type ShutdownReport struct {
	Completed []string
	Failed    string
	Err       error
	Elapsed   time.Duration
}

// FakeClock returns a mutex-protected, injectable clock: now reads the
// current instant, advance moves it forward. p1-p4 tests use it so time-based
// assertions never depend on real wall-clock precision.
func FakeClock(start time.Time) (now func() time.Time, advance func(time.Duration)) {
	var mu sync.Mutex
	current := start
	now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return current
	}
	advance = func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		current = current.Add(d)
	}
	return now, advance
}
