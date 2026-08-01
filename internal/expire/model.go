// 已就位（AI 生成）：sk-m5a 三个 phase 共用的契约、可注入时钟与测试样板。
package expire

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/nbhaohao/go-seckill/internal/overload"
)

const ExpireZSetKey = "seckill:order:expire"

const (
	StatusCreated = "created"
	StatusPaid    = "paid"
	StatusClosed  = "closed"
)

// ErrInvalidPercentile 是 p3 唯一的入参哨兵：分位点必须落在 (0,1]。
// 它存在的理由是 Percentiles 的 error 返回值必须有人负责——分位点写成 95 而不是 0.95
// 是这类统计代码最常见的手滑，静默按 nearest-rank 截断会给出一个看着像样的错数字。
var ErrInvalidPercentile = errors.New("expire: percentile must be in (0, 1]")

// Clock keeps wall time out of expiry decisions and scanner tests.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time                         { return time.Now() }
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type fakeWaiter struct {
	at time.Time
	ch chan time.Time
}

// FakeClock is a deterministic clock. Advance releases every due waiter.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []fakeWaiter
}

func NewFakeClock(start time.Time) *FakeClock { return &FakeClock{now: start} }

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.waiters = append(c.waiters, fakeWaiter{at: c.now.Add(d), ch: ch})
	return ch
}

func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	remaining := c.waiters[:0]
	for _, waiter := range c.waiters {
		if waiter.at.After(c.now) {
			remaining = append(remaining, waiter)
			continue
		}
		waiter.ch <- c.now
		close(waiter.ch)
	}
	c.waiters = remaining
	c.mu.Unlock()
}

func (c *FakeClock) PendingWaiters() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.waiters)
}

// CloseResult carries the changed-row claim and the exact stock compensation.
type CloseResult struct {
	OrderID      int64
	ProductID    int64
	Quantity     int
	RowsAffected int64
}

// LatencyCollector owns sample storage; Record is the p3 learning boundary.
type LatencyCollector struct {
	mu      sync.Mutex
	samples []time.Duration
}

func NewLatencyCollector() *LatencyCollector { return &LatencyCollector{} }

func (c *LatencyCollector) Samples() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := append([]time.Duration(nil), c.samples...)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

// ScannerShutdownStep adapts a running scanner to m05's ordered shutdown.
func ScannerShutdownStep(name string, cancel context.CancelFunc, done <-chan error) overload.ShutdownStep {
	return overload.ShutdownStep{
		Name: name,
		Fn: func(ctx context.Context) error {
			cancel()
			select {
			case err := <-done:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
}
