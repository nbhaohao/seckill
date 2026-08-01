package expire

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nbhaohao/go-seckill/internal/overload"
)

func TestM5AP3PercentilesAndCollectorKeepAllSamples(t *testing.T) {
	collector := NewLatencyCollector()
	const workerCount = 2
	const samplesPerWorker = 100
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for sample := 0; sample < samplesPerWorker; sample++ {
				collector.Record(time.Duration(worker*samplesPerWorker+sample+1) * time.Millisecond)
			}
		}(worker)
	}
	wg.Wait()
	samples := collector.Samples()
	if want := workerCount * samplesPerWorker; len(samples) != want {
		t.Fatalf("concurrent collector must keep every sample: workers=%d each=%d want=%d got %d", workerCount, samplesPerWorker, want, len(samples))
	}

	got, err := Percentiles([]time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond, 40 * time.Millisecond}, 0.50, 0.95, 0.99)
	if err != nil {
		t.Fatalf("calculate p50/p95/p99 from four samples: got %v", err)
	}
	want := []time.Duration{20 * time.Millisecond, 40 * time.Millisecond, 40 * time.Millisecond}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("nearest-rank percentiles must match: samples=[10ms 20ms 30ms 40ms] requested=[.50 .95 .99] want=%v got %v", want, got)
	}

	// 分位点写成 95 而不是 0.95 必须报错,不许静默截断成一个看着像样的数字。
	if _, err := Percentiles([]time.Duration{10 * time.Millisecond}, 95); !errors.Is(err, ErrInvalidPercentile) {
		t.Fatalf("超出 (0,1] 的分位点必须被拒绝: requested=95 want=%v got %v", ErrInvalidPercentile, err)
	}
}

func TestM5AP3ScannerStopsWithinShutdownDeadline(t *testing.T) {
	clock := NewFakeClock(time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC))
	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var scans atomic.Int64
	go func() {
		done <- RunScanner(runCtx, clock, 50*time.Millisecond, func(context.Context) error {
			scans.Add(1)
			return nil
		})
	}()

	pollUntil(t, 500*time.Millisecond, func() bool { return clock.PendingWaiters() == 1 }, "scanner to begin its injected wait")
	clock.Advance(50 * time.Millisecond)
	pollUntil(t, 500*time.Millisecond, func() bool { return scans.Load() == 1 }, "scanner to observe one injected tick")
	shutdownCtx, cancelDeadline := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelDeadline()
	report := overload.Shutdown(shutdownCtx, []overload.ShutdownStep{
		ScannerShutdownStep("expire-scanner", cancelRun, done),
	})
	if report.Err != nil || len(report.Completed) != 1 || report.Completed[0] != "expire-scanner" {
		t.Fatalf("scanner must stop inside shared shutdown deadline: deadline=%v scans=%d got report=%+v", 500*time.Millisecond, scans.Load(), report)
	}
}

func pollUntil(t *testing.T, timeout time.Duration, condition func() bool, description string) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out after %v waiting for %s: got condition=false", timeout, description)
		case <-ticker.C:
		}
	}
}
