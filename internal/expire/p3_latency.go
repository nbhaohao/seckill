package expire

import (
	"context"
	"math"
	"sort"
	"time"
)

// Record is sk-m5a p3 S1. AI 将在 p3 学习时分切片实现。
//  1. 并发 worker 上报时必须保护样本集合，观测不能悄悄少记。
func (c *LatencyCollector) Record(sample time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples = append(c.samples, sample)
}

// Percentiles is sk-m5a p3 S2. AI 将在 p3 学习时分切片实现。
//  1. 对快照排序后用 nearest-rank 计算请求的分位点；空样本返回空结果。
func Percentiles(samples []time.Duration, requested ...float64) ([]time.Duration, error) {
	for _, p := range requested {
		if p <= 0 || p > 1 {
			return nil, ErrInvalidPercentile
		}
	}
	if len(samples) == 0 {
		return []time.Duration{}, nil
	}

	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	result := make([]time.Duration, len(requested))
	for i, p := range requested {
		rank := int(math.Ceil(p * float64(len(sorted))))
		result[i] = sorted[rank-1]
	}
	return result, nil
}

// RunScanner is sk-m5a p3 S3. AI 将在 p3 学习时分切片实现。
//  1. 每个 scanInterval 触发一次有界 scan，clock 让等待在测试中可控。
//  2. ctx 取消必须抢占等待并退出，才能接入 overload.Shutdown 的总 deadline。
func RunScanner(ctx context.Context, clock Clock, scanInterval time.Duration, scan func(context.Context) error) error {
	// TODO(sk-m5a-p3-s3): wait for injected ticks, scan, and stop promptly on ctx cancellation.
	panic("TODO: phase p3 run expiry scanner")
}
