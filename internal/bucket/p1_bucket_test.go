package bucket

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/order"
	"github.com/nbhaohao/go-seckill/internal/testutil"
)

func TestM5BP1BucketDeductNoOversellAndClearsEveryBucket(t *testing.T) {
	rdb := openBucketRedis(t)
	ctx := context.Background()
	const productID int64 = 10561
	cfg := Config{BucketCount: 4, MaxProbes: 3} // 1+3 = 4 = N：探桶额度足够绕完一圈
	const perBucket = 5
	totalStock := perBucket * cfg.BucketCount
	resetBuckets(t, rdb, productID, cfg, perBucket)

	const attempts = 60
	var mu sync.Mutex
	hitsPerBucket := map[int]int{}
	successes, insufficient := 0, 0
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := DeductWithProbe(ctx, rdb, cfg, productID, fmt.Sprintf("m5b-p1-%03d", i), 1)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
				hitsPerBucket[res.Bucket]++
			case errors.Is(err, order.ErrInsufficientStock):
				insufficient++
			default:
				t.Errorf("并发扣减不该出现意外错误: request=%d got %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	final := readBuckets(t, rdb, productID, cfg)
	var remaining int64
	for _, v := range final {
		remaining += v
	}
	t.Logf("N=%d k=%d 初始每桶=%d 总量=%d；并发 %d 发 → 成功=%d 不足=%d；命中分布=%v 各桶剩余=%v",
		cfg.BucketCount, cfg.MaxProbes, perBucket, totalStock, attempts, successes, insufficient, hitsPerBucket, final)

	// 零超卖 + 不遗漏：卖出的件数必须等于铺进去的总量，一件不多一件不少。
	if successes != totalStock || int64(successes) != int64(totalStock)-remaining {
		t.Fatalf("分桶扣减必须卖光且不超卖: total=%d successes=%d remaining=%d buckets=%v", totalStock, successes, remaining, final)
	}
	if successes+insufficient != attempts {
		t.Fatalf("每一发请求都必须有明确归宿: attempts=%d successes=%d insufficient=%d", attempts, successes, insufficient)
	}
	for b, v := range final {
		if v < 0 {
			t.Fatalf("任何一个桶都不许被扣成负数: bucket=%d got %d buckets=%v", b, v, final)
		}
	}
}

func TestM5BP1ProbeStopsAtExplicitBound(t *testing.T) {
	rdb := openBucketRedis(t)
	ctx := context.Background()
	const productID int64 = 10562
	cfg := Config{BucketCount: 8, MaxProbes: 2}
	const requestID = "m5b-p1-bound"
	deleteBuckets(t, rdb, productID, cfg)

	start := StartBucket(requestID, cfg.BucketCount)
	// 起始桶和它后面 k 个桶全空；货全压在探测窗口之外的那个桶里。
	for i := 0; i <= cfg.MaxProbes; i++ {
		setBucket(t, rdb, productID, (start+i)%cfg.BucketCount, 0)
	}
	const farOffset = 5 // > MaxProbes，必须落在窗口外
	farBucket := (start + farOffset) % cfg.BucketCount
	const farStock = 100
	setBucket(t, rdb, productID, farBucket, farStock)

	res, err := DeductWithProbe(ctx, rdb, cfg, productID, requestID, 1)
	after := readBuckets(t, rdb, productID, cfg)
	t.Logf("start=%d k=%d 窗口=%v 远桶=%d(存货 %d)；结果 probes=%d bucket=%d err=%v 各桶=%v",
		start, cfg.MaxProbes, probeWindow(start, cfg), farBucket, farStock, res.Probes, res.Bucket, err, after)

	if !errors.Is(err, order.ErrInsufficientStock) {
		t.Fatalf("窗口内全空时必须直接判不足,不许继续往下探: start=%d far_bucket=%d want=%v got err=%v res=%+v", start, farBucket, order.ErrInsufficientStock, err, res)
	}
	// 上界是这一关的全部意义:没有它,库存见底时每一发请求都会把 N 个桶挨个访问一遍。
	if want := 1 + cfg.MaxProbes; res.Probes != want {
		t.Fatalf("访问桶数必须停在显式上界: N=%d k=%d want_probes=%d got %d", cfg.BucketCount, cfg.MaxProbes, want, res.Probes)
	}
	if res.Bucket != -1 {
		t.Fatalf("没扣成时不许报出一个桶号(调用方会拿它去回滚): got bucket=%d res=%+v", res.Bucket, res)
	}
	if after[farBucket] != farStock {
		t.Fatalf("窗口外的桶一次都不该被碰: far_bucket=%d want=%d got %d 各桶=%v", farBucket, farStock, after[farBucket], after)
	}
}

func TestM5BP1RollbackReturnsToProbedBucketNotStartBucket(t *testing.T) {
	rdb := openBucketRedis(t)
	ctx := context.Background()
	const productID int64 = 10563
	cfg := Config{BucketCount: 8, MaxProbes: 3}
	const requestID = "m5b-p1-rollback"
	const quantity = 2
	const hitStock = 5
	deleteBuckets(t, rdb, productID, cfg)

	start := StartBucket(requestID, cfg.BucketCount)
	setBucket(t, rdb, productID, start, 0)
	setBucket(t, rdb, productID, (start+1)%cfg.BucketCount, 0)
	hitBucket := (start + 2) % cfg.BucketCount
	setBucket(t, rdb, productID, hitBucket, hitStock)

	res, err := DeductWithProbe(ctx, rdb, cfg, productID, requestID, quantity)
	if err != nil {
		t.Fatalf("探到第 3 个桶应当扣成: start=%d hit_bucket=%d got %v", start, hitBucket, err)
	}
	if res.Bucket != hitBucket {
		t.Fatalf("命中桶号必须是真正扣走库存的那个: start=%d want=%d got %d res=%+v", start, hitBucket, res.Bucket, res)
	}
	deducted := readBuckets(t, rdb, productID, cfg)

	if err := RollbackToBucket(ctx, rdb, productID, res.Bucket, quantity); err != nil {
		t.Fatalf("回滚 bucket=%d qty=%d: got %v", res.Bucket, quantity, err)
	}
	restored := readBuckets(t, rdb, productID, cfg)
	t.Logf("start=%d 命中桶=%d qty=%d；扣后=%v 回滚后=%v", start, hitBucket, quantity, deducted, restored)

	if restored[hitBucket] != hitStock {
		t.Fatalf("必须还回真正扣走的那个桶: bucket=%d want=%d got %d 各桶=%v", hitBucket, hitStock, restored[hitBucket], restored)
	}
	if restored[start] != 0 {
		t.Fatalf("起始桶没被扣过就不许凭空多货(总量对了但分布错了最难查): start=%d want=0 got %d 各桶=%v", start, restored[start], restored)
	}

	// 没命中桶时的取值 -1：这是「压根没扣过」，回滚必须是空操作而不是往桶 -1 或桶 0 加货。
	if err := RollbackToBucket(ctx, rdb, productID, -1, quantity); err != nil {
		t.Fatalf("bucket=-1 的回滚必须是空操作: got %v", err)
	}
	afterNoop := readBuckets(t, rdb, productID, cfg)
	if afterNoop[0] != restored[0] || afterNoop[hitBucket] != restored[hitBucket] {
		t.Fatalf("空操作回滚不许改动任何桶: before=%v after=%v", restored, afterNoop)
	}
}

// ── 测试辅助（已就位）──────────────────────────────────────────────────────

func probeWindow(start int, cfg Config) []int {
	window := make([]int, 0, cfg.MaxProbes+1)
	for i := 0; i <= cfg.MaxProbes; i++ {
		window = append(window, (start+i)%cfg.BucketCount)
	}
	return window
}

func openBucketRedis(t *testing.T) *redis.Client {
	t.Helper()
	return testutil.OpenTestRedis(t)
}

func bucketKeys(productID int64, cfg Config) []string {
	keys := make([]string, cfg.BucketCount)
	for b := 0; b < cfg.BucketCount; b++ {
		keys[b] = StockKey(productID, b)
	}
	return keys
}

func deleteBuckets(t *testing.T, rdb *redis.Client, productID int64, cfg Config) {
	t.Helper()
	testutil.DeleteKeys(t, rdb, bucketKeys(productID, cfg)...)
	t.Cleanup(func() { _ = rdb.Del(context.Background(), bucketKeys(productID, cfg)...).Err() })
}

func setBucket(t *testing.T, rdb *redis.Client, productID int64, bucket int, value int) {
	t.Helper()
	if err := rdb.Set(context.Background(), StockKey(productID, bucket), value, 0).Err(); err != nil {
		t.Fatalf("set bucket product_id=%d bucket=%d value=%d: got %v", productID, bucket, value, err)
	}
}

func resetBuckets(t *testing.T, rdb *redis.Client, productID int64, cfg Config, perBucket int) {
	t.Helper()
	deleteBuckets(t, rdb, productID, cfg)
	for b := 0; b < cfg.BucketCount; b++ {
		setBucket(t, rdb, productID, b, perBucket)
	}
}

func readBuckets(t *testing.T, rdb *redis.Client, productID int64, cfg Config) []int64 {
	t.Helper()
	values := make([]int64, cfg.BucketCount)
	for b := 0; b < cfg.BucketCount; b++ {
		n, err := rdb.Get(context.Background(), StockKey(productID, b)).Int64()
		if errors.Is(err, redis.Nil) {
			values[b] = 0
			continue
		}
		if err != nil {
			t.Fatalf("read bucket product_id=%d bucket=%d: got %v", productID, b, err)
		}
		values[b] = n
	}
	return values
}
