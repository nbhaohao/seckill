package bucket

import (
	"context"
	"errors"
	"testing"

	"github.com/nbhaohao/go-seckill/internal/order"
	"github.com/nbhaohao/go-seckill/internal/testutil"
)

func TestM5BP2SpreadStockSplitsRemainderAcrossLeadingBucketsAndIsRepeatable(t *testing.T) {
	rdb := openBucketRedis(t)
	db := testutil.OpenTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	const productID int64 = 10564
	const totalStock = 23 // 故意除不尽 8，余数怎么分是这一关的考点
	cfg := Config{BucketCount: 8, MaxProbes: 2}
	testutil.ResetProduct(t, db, productID, totalStock)
	deleteBuckets(t, rdb, productID, cfg)

	planned, err := SpreadStock(ctx, rdb, db, productID, cfg)
	if err != nil {
		t.Fatalf("spread stock product_id=%d total=%d N=%d: got %v", productID, totalStock, cfg.BucketCount, err)
	}
	inRedis := readBuckets(t, rdb, productID, cfg)
	t.Logf("total=%d N=%d → 计划=%v Redis 实际=%v", totalStock, cfg.BucketCount, planned, inRedis)

	want := []int64{3, 3, 3, 3, 3, 3, 3, 2} // 23 = 8×2 + 7，前 7 个桶各多 1
	if len(planned) != cfg.BucketCount {
		t.Fatalf("返回值必须逐桶给出计划量: N=%d got len=%d planned=%v", cfg.BucketCount, len(planned), planned)
	}
	var sum int64
	for b := range want {
		sum += planned[b]
		if planned[b] != want[b] || inRedis[b] != want[b] {
			t.Fatalf("余数必须一件一件摊给前几个桶,不许整块塞给某一个桶: bucket=%d want=%d planned=%d in_redis=%d 全量 want=%v planned=%v redis=%v",
				b, want[b], planned[b], inRedis[b], want, planned, inRedis)
		}
	}
	if sum != totalStock {
		t.Fatalf("拆桶不许凭空增减库存: total=%d sum(planned)=%d planned=%v", totalStock, sum, planned)
	}

	// 铺桶写的是绝对值,不是累加:重复铺一次必须回到同一个状态。
	// 写成 INCRBY 的话这里会变成两倍,而压测每一轮开头都要铺一次。
	if _, err := SpreadStock(ctx, rdb, db, productID, cfg); err != nil {
		t.Fatalf("second spread product_id=%d: got %v", productID, err)
	}
	again := readBuckets(t, rdb, productID, cfg)
	if len(again) != len(inRedis) {
		t.Fatalf("重复铺桶后桶数变了: first=%v second=%v", inRedis, again)
	}
	for b := range again {
		if again[b] != inRedis[b] {
			t.Fatalf("重复铺桶必须幂等: bucket=%d first=%d second=%d first_all=%v second_all=%v", b, inRedis[b], again[b], inRedis, again)
		}
	}
}

func TestM5BP2FragmentedBucketsRejectOrderWhileStockRemains(t *testing.T) {
	rdb := openBucketRedis(t)
	ctx := context.Background()
	const productID int64 = 10565
	// MaxProbes = N-1：探桶额度给到能绕完整整一圈，排除「是不是探得不够远」这个解释。
	cfg := Config{BucketCount: 8, MaxProbes: 7}
	const quantity = 2
	deleteBuckets(t, rdb, productID, cfg)

	// 卖到尾声的真实形态：总共还剩 5 件，但摊在 5 个桶里，每桶只剩 1 件。
	scattered := []int{1, 0, 1, 0, 1, 1, 0, 1}
	for b, v := range scattered {
		setBucket(t, rdb, productID, b, v)
	}

	total, perBucket, err := TotalRemaining(ctx, rdb, productID, cfg)
	if err != nil {
		t.Fatalf("total remaining product_id=%d: got %v", productID, err)
	}
	res, deductErr := DeductWithProbe(ctx, rdb, cfg, productID, "m5b-p2-fragment", quantity)
	after, afterPerBucket, err := TotalRemaining(ctx, rdb, productID, cfg)
	if err != nil {
		t.Fatalf("total remaining after attempt product_id=%d: got %v", productID, err)
	}
	t.Logf("N=%d k=%d 各桶=%v 总剩余=%d；请求 %d 件 → probes=%d bucket=%d err=%v；事后各桶=%v 总剩余=%d",
		cfg.BucketCount, cfg.MaxProbes, perBucket, total, quantity, res.Probes, res.Bucket, deductErr, afterPerBucket, after)

	if total != 5 {
		t.Fatalf("场景前提没摆对: want_total=5 got %d 各桶=%v", total, perBucket)
	}
	// 这就是分桶主动付出的代价:总量 5 ≥ 要买的 2,但没有任何一个桶单独凑得出 2 件。
	// 单 key 版在同样的剩余量下这一单是能成的——差别不来自并发,只来自库存被切开了。
	if !errors.Is(deductErr, order.ErrInsufficientStock) {
		t.Fatalf("碎片场景必须给出明确失败,不许伪造成受理成功: total=%d quantity=%d 各桶=%v got err=%v res=%+v", total, quantity, perBucket, deductErr, res)
	}
	if res.Probes != cfg.BucketCount {
		t.Fatalf("探遍全环仍应失败,以此排除「探得不够远」这个解释: N=%d want_probes=%d got %d", cfg.BucketCount, cfg.BucketCount, res.Probes)
	}
	if after != total {
		t.Fatalf("失败的请求不许扣走任何一件: before=%d after=%d before_buckets=%v after_buckets=%v", total, after, perBucket, afterPerBucket)
	}
}
