package bucket

import (
	"context"
	"errors"
	"hash/fnv"

	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/order"
)

// ── sk-m5b p1 · 分桶扣减 + 探桶上界 ⭐（本模块课眼）─────────────────────────
//
// 下面三个函数由 AI 在 p1 学习时分三个切片实现，每写一段停下来问一句再往下。
// 现在是红态：签名与注释已锁死，函数体是可定位的 panic。
//
// 这一关只回答两个问题：一发请求该打哪个桶？打不中时最多再试几个桶？
// 「桶里的库存一开始是怎么放进去的」「拆完之后总量还对不对」不在本关，别提前掺进来。

// StartBucket 是 p1 S1：把一个 requestID 映射到它的起始桶号，返回值必须落在 [0, bucketCount)。
//
// 为什么是 requestID 而不是随机数：同一个 requestID 重试时必须落回同一个桶，
// 否则「这单到底扣了哪个桶」这个问题在重试之后就没有确定答案了，补偿也就无从谈起。
// 为什么不能用 Go 的 map 迭代顺序 / time.Now() 之类：它们每次都变，选桶必须是纯函数。
//
// bucketCount <= 0 时返回 0（退化成单桶，等价于 m03 的单 key 行为，不 panic）。
func StartBucket(requestID string, bucketCount int) int {
	if bucketCount <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(requestID))
	return int(h.Sum32() % uint32(bucketCount))
}

// DeductWithProbe 是 p1 S2：从起始桶开始扣 qty，扣不到就往后探，探桶次数有显式上界。
//
// 要实现的形状：
//  1. start := StartBucket(requestID, cfg.BucketCount)
//  2. 依次尝试 start、start+1、…（对 cfg.BucketCount 取模绕回来），
//     一共最多访问 1 + cfg.MaxProbes 个桶——起始桶那一次不算「再探」
//  3. 某个桶用 deductOneBucket 扣成功，就带着**那个桶号**和已访问桶数返回
//  4. 额度用完仍然扣不到，返回 order.ErrInsufficientStock，
//     并且 DeductResult.Bucket = -1（没有命中桶，就不能谎报一个桶号出去，
//     否则调用方拿着它去回滚，会把库存加到一个根本没扣过的桶上）
//
// 上界为什么必须显式：见 Config.MaxProbes 的注释。这里绝不允许写成
// 「for i := 0; i < cfg.BucketCount; i++」这种「探到有货为止」的循环。
func DeductWithProbe(ctx context.Context, rdb *redis.Client, cfg Config, productID int64, requestID string, qty int) (DeductResult, error) {
	start := StartBucket(requestID, cfg.BucketCount)
	budget := 1 + cfg.MaxProbes
	for i := 0; i < budget; i++ {
		bucket := (start + i) % cfg.BucketCount
		remaining, err := deductOneBucket(ctx, rdb, productID, bucket, qty)
		if err == nil {
			return DeductResult{Bucket: bucket, Remaining: remaining, Probes: i + 1}, nil
		}
		if !errors.Is(err, order.ErrInsufficientStock) {
			return DeductResult{Bucket: -1, Probes: i + 1}, err
		}
	}
	return DeductResult{Bucket: -1, Probes: budget}, order.ErrInsufficientStock
}

// RollbackToBucket 是 p1 S3：把 qty 还回**指定的那一个**桶（跑 rollbackBucketScript）。
//
// 签名里为什么要显式传 bucket，而不是在函数内部重新算一次 StartBucket：
// 扣减真正命中的桶可能是探出来的，跟起始桶不是同一个。重新算一次会把库存
// 加到起始桶上，于是总量没错、分布错了——这种账目最难查，因为恒等式还是成立的。
//
// bucket < 0（DeductWithProbe 失败时的取值）视为「压根没扣过」，直接返回 nil，不发命令。
func RollbackToBucket(ctx context.Context, rdb *redis.Client, productID int64, bucket int, qty int) error {
	if bucket < 0 {
		return nil
	}
	return rollbackBucketScript.Run(ctx, rdb, []string{StockKey(productID, bucket)}, qty).Err()
}
