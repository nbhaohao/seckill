package expire

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// IndexOrder is sk-m5a p1 S1. AI 将在 p1 学习时分切片实现。
//  1. 到期时刻写进 score，orderID 写进 member；索引只表达何时可领取。
func IndexOrder(ctx context.Context, rdb *redis.Client, orderID string, expiresAt time.Time) error {
	return rdb.ZAdd(ctx, ExpireZSetKey, redis.Z{
		Score:  float64(expiresAt.UnixMilli()),
		Member: orderID,
	}).Err()
}

// ClaimDue is sk-m5a p1 S2. AI 将在 p1 学习时分切片实现。
//  1. 一段 Lua 在同一次原子执行中取出最多 batchSize 个到期 member。
//  2. 领取的 member score 一律推后 claimExtension，两个 worker 才不会同时领到它。
func ClaimDue(ctx context.Context, rdb *redis.Client, clock Clock, batchSize int, claimExtension time.Duration) ([]string, error) {
	// TODO(sk-m5a-p1-s2): atomically ZRANGE BYSCORE LIMIT and push claimed scores forward.
	panic("TODO: phase p1 claim due orders")
}
