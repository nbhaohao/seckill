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

var claimDueScript = redis.NewScript(`
local key = KEYS[1]
local now = tonumber(ARGV[1])
local batchSize = tonumber(ARGV[2])
local newScore = tonumber(ARGV[3])

local due = redis.call('ZRANGE', key, '-inf', now, 'BYSCORE', 'LIMIT', 0, batchSize)
for _, member in ipairs(due) do
	redis.call('ZADD', key, newScore, member)
end
return due
`)

// ClaimDue is sk-m5a p1 S2. AI 将在 p1 学习时分切片实现。
//  1. 一段 Lua 在同一次原子执行中取出最多 batchSize 个到期 member。
//  2. 领取的 member score 一律推后 claimExtension，两个 worker 才不会同时领到它。
func ClaimDue(ctx context.Context, rdb *redis.Client, clock Clock, batchSize int, claimExtension time.Duration) ([]string, error) {
	now := clock.Now()
	newScore := now.Add(claimExtension).UnixMilli()
	return claimDueScript.Run(ctx, rdb, []string{ExpireZSetKey}, now.UnixMilli(), batchSize, newScore).StringSlice()
}
