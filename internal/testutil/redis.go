// 已就位（AI 生成）：m02 集成测试的 Redis 接线——连真实 Redis + 清干净测试用的 key。
package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/redisconn"
)

// OpenTestRedis 连 docker-compose.yml 里 redis 服务映射到宿主的端口（可用 TEST_REDIS_ADDR 覆盖）。
// 连不上就 t.Skip——这是集成测试，前提是 `docker compose up -d mysql redis` 已经跑起来。
func OpenTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := redisconn.Open(ctx, redisconn.Config{
		Addr:     envOr("TEST_REDIS_ADDR", "127.0.0.1:6379"),
		Password: envOr("TEST_REDIS_PASSWORD", ""),
		DB:       envIntOr("TEST_REDIS_DB", 0),
	})
	if err != nil {
		t.Skipf("skip integration test: cannot connect to Redis (%v); run `docker compose up -d redis` first", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// DeleteKeys 删掉给定 key（不存在也不报错），让每个测试从"缓存里什么都没有"的状态起跑。
// 故意不用 FLUSHDB：同一个 Redis 里可能还有别的东西，测试只清自己造的那几个 key。
func DeleteKeys(t *testing.T, rdb *redis.Client, keys ...string) {
	t.Helper()
	if len(keys) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Del(ctx, keys...).Err(); err != nil {
		t.Fatalf("delete keys %v: %v", keys, err)
	}
}
