// 已就位（AI 生成）：连上 Redis 是样板，不是 m02 的教学点（教学点在 internal/cache 怎么用它做
// cache-aside、空值缓存、TTL 抖动与并发合并）。用法照 go-redis v9 README。
package redisconn

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config 是连 Redis 需要的最小字段集合，来自环境变量。
type Config struct {
	Addr     string
	Password string
	DB       int
}

// Open 建客户端并立刻 Ping 一次，保证返回的 client 是真的能用的。
// go-redis 的每个命令都要求传 context——这正是 COURSE_SPEC「连接治理纪律」要求的
// 「请求 ctx 带 deadline 并传到 Redis 的 Context API」，业务代码里不许用 context.Background()。
func Open(ctx context.Context, cfg Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
		// 这几个超时同样是「默认值不要指望」的例子：go-redis 的默认 Dial/Read/WriteTimeout
		// 不是无限（分别 5s/3s/3s），但显式写出来才能在压测报告里说清楚上限是多少。
		DialTimeout:  3 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		PoolSize:     20,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redisconn: ping: %w", err)
	}
	return client, nil
}
