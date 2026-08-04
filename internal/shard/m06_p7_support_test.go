// 已就位（AI 生成）：p7 集成测试在现有 compose MySQL 内准备四个 schema，不新增容器。
// 放在 shard_test 包而不是 internal/testutil：testutil 被 internal/order 的测试引用，
// 而 shard 依赖 order，helper 若落在 testutil 会形成 order(test) -> testutil -> shard -> order 的导入环。
package shard_test

import (
	"os"
	"strconv"
	"testing"

	"github.com/jmoiron/sqlx"

	"github.com/nbhaohao/go-seckill/internal/dbconn"
	"github.com/nbhaohao/go-seckill/internal/shard"
)

// openOrderShards 用 root 账号幂等准备测试 schema，再用业务账号返回四个真实连接池。
func openOrderShards(t *testing.T) [shard.ShardCount]*sqlx.DB {
	t.Helper()
	adminCfg := dbconn.Config{
		Host:     envOr("TEST_DB_HOST", "127.0.0.1"),
		Port:     envIntOr("TEST_DB_PORT", 3306),
		User:     envOr("TEST_DB_ADMIN_USER", "root"),
		Password: envOr("TEST_DB_ADMIN_PASSWORD", "root"),
		DBName:   "mysql",
	}
	admin, err := dbconn.Open(adminCfg)
	if err != nil {
		t.Skipf("skip p7 integration test: cannot connect to MySQL admin (%v); run `docker compose up -d mysql` first", err)
	}
	defer admin.Close()

	appUser := envOr("TEST_DB_USER", "seckill")
	for i := 0; i < shard.ShardCount; i++ {
		schema := shard.SchemaName(i)
		if _, err := admin.Exec("CREATE DATABASE IF NOT EXISTS " + schema + " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
			t.Fatalf("prepare shard schema=%s: %v", schema, err)
		}
		if _, err := admin.Exec("GRANT ALL PRIVILEGES ON " + schema + ".* TO '" + appUser + "'@'%'"); err != nil {
			t.Fatalf("grant shard schema=%s user=%s: %v", schema, appUser, err)
		}
		if _, err := admin.Exec(`CREATE TABLE IF NOT EXISTS ` + schema + `.orders (
id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
product_id BIGINT UNSIGNED NOT NULL,
user_id BIGINT UNSIGNED NOT NULL,
request_id VARCHAR(64) NOT NULL,
quantity INT NOT NULL,
status VARCHAR(16) NOT NULL DEFAULT 'created',
created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
UNIQUE KEY uk_request_id (request_id),
KEY idx_product_id (product_id),
KEY idx_created_id (created_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`); err != nil {
			t.Fatalf("prepare shard table schema=%s: %v", schema, err)
		}
	}

	base := dbconn.Config{
		Host:     envOr("TEST_DB_HOST", "127.0.0.1"),
		Port:     envIntOr("TEST_DB_PORT", 3306),
		User:     appUser,
		Password: envOr("TEST_DB_PASSWORD", "seckill"),
	}
	pools, err := shard.OpenPools(base)
	if err != nil {
		t.Fatalf("open p7 shard pools: %v", err)
	}
	t.Cleanup(func() { shard.ClosePools(pools) })
	return pools
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return value
}
