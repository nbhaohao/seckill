// 已就位（AI 生成）：集成测试的公共接线——连真实 MySQL + 复位测试数据，不是任何 stage 的教学点。
package testutil

import (
	"os"
	"strconv"
	"testing"

	"github.com/jmoiron/sqlx"

	"github.com/nbhaohao/go-seckill/internal/dbconn"
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envIntOr(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// OpenTestDB 连 docker-compose.yml 里 mysql 服务映射到宿主的端口（可用 TEST_DB_* 覆盖）。
// 连不上就 t.Skip——这是集成测试，前提是 `docker compose up -d mysql` 已经跑起来。
func OpenTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	cfg := dbconn.Config{
		Host:     envOr("TEST_DB_HOST", "127.0.0.1"),
		Port:     envIntOr("TEST_DB_PORT", 3306),
		User:     envOr("TEST_DB_USER", "seckill"),
		Password: envOr("TEST_DB_PASSWORD", "seckill"),
		DBName:   envOr("TEST_DB_NAME", "seckill"),
	}
	db, err := dbconn.Open(cfg)
	if err != nil {
		t.Skipf("skip integration test: cannot connect to MySQL (%v); run `docker compose up -d mysql` first", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("skip integration test: mysql ping failed (%v); run `docker compose up -d mysql` first", err)
	}
	return db
}

// ResetProduct 把一个商品行复位到指定库存，并清空它名下的历史订单，让每个测试都从干净状态起跑。
func ResetProduct(t *testing.T, db *sqlx.DB, id int64, stock int) {
	t.Helper()
	if _, err := db.Exec("DELETE FROM orders WHERE product_id = ?", id); err != nil {
		t.Fatalf("reset: delete orders: %v", err)
	}
	if _, err := db.Exec(
		"INSERT INTO products (id, name, stock) VALUES (?, 'test-product', ?) ON DUPLICATE KEY UPDATE stock = VALUES(stock)",
		id, stock,
	); err != nil {
		t.Fatalf("reset: upsert product: %v", err)
	}
}
