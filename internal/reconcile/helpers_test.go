package reconcile

import (
	"context"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/deduct"
	"github.com/nbhaohao/go-seckill/internal/testutil"
)

// openM5CEnv 连真实 MySQL + Redis；任一连不上就 skip（集成测试的既有约定）。
func openM5CEnv(t *testing.T) (*sqlx.DB, *redis.Client) {
	t.Helper()
	return testutil.OpenTestDB(t), testutil.OpenTestRedis(t)
}

// resetLedger 清掉本包写过的全部台账。测试环境里用 KEYS 无所谓——
// 生产路径上为什么必须换成 SCAN，是 p2 S2 的教学点。
func resetLedger(t *testing.T, rdb *redis.Client) {
	t.Helper()
	ctx := context.Background()
	keys, err := rdb.Keys(ctx, LedgerPrefix+"*").Result()
	if err != nil {
		t.Fatalf("list ledger keys: %v", err)
	}
	if len(keys) == 0 {
		return
	}
	if err := rdb.Del(ctx, keys...).Err(); err != nil {
		t.Fatalf("delete ledger keys: %v", err)
	}
}

func setRedisStock(t *testing.T, rdb *redis.Client, productID int64, stock int64) {
	t.Helper()
	if err := rdb.Set(context.Background(), deduct.StockKey(productID), stock, 0).Err(); err != nil {
		t.Fatalf("seed redis stock product=%d: %v", productID, err)
	}
}

func redisStock(t *testing.T, rdb *redis.Client, productID int64) int64 {
	t.Helper()
	got, err := rdb.Get(context.Background(), deduct.StockKey(productID)).Int64()
	if err != nil {
		t.Fatalf("read redis stock product=%d: %v", productID, err)
	}
	return got
}

// landOrder 模拟 m04 的消费者把一条 order.created 落进 DB。
// 直接 INSERT 而不是走 order.PlaceOrderTx，是因为本模块关心的只有
// 「DB 里到底有没有这一单」，不想把消费者的库存扣减语义搅进恒等式。
func landOrder(t *testing.T, db *sqlx.DB, orderID, productID int64, requestID string, quantity int) {
	t.Helper()
	_, err := db.Exec(
		"INSERT INTO orders (id, product_id, user_id, request_id, quantity, status) VALUES (?, ?, ?, ?, ?, 'created')",
		orderID, productID, 7001, requestID, quantity)
	if err != nil {
		t.Fatalf("land order request_id=%s: %v", requestID, err)
	}
}

// ledgerState 读一条台账当前的状态字段；台账不存在时返回空串。
func ledgerState(t *testing.T, rdb *redis.Client, requestID string) string {
	t.Helper()
	raw, err := rdb.Get(context.Background(), LedgerKey(requestID)).Result()
	if err == redis.Nil {
		return ""
	}
	if err != nil {
		t.Fatalf("read ledger request_id=%s: %v", requestID, err)
	}
	entry, err := DecodeEntry(raw)
	if err != nil {
		t.Fatalf("decode ledger request_id=%s: %v", requestID, err)
	}
	return entry.State
}
