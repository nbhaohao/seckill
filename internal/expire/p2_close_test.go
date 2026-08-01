package expire

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/deduct"
	"github.com/nbhaohao/go-seckill/internal/testutil"
)

func TestM5AP2PaidOrderIsNeverClosedOrRestocked(t *testing.T) {
	db, rdb := openM5AEnv(t)
	const productID int64 = 10521
	const orderID int64 = 10521001
	const stockAfterPreDeduct = 7
	testutil.ResetProduct(t, db, productID, 10)
	insertExpiryOrder(t, db, orderID, productID, 1, StatusPaid)
	setRedisStock(t, rdb, productID, stockAfterPreDeduct)

	result, err := CloseCreatedOrder(context.Background(), db, orderID)
	if err != nil {
		t.Fatalf("close paid order id=%d: got %v", orderID, err)
	}
	if err := RestoreClosedStock(context.Background(), rdb, result); err != nil {
		t.Fatalf("restore decision for paid order id=%d result=%+v: got %v", orderID, result, err)
	}
	if got := orderStatus(t, db, orderID); got != StatusPaid {
		t.Fatalf("paid order must remain paid: order_id=%d want=%q got %q result=%+v", orderID, StatusPaid, got, result)
	}
	if got := redisStock(t, rdb, productID); result.RowsAffected != 0 || got != stockAfterPreDeduct {
		t.Fatalf("zero changed rows must not restore stock: order_id=%d rows=%d stock_before=%d got %d", orderID, result.RowsAffected, stockAfterPreDeduct, got)
	}
}

func TestM5AP2ClaimedThenRestartedClosesAndRestoresExactlyOnce(t *testing.T) {
	db, rdb := openM5AEnv(t)
	ctx := context.Background()
	const productID int64 = 10522
	const orderID int64 = 10522001
	const quantityToRestore = 2
	const stockAfterPreDeduct = 8
	testutil.ResetProduct(t, db, productID, 10)
	insertExpiryOrder(t, db, orderID, productID, quantityToRestore, StatusCreated)
	setRedisStock(t, rdb, productID, stockAfterPreDeduct)
	clock := NewFakeClock(time.Date(2026, 8, 1, 11, 0, 0, 0, time.UTC))
	if err := IndexOrder(ctx, rdb, fmt.Sprint(orderID), clock.Now().Add(-time.Second)); err != nil {
		t.Fatalf("index restart-test order id=%d: got %v", orderID, err)
	}
	claimExtension := 50 * time.Millisecond
	firstClaim, err := ClaimDue(ctx, rdb, clock, 1, claimExtension)
	if err != nil {
		t.Fatalf("first claim order id=%d: got %v", orderID, err)
	}
	// Simulate termination after claiming: no close, restore, or index removal occurs.
	clock.Advance(claimExtension + time.Millisecond)
	restartedClaim, err := ClaimDue(ctx, rdb, clock, 1, claimExtension)
	if err != nil {
		t.Fatalf("restarted claim order id=%d: got %v", orderID, err)
	}

	firstResult, err := CloseCreatedOrder(ctx, db, orderID)
	if err != nil {
		t.Fatalf("restarted close order id=%d: got %v", orderID, err)
	}
	if err := RestoreClosedStock(ctx, rdb, firstResult); err != nil {
		t.Fatalf("first restore order id=%d result=%+v: got %v", orderID, firstResult, err)
	}
	if err := RemoveProcessedExpiry(ctx, rdb, orderID); err != nil {
		t.Fatalf("remove processed expiry order id=%d: got %v", orderID, err)
	}
	// 移除必须真的发生:否则这张订单会在下一个可见性超时后被再领一次,
	// 而那时它已经是 closed,扫描器会白跑一轮却看不出任何异常。
	if _, err := rdb.ZScore(ctx, ExpireZSetKey, fmt.Sprint(orderID)).Result(); !errors.Is(err, redis.Nil) {
		remaining, _ := rdb.ZRange(ctx, ExpireZSetKey, 0, -1).Result()
		t.Fatalf("处理完成的订单必须离开到期索引: order_id=%d want=redis.Nil got err=%v remaining_members=%v", orderID, err, remaining)
	}
	duplicateResult, err := CloseCreatedOrder(ctx, db, orderID)
	if err != nil {
		t.Fatalf("duplicate close order id=%d: got %v", orderID, err)
	}
	if err := RestoreClosedStock(ctx, rdb, duplicateResult); err != nil {
		t.Fatalf("duplicate restore order id=%d result=%+v: got %v", orderID, duplicateResult, err)
	}

	stockAfter := redisStock(t, rdb, productID)
	t.Logf("claimed orderIDs before stop=%v after restart=%v; RowsAffected=[%d %d]; Redis stock before=%d after=%d", firstClaim, restartedClaim, firstResult.RowsAffected, duplicateResult.RowsAffected, stockAfterPreDeduct, stockAfter)
	if len(firstClaim) != 1 || len(restartedClaim) != 1 || firstClaim[0] != fmt.Sprint(orderID) || restartedClaim[0] != fmt.Sprint(orderID) {
		t.Fatalf("restart must reclaim the same order: order_id=%d first=%v restarted=%v", orderID, firstClaim, restartedClaim)
	}
	if got := orderStatus(t, db, orderID); got != StatusClosed || firstResult.RowsAffected != 1 || duplicateResult.RowsAffected != 0 {
		t.Fatalf("order must close exactly once: order_id=%d want_status=%q got %q rows=[%d %d]", orderID, StatusClosed, got, firstResult.RowsAffected, duplicateResult.RowsAffected)
	}
	if want := stockAfterPreDeduct + quantityToRestore; stockAfter != want {
		t.Fatalf("stock must be restored exactly once: product_id=%d before=%d quantity=%d want=%d got %d", productID, stockAfterPreDeduct, quantityToRestore, want, stockAfter)
	}
}

func openM5AEnv(t *testing.T) (*sqlx.DB, *redis.Client) {
	t.Helper()
	db := testutil.OpenTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	rdb := testutil.OpenTestRedis(t)
	testutil.DeleteKeys(t, rdb, ExpireZSetKey)
	return db, rdb
}

func insertExpiryOrder(t *testing.T, db *sqlx.DB, orderID, productID int64, quantity int, status string) {
	t.Helper()
	_, err := db.Exec("INSERT INTO orders (id, product_id, user_id, request_id, quantity, status) VALUES (?, ?, 42, ?, ?, ?)", orderID, productID, fmt.Sprintf("m5a-%d", orderID), quantity, status)
	if err != nil {
		t.Fatalf("insert expiry order id=%d status=%q: got %v", orderID, status, err)
	}
}

func setRedisStock(t *testing.T, rdb *redis.Client, productID int64, stock int) {
	t.Helper()
	if err := rdb.Set(context.Background(), deduct.StockKey(productID), stock, 0).Err(); err != nil {
		t.Fatalf("set Redis stock product_id=%d stock=%d: got %v", productID, stock, err)
	}
	t.Cleanup(func() { _ = rdb.Del(context.Background(), deduct.StockKey(productID)).Err() })
}

func redisStock(t *testing.T, rdb *redis.Client, productID int64) int {
	t.Helper()
	got, err := rdb.Get(context.Background(), deduct.StockKey(productID)).Int()
	if err != nil {
		t.Fatalf("read Redis stock product_id=%d: got %v", productID, err)
	}
	return got
}

func orderStatus(t *testing.T, db *sqlx.DB, orderID int64) string {
	t.Helper()
	var got string
	if err := db.Get(&got, "SELECT status FROM orders WHERE id = ?", orderID); err != nil {
		t.Fatalf("read order status id=%d: got %v", orderID, err)
	}
	return got
}
