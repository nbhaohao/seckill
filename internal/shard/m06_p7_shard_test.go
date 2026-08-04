package shard_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/nbhaohao/go-seckill/internal/order"
	"github.com/nbhaohao/go-seckill/internal/shard"
	"github.com/nbhaohao/go-seckill/internal/testutil"
)

func TestM06P7StableRouteKeepsDuplicateRequestInOneShard(t *testing.T) {
	pools := openOrderShards(t)
	const prefix = "m06-p7-idem-"
	cleanupOrders(t, pools, prefix)

	var distribution [shard.ShardCount]int
	for userID := int64(1); userID <= 512; userID++ {
		first := shard.Route(userID)
		if first < 0 || first >= shard.ShardCount {
			t.Fatalf("路由结果必须可作为四库下标: user_id=%d got_shard=%d want_range=[0,%d)", userID, first, shard.ShardCount)
		}
		for repeat := 0; repeat < 20; repeat++ {
			if got := shard.Route(userID); got != first {
				t.Fatalf("同一 user_id 反复路由必须稳定: user_id=%d first_shard=%d repeat=%d got_shard=%d", userID, first, repeat, got)
			}
		}
		distribution[first]++
	}

	const userID = int64(70001)
	target := shard.Route(userID)
	requestID := prefix + "same-request"
	firstID := orderIDForShard(target, 1)
	secondID := orderIDForShard(target, 2)
	insertOrder(t, pools[target], firstID, 91001, userID, requestID, 1, time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))
	_, retryErr := pools[target].Exec(
		"INSERT INTO orders (id, product_id, user_id, request_id, quantity, status, created_at) VALUES (?, ?, ?, ?, 1, 'created', ?)",
		secondID, 91001, userID, requestID, time.Date(2026, 8, 3, 12, 0, 1, 0, time.UTC),
	)
	if retryErr == nil {
		t.Fatalf("重复 requestID 的第二次写入必须被目标分片拒绝: request_id=%q shard=%d second_order_id=%d", requestID, target, secondID)
	}
	counts := countOrders(t, pools, prefix)
	total := sumCounts(counts)
	t.Logf("stable route distribution=%v duplicate_request_id=%q target_shard=%d per_shard_orders=%v total=%d", distribution, requestID, target, counts, total)
	if total != 1 {
		t.Fatalf("同一 requestID 重试后四库合计只能有一张订单: request_id=%q target_shard=%d per_shard=%v want_total=1 got_total=%d", requestID, target, counts, total)
	}
}

func TestM06P7GetByOrderIDTouchesExactlyOneShard(t *testing.T) {
	pools := openOrderShards(t)
	const prefix = "m06-p7-direct-"
	cleanupOrders(t, pools, prefix)

	const target = 2
	orderID := orderIDForShard(target, 17)
	insertOrder(t, pools[target], orderID, 91002, 80002, prefix+"target", 1, time.Date(2026, 8, 3, 12, 1, 0, 0, time.UTC))
	probe := shard.NewAccessProbe()
	store := shard.NewStore(pools, probe)
	got, err := store.GetByOrderID(context.Background(), orderID)
	if err != nil {
		t.Fatalf("按 order_id 直查必须返回目标订单: order_id=%d encoded_shard=%d err=%v", orderID, target, err)
	}
	if got.ID != orderID {
		t.Fatalf("按 order_id 直查不能返回别的订单: want_id=%d got_id=%d order=%+v", orderID, got.ID, got)
	}
	accesses := probe.Snapshot()
	t.Logf("direct lookup order_id=%d encoded_shard=%d db_accesses=%v", orderID, target, accesses)
	for i, count := range accesses {
		want := 0
		if i == target {
			want = 1
		}
		if count != want {
			t.Fatalf("按 order_id 直查只允许访问编码指向的一个库: order_id=%d target=%d db_accesses=%v shard=%d want=%d got=%d", orderID, target, accesses, i, want, count)
		}
	}
}

func TestM06P7FanOutCursorAndFourShardInvariant(t *testing.T) {
	pools := openOrderShards(t)
	baseDB := testutil.OpenTestDB(t)
	defer baseDB.Close()
	const (
		prefix       = "m06-p7-list-"
		productID    = int64(91003)
		initialStock = 12
		remaining    = 4
	)
	cleanupOrders(t, pools, prefix)
	testutil.ResetProduct(t, baseDB, productID, initialStock)
	if _, err := baseDB.Exec("UPDATE products SET stock = ? WHERE id = ?", remaining, productID); err != nil {
		t.Fatalf("seed invariant remaining stock: product_id=%d remaining=%d err=%v", productID, remaining, err)
	}

	baseTime := time.Date(2026, 8, 3, 13, 0, 0, 0, time.UTC)
	for i := 0; i < 8; i++ {
		shardNo := i % shard.ShardCount
		insertOrder(t, pools[shardNo], orderIDForShard(shardNo, int64(100+i)), productID, int64(90000+i), fmt.Sprintf("%s%02d", prefix, i), 1, baseTime.Add(-time.Duration(i/2)*time.Second))
	}

	store := shard.NewStore(pools, nil)
	var (
		cursor    shard.Cursor
		merged    []order.Order
		cursorIDs []int64
	)
	for page := 0; page < 10; page++ {
		rows, next, err := store.List(context.Background(), cursor, 3)
		if err != nil {
			t.Fatalf("fan-out 第 %d 页必须成功: cursor=%+v err=%v", page, cursor, err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			cursorIDs = append(cursorIDs, row.ID)
		}
		merged = append(merged, rows...)
		cursor = next
	}
	if len(merged) != 8 {
		t.Fatalf("游标遍历四库必须不漏订单: want_rows=8 got_rows=%d ids=%v", len(merged), cursorIDs)
	}
	seen := make(map[int64]struct{}, len(merged))
	for i, row := range merged {
		if _, exists := seen[row.ID]; exists {
			t.Fatalf("游标翻页不得重复订单: duplicate_id=%d merged_ids=%v", row.ID, cursorIDs)
		}
		seen[row.ID] = struct{}{}
		if i > 0 && orderBefore(row, merged[i-1]) {
			t.Fatalf("四库归并必须按 created_at,id 降序: index=%d previous=%+v current=%+v", i, merged[i-1], row)
		}
	}

	counts := countOrders(t, pools, prefix)
	var orderedQuantity int
	for i, db := range pools {
		var quantity int
		if err := db.Get(&quantity, "SELECT COALESCE(SUM(quantity), 0) FROM orders WHERE request_id LIKE ?", prefix+"%"); err != nil {
			t.Fatalf("sum shard quantity: shard=%d err=%v", i, err)
		}
		orderedQuantity += quantity
	}
	deducted := initialStock - remaining
	t.Logf("fan-out per_shard_orders=%v merged_cursor_sequence=%v initial_stock=%d remaining_stock=%d four_shard_quantity=%d", counts, cursorIDs, initialStock, remaining, orderedQuantity)
	if deducted != orderedQuantity {
		t.Fatalf("库存扣减必须等于四个分片订单数量之和: initial=%d remaining=%d deducted=%d per_shard=%v ordered_quantity=%d", initialStock, remaining, deducted, counts, orderedQuantity)
	}
}

func orderIDForShard(shardNo int, sequence int64) int64 {
	const syntheticMillis = int64(100_000)
	return (syntheticMillis << 22) | (int64(shardNo) << 12) | sequence
}

func insertOrder(t *testing.T, db *sqlx.DB, id, productID, userID int64, requestID string, quantity int, createdAt time.Time) {
	t.Helper()
	_, err := db.Exec(
		"INSERT INTO orders (id, product_id, user_id, request_id, quantity, status, created_at) VALUES (?, ?, ?, ?, ?, 'created', ?)",
		id, productID, userID, requestID, quantity, createdAt,
	)
	if err != nil {
		t.Fatalf("seed shard order: id=%d request_id=%q err=%v", id, requestID, err)
	}
}

func cleanupOrders(t *testing.T, pools [shard.ShardCount]*sqlx.DB, prefix string) {
	t.Helper()
	for i, db := range pools {
		if _, err := db.Exec("DELETE FROM orders WHERE request_id LIKE ?", prefix+"%"); err != nil {
			t.Fatalf("clean shard orders: shard=%d prefix=%q err=%v", i, prefix, err)
		}
	}
	t.Cleanup(func() {
		for _, db := range pools {
			_, _ = db.Exec("DELETE FROM orders WHERE request_id LIKE ?", prefix+"%")
		}
	})
}

func countOrders(t *testing.T, pools [shard.ShardCount]*sqlx.DB, prefix string) [shard.ShardCount]int {
	t.Helper()
	var counts [shard.ShardCount]int
	for i, db := range pools {
		if err := db.Get(&counts[i], "SELECT COUNT(*) FROM orders WHERE request_id LIKE ?", prefix+"%"); err != nil {
			t.Fatalf("count shard orders: shard=%d prefix=%q err=%v", i, prefix, err)
		}
	}
	return counts
}

func sumCounts(counts [shard.ShardCount]int) int {
	total := 0
	for _, count := range counts {
		total += count
	}
	return total
}

func orderBefore(a, b order.Order) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.After(b.CreatedAt)
	}
	return a.ID > b.ID
}
