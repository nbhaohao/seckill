// 已就位（AI 生成）：m03 测试共用的接线——连真实 MySQL + 真实 Redis、把商品复位到干净起跑线。
// 它们只是造并发窗口的工具，本身不是教学点。
package deduct

import (
	"context"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/idgen"
	"github.com/nbhaohao/go-seckill/internal/testutil"
)

// newTestDeps 连真实 MySQL + Redis，把商品复位到给定库存（version 归零、历史订单清空），
// 并顺手删掉该商品的库存 key 与锁 key，让每条测试都从"什么都没有"起跑。
func newTestDeps(t *testing.T, productID int64, stock int) (*sqlx.DB, *redis.Client, *idgen.Node) {
	t.Helper()
	db := testutil.OpenTestDB(t)
	rdb := testutil.OpenTestRedis(t)
	testutil.ResetProduct(t, db, productID, stock)
	resetVersion(t, db, productID)
	testutil.DeleteKeys(t, rdb, StockKey(productID), LockKey(productID))
	return db, rdb, newTestNode(t)
}

func newTestNode(t *testing.T) *idgen.Node {
	t.Helper()
	n, err := idgen.NewNode(3)
	if err != nil {
		t.Fatalf("idgen.NewNode: %v", err)
	}
	return n
}

// resetVersion 把 version 列归零。列不存在说明 migrations/0003 还没应用——
// 直接 Skip 并说清怎么修，别让人对着一条 "Unknown column 'version'" 猜。
func resetVersion(t *testing.T, db *sqlx.DB, productID int64) {
	t.Helper()
	if _, err := db.Exec("UPDATE products SET version = 0 WHERE id = ?", productID); err != nil {
		t.Skipf("skip: products.version 不存在（%v）；先应用 migrations/0003_products_version.sql，或直接跑 ./scripts/checks_m03.sh", err)
	}
}

// guard 把"这个 phase 还没实现"的 panic 转成当前这条测试的普通失败。
// 不加它，一个 panic 会把整个测试进程打挂，同一个包里其他 phase 的测试根本跑不到，
// 红态下就看不出"哪些绿了、哪些还红着"。
func guard(t *testing.T) {
	if r := recover(); r != nil {
		t.Fatalf("被测方法 panic（对应 phase 还没实现？）：%v", r)
	}
}

// guardConcurrent 是 guard 的 goroutine 版：并发测试里不能用 t.Fatalf（它只能在测试
// goroutine 里调），改成 t.Errorf 记一笔。
func guardConcurrent(t *testing.T) {
	if r := recover(); r != nil {
		t.Errorf("被测方法 panic（对应 phase 还没实现？）：%v", r)
	}
}

// countOrders 数某个商品名下的订单行数——恒等式的右边。
func countOrders(t *testing.T, db *sqlx.DB, productID int64) int {
	t.Helper()
	var n int
	if err := db.Get(&n, "SELECT COUNT(*) FROM orders WHERE product_id = ?", productID); err != nil {
		t.Fatalf("count orders: %v", err)
	}
	return n
}

// readStock 读 DB 里的当前库存——恒等式的左边。
func readStock(t *testing.T, db *sqlx.DB, productID int64) int {
	t.Helper()
	var stock int
	if err := db.Get(&stock, "SELECT stock FROM products WHERE id = ?", productID); err != nil {
		t.Fatalf("read stock: %v", err)
	}
	return stock
}

// readRedisStock 读 Redis 里的预扣库存（p4 用）。key 不存在返回 -1 以示区分。
func readRedisStock(t *testing.T, rdb *redis.Client, productID int64) int64 {
	t.Helper()
	v, err := rdb.Get(context.Background(), StockKey(productID)).Int64()
	if err == redis.Nil {
		return -1
	}
	if err != nil {
		t.Fatalf("read redis stock: %v", err)
	}
	return v
}
