// 已就位（AI 生成）：m03 三个防超卖方案共用的 key 拼法、哨兵错误与下单样板。
// 这些是四个 phase 共用的契约，本身不是教学点——教学点是三个方案各自怎么把
// 「读库存 → 判断够不够 → 扣减」这三步做成安全的。
package deduct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/idgen"
	"github.com/nbhaohao/go-seckill/internal/order"
)

var (
	// ErrInsufficientStock：库存不够。直接复用 m01 的哨兵，让四条路径（含 m01 悲观锁基线）
	// 对"卖光了"这件事返回同一个错误，p5 的对比表才好按结果分类计数。
	ErrInsufficientStock = order.ErrInsufficientStock
	// ErrProductNotFound：商品行不存在。
	ErrProductNotFound = order.ErrProductNotFound

	// ErrCASRetriesExhausted（p1）：乐观锁重试到上限仍未成功。
	// 注意它和 ErrInsufficientStock 是两件事：前者是"一直被别人抢先"，后者是"确实没货了"。
	// 混成一类，压测报告就分不出"高竞争"和"卖光"，那张对比表也就废了一列。
	ErrCASRetriesExhausted = errors.New("deduct: optimistic CAS retries exhausted")

	// ErrLockNotAcquired（p2）：没抢到分布式锁。
	ErrLockNotAcquired = errors.New("deduct: lock not acquired")
	// ErrLockLost（p2/p3）：释放/续期时发现锁已经不是自己的了（过期后被别人抢走）。
	// 这个错误的存在本身就是 p3 的题眼：锁可能在你还在临界区里的时候就没了。
	ErrLockLost = errors.New("deduct: lock lost (token mismatch)")
)

// MaxCASRetries 是 p1 乐观锁重试的上限。有界是刻意的：无界重试在高竞争下会把 CPU 和
// DB 连接一起耗光，"失败得干脆"比"永远在重试"更容易被上层限流/降级接住。
const MaxCASRetries = 10

// StockKey 是 p4 预扣库存在 Redis 里的 key（和 m02 的 seckill:product:<id> 是两个东西：
// 那个存的是商品 JSON 视图，这个存的是一个可被 DECRBY 的纯数字）。
func StockKey(productID int64) string { return fmt.Sprintf("seckill:stock:%d", productID) }

// LockKey 是 p2/p3 分布式锁的 key。按商品分锁而不是一把全局大锁——
// 不同商品之间本来就没有竞争关系，共用一把锁等于自造串行瓶颈。
func LockKey(productID int64) string { return fmt.Sprintf("seckill:lock:product:%d", productID) }

// WarmStock 把 DB 里的库存灌进 Redis，是 p4 预扣的前提（Redis 侧得先有个数才谈得上扣）。
// 用 SET 覆盖而不是 SETNX：预热就是"以 DB 为准重置一次"，这是运营动作，不是并发路径。
func WarmStock(ctx context.Context, rdb *redis.Client, db *sqlx.DB, productID int64) error {
	var stock int
	err := db.GetContext(ctx, &stock, "SELECT stock FROM products WHERE id = ?", productID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrProductNotFound
	}
	if err != nil {
		return err
	}
	return rdb.Set(ctx, StockKey(productID), stock, 0).Err()
}

// insertOrderTx 在给定事务里插一行订单。三个方案共用它——m03 只替换「扣库存」那一步，
// 订单怎么写、幂等怎么保（orders.request_id 唯一索引）都沿用 m01 的结论，不重新论证。
func insertOrderTx(ctx context.Context, tx *sqlx.Tx, node *idgen.Node, req order.PlaceOrderRequest) (*order.Order, error) {
	id, err := node.NextID()
	if err != nil {
		return nil, err
	}
	o := &order.Order{
		ID:        id,
		ProductID: req.ProductID,
		UserID:    req.UserID,
		RequestID: req.RequestID,
		Quantity:  req.Quantity,
		Status:    "created",
	}
	_, err = tx.ExecContext(ctx,
		"INSERT INTO orders (id, product_id, user_id, request_id, quantity, status) VALUES (?,?,?,?,?,?)",
		o.ID, o.ProductID, o.UserID, o.RequestID, o.Quantity, o.Status)
	if err != nil {
		return nil, err
	}
	return o, nil
}

// validateRequest 是四条路径共用的入参检查，挡在所有并发机制之前。
func validateRequest(req order.PlaceOrderRequest) error {
	if req.Quantity <= 0 {
		return order.ErrInvalidQuantity
	}
	return nil
}
