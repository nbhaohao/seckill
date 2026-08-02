// Package bucket 是 sk-m5b 的分段库存（热点打散）实现。
//
// m03 p4 把「够不够 + 扣减」压进一段 Lua，超卖问题解决了，但那段 Lua 每一发都打在
// 同一个 key（seckill:stock:<id>）上。一个 key 只能落在一个 hash slot、进而只能落在
// 一个实例上（Redis Cluster 规范），所以这条路纵向优化到头之后就没有更多机器可加——
// 本模块把库存横向拆成 N 个桶 key，让流量摊到 N 个 slot 上，同时把拆桶自己的代价量出来。
//
// 与 m05 冻结的单 key 版**并存**：单 key 路径（deduct.PlaceOrderWithPreDeduct）一个字不动，
// 分桶路径是 /debug/orders/bucket 这条新入口，before/after 才能反复复跑。
package bucket

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/idgen"
	"github.com/nbhaohao/go-seckill/internal/order"
)

// 已就位（AI 生成）：本文件是分桶路径的样板层——key 命名、单桶 Lua、下单编排。
// 学习时要实现的三个函数在 p1_bucket.go，两个在 p2_spread.go。

// Config 是分桶路径的两个旋钮，压测时按档位改的就是它俩。
//
//   - BucketCount(N)：库存拆成几个 key。N 越大热点摊得越开，碎片也越碎。
//   - MaxProbes(k)：起始桶扣不到时，最多**再**探几个桶。它必须有显式上界——
//     没有上界的话，库存快见底时每一发请求都会把 N 个桶挨个访问一遍，
//     原本 1 次 Redis 往返变成 N 次，热点没消失只是换了个形状。
type Config struct {
	BucketCount int
	MaxProbes   int
}

// DefaultConfig 是本课的基准档位（N=8、k=2）。压测另有档位由脚本用环境变量覆盖。
func DefaultConfig() Config { return Config{BucketCount: 8, MaxProbes: 2} }

// StockKey 是第 bucket 号桶的库存 key：seckill:stock:<productID>:b<bucket>。
//
// 沿用 m03 的 seckill:stock:<id> 前缀风格，末尾加桶号后缀。这里不用 Redis 的
// hash tag（{}），正因为我们**要**这 N 个 key 散到不同 slot 去——加了 tag 就等于
// 把它们钉回同一个 slot，白拆。
func StockKey(productID int64, bucket int) string {
	return fmt.Sprintf("seckill:stock:%d:b%d", productID, bucket)
}

// DeductResult 记录一次分桶扣减到底发生了什么。
//
// Bucket 是**实际命中**的桶号（可能不是起始桶）——回滚必须还回这一个桶，
// 这是分桶路径最容易写错的地方。失败时 Bucket 为 -1。
// Probes 是这次调用一共访问了几个桶（含起始桶），压测时它 ÷ 请求数就是
// 「平均每单打了几次 Redis」，也是探桶上界这个旋钮的直接代价指标。
type DeductResult struct {
	Bucket    int
	Remaining int64
	Probes    int
}

// deductBucketScript 与 m03 的 preDeductScript 是同一段逻辑，只是 KEYS[1] 从
// 「那一个库存 key」变成「某一个桶的库存 key」。
//
// 这件事本身就是本模块的一个结论：分桶不改变原子性怎么保证（还是 Lua 一次执行），
// 它只改变 key 的拓扑。谁在保证不超卖？仍然是 Redis 单次脚本执行的原子性，
// 而不是「桶变多了所以锁变细了」——后者是错的，这里根本没有锁。
var deductBucketScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
	return -1
end
local stock = tonumber(redis.call('GET', KEYS[1]))
local qty = tonumber(ARGV[1])
if stock < qty then
	return -1
end
return redis.call('DECRBY', KEYS[1], qty)
`)

// rollbackBucketScript 把量还回某一个桶。同 m03 的 rollbackScript，只是 key 变了。
var rollbackBucketScript = redis.NewScript(`
return redis.call('INCRBY', KEYS[1], ARGV[1])
`)

// deductOneBucket 试着从**指定的一个桶**扣 qty。
//
// 扣成功返回该桶扣完后的剩余；这个桶不够（或 key 不存在）返回 order.ErrInsufficientStock。
// 注意它只回答「这一个桶行不行」，完全不知道别的桶——「不行了要不要换个桶」是
// DeductWithProbe 的决定，两件事分开才说得清失败到底来自哪一层。
func deductOneBucket(ctx context.Context, rdb *redis.Client, productID int64, bucket int, qty int) (int64, error) {
	n, err := deductBucketScript.Run(ctx, rdb, []string{StockKey(productID, bucket)}, qty).Int64()
	if err != nil {
		return 0, err
	}
	if n == -1 {
		return 0, order.ErrInsufficientStock
	}
	return n, nil
}

// PlaceOrderWithBucketDeduct 是 /debug/orders/bucket 这条路径的编排，形状照抄 m03 的
// PlaceOrderWithPreDeduct：预扣 → 落库 → 落库失败就把预扣还回去。
//
// 唯一的差别在最后那步补偿：单 key 版还回「那个 key」没得选，分桶版必须还回
// res.Bucket——DeductWithProbe 告诉我们钱是从哪个桶扣走的，还就得还到哪个桶去。
func PlaceOrderWithBucketDeduct(ctx context.Context, rdb *redis.Client, db *sqlx.DB, node *idgen.Node, cfg Config, req order.PlaceOrderRequest) (*order.Order, error) {
	if req.Quantity <= 0 {
		return nil, order.ErrInvalidQuantity
	}

	res, err := DeductWithProbe(ctx, rdb, cfg, req.ProductID, req.RequestID, req.Quantity)
	if err != nil {
		return nil, err
	}

	rollback := func() { _ = RollbackToBucket(ctx, rdb, req.ProductID, res.Bucket, req.Quantity) }

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		rollback()
		return nil, err
	}
	if _, err := tx.ExecContext(ctx,
		"UPDATE products SET stock = stock - ? WHERE id = ?",
		req.Quantity, req.ProductID); err != nil {
		_ = tx.Rollback()
		rollback()
		return nil, err
	}
	id, err := node.NextID()
	if err != nil {
		_ = tx.Rollback()
		rollback()
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
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO orders (id, product_id, user_id, request_id, quantity, status) VALUES (?,?,?,?,?,?)",
		o.ID, o.ProductID, o.UserID, o.RequestID, o.Quantity, o.Status); err != nil {
		_ = tx.Rollback()
		rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		rollback()
		return nil, err
	}
	return o, nil
}
