package deduct

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/idgen"
	"github.com/nbhaohao/go-seckill/internal/order"
)

// ── m03 p4 · Redis + Lua 原子预扣 ⭐（本模块课眼）────────────────────────────
//
// 前两个方案都还在跟 DB 较劲：乐观锁靠重试、分布式锁靠串行化，热路径上每一发请求
// 都要打一次 MySQL。第三个方案换了个思路——把"还有没有货"这个判断整个搬到 Redis 里，
// 用一段 Lua 一次做完"判断 + 扣减"，DB 只在预扣成功之后才被碰一次。
//
// 为什么必须是 Lua：GET 判断 + DECRBY 扣减是两次往返，中间的窗口就是超卖的入口
// （两个请求都 GET 到 1，然后都 DECRBY，库存变成 -1）。Redis 执行脚本期间不插入
// 别的命令，把两步塞进一个脚本，它们对外就是一步。

// preDeductScript 是 p4 S1 要写的 Lua：原子地「判断够不够 + 扣减」。
//
// KEYS[1] = 库存 key，ARGV[1] = 要扣的数量。
// 约定的返回值：扣成功返回扣完之后的剩余库存（>= 0 的数字）；库存不足返回 -1。
// 用一个负数当"不足"的信号，是因为剩余库存本身永远不会是负数——它扣不下去就不该扣。
//
// key 不存在时也要按"不足"处理：预扣的前提是 WarmStock 已经把库存灌进来了，
// key 不在说明这个商品根本没开卖，放行就等于凭空造货。
var preDeductScript = redis.NewScript(`
-- TODO: phase p4 — AI 将在 p4 学习时实现「判断 + 扣减」的原子脚本
return redis.error_reply("TODO: phase p4 preDeductScript not implemented")
`)

// rollbackScript 是 p4 S2 要写的 Lua：把预扣的量还回去。
//
// KEYS[1] = 库存 key，ARGV[1] = 要还的数量，返回还完之后的库存。
// 它只在"预扣成功了但后面落库失败"这条路径上被调用——没有它，那部分库存就凭空蒸发，
// Redis 说卖光了、DB 里却没有对应的订单，恒等式当场破掉。
var rollbackScript = redis.NewScript(`
-- TODO: phase p4 — AI 将在 p4 学习时实现预扣回滚
return redis.error_reply("TODO: phase p4 rollbackScript not implemented")
`)

// PreDeduct 是 p4 S1：跑一次 preDeductScript。
//
// 要实现的形状：preDeductScript.Run(ctx, rdb, []string{StockKey(productID)}, qty)，
// 取出返回的整数——是 -1 就返回 ErrInsufficientStock，否则把它当作剩余库存返回。
func PreDeduct(ctx context.Context, rdb *redis.Client, productID int64, qty int) (remaining int64, err error) {
	panic("TODO: phase p4 — AI 将在 p4 学习时分切片实现 Lua 原子预扣")
}

// RollbackPreDeduct 是 p4 S2 的补偿动作：跑 rollbackScript 把量还回去。
func RollbackPreDeduct(ctx context.Context, rdb *redis.Client, productID int64, qty int) error {
	panic("TODO: phase p4 — AI 将在 p4 学习时分切片实现预扣回滚")
}

// PlaceOrderWithPreDeduct 是 p4 S2：预扣成功 → 同步落库 → 落库失败就把预扣还回去。
//
// 要实现的形状：
//  1. PreDeduct，失败（库存不足）直接返回，此时一次 DB 都没打——这就是这个方案最值钱的地方
//  2. 预扣成功后开事务：插订单 + 把 DB 库存也扣掉
//     注意这里不再判断 DB 库存够不够，闸门已经在 Redis 那一步过了；
//     DB 这条 UPDATE 只是把真相源同步过来
//  3. 事务里任何一步失败 → RollbackPreDeduct 把 Redis 那份预扣还回去，再返回错误
//
// 落库这一步在本模块是同步的。m04 会把它换成"发条消息进 Kafka、异步消费落库"，
// 那是削峰的事，本关不碰。
func PlaceOrderWithPreDeduct(ctx context.Context, rdb *redis.Client, db *sqlx.DB, node *idgen.Node, req order.PlaceOrderRequest) (*order.Order, error) {
	panic("TODO: phase p4 — AI 将在 p4 学习时分切片实现预扣后同步落库")
}
