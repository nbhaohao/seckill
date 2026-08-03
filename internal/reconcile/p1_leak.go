package reconcile

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/deduct"
	"github.com/nbhaohao/go-seckill/internal/order"
)

// ── sk-m5c p1 · 把泄漏造出来 ────────────────────────────────────────────────
//
// 下面两个函数由 AI 在 p1 学习时分两个切片实现；现在是红态，签名锁死、函数体是可定位的 panic。
//
// 这一关不修任何东西，只做一件事：让 m04 那句「投递失败就回滚预扣」自己露出边界。
// 补偿代码写在进程里，进程还活着它才跑得了；一旦进程死在预扣与投递之间，
// 那段 defer/if err != nil 一行都不会执行。这一关要把那条差额量出来。

// EnqueueWithCrash 是 p1 S1：带可控崩溃开关的异步下单前半段。
//
// 要实现的形状：
//  1. 先 deduct.PreDeduct 扣 Redis 库存；不足就原样返回 order.ErrInsufficientStock，
//     此时什么都还没发生，也没有什么需要补偿
//  2. 预扣成功之后立刻问一次 crash.Fire()：为 true 就直接返回 ErrSimulatedCrash，
//     **不许回滚、不许调 produce**。这里模拟的是进程当场死亡，
//     真实的死亡没有任何机会执行补偿——写成「返回前先回滚一下」就把要复现的事故抹掉了
//  3. 没崩就调 produce；只有 produce 返回错误时才走 deduct.RollbackPreDeduct，
//     这条路径与 m04 的 mq.EnqueueOrder 完全一致，本关不改它的语义
func EnqueueWithCrash(ctx context.Context, rdb *redis.Client, req order.PlaceOrderRequest, produce ProduceFunc, crash *CrashSwitch) error {
	if _, err := deduct.PreDeduct(ctx, rdb, req.ProductID, req.Quantity); err != nil {
		return err
	}
	if crash.Fire() {
		return ErrSimulatedCrash
	}
	if err := produce(ctx, req); err != nil {
		_ = deduct.RollbackPreDeduct(ctx, rdb, req.ProductID, req.Quantity)
		return err
	}
	return nil
}

// CheckIdentity 是 p1 S2：把恒等式核成一个可打印的快照。
//
// 要实现的形状：
//  1. 读 Redis 里 deduct.StockKey(productID) 的当前值当作 RedisRemaining；
//     key 不存在按 0 计（商品没预热过，不该让核对函数自己报错）
//  2. 从 DB 数这个商品真实落库的订单量：
//     SELECT COALESCE(SUM(quantity), 0) FROM orders WHERE product_id = ?
//     ——数量列而不是行数，因为一单可以买多件，行数会把恒等式算错
//  3. RedisDeducted = initialStock - RedisRemaining；Leaked = RedisDeducted - DBOrdered
//
// 这个函数是本模块唯一的验收口径：p1 用它证明泄漏存在，p2 用同一个函数证明泄漏被补回来。
// 两边用同一把尺子，对账「有没有真的生效」才不是自说自话。
func CheckIdentity(ctx context.Context, rdb *redis.Client, db *sqlx.DB, productID int64, initialStock int64) (Identity, error) {
	panic("TODO: phase p1 · S2 CheckIdentity 尚未实现（AI 将在 p1 学习时分切片实现）")
}
