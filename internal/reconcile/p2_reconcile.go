package reconcile

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/deduct"
	"github.com/nbhaohao/go-seckill/internal/order"
)

// ── sk-m5c p2 · 对账 job ⭐（本模块课眼）────────────────────────────────────
//
// 下面三个函数由 AI 在 p2 学习时分三个切片实现；现在是红态。
//
// p1 已经证明：进程死在预扣与投递之间，库存就没人还了。补偿代码救不了它，
// 因为补偿代码和事故本身死在同一个进程里。所以补偿必须搬到进程外面去——
// 由另一个进程，在事后，凭一份留在 Redis 里的记录，把账拉平。
// 这一关就是把那份记录（台账）挂进下单路径，再把那个进程（对账 job）建起来。

// EnqueueWithLedger 是 p2 S1：在 p1 那条会泄漏的路径上挂一份台账。
//
// 要实现的形状：
//  1. deduct.PreDeduct 扣库存；不足原样返回 order.ErrInsufficientStock
//  2. 立刻 RecordPreDeduct(ctx, rdb, req, now)——台账必须写在崩溃点**之前**。
//     写在后面的话，崩掉的那些请求恰好是没有台账的那些，对账 job 扫不到它们，
//     于是对账只对那些本来就没出事的请求生效
//  3. crash.Fire() 为 true：返回 ErrSimulatedCrash，不回滚、不投递。
//     台账留在 pending，这就是对账 job 事后唯一的抓手
//  4. produce 失败：deduct.RollbackPreDeduct 把库存还回去，**并且**
//     用 settleEntry(ctx, rdb, entry, 0) 把这条台账结案。
//     少了结案这一步，对账 job 事后会发现「有台账、DB 里没订单」，再退一次——
//     同一笔库存被退两遍，超卖就是这么来的
//
// 注意这条路径把泄漏窗口从「预扣到投递」缩到了「预扣到写台账」两条 Redis 命令之间，
// 但没有缩到零：进程仍可能死在第 1 步和第 2 步中间。彻底消掉它要求台账与预扣写在同一段
// Lua 里，那会改动 m03 已冻结的预扣脚本，本课不做——这个残留窗口写进 write-up 里。
func EnqueueWithLedger(ctx context.Context, rdb *redis.Client, req order.PlaceOrderRequest, now time.Time, produce ProduceFunc, crash *CrashSwitch) error {
	if _, err := deduct.PreDeduct(ctx, rdb, req.ProductID, req.Quantity); err != nil {
		return err
	}
	if err := RecordPreDeduct(ctx, rdb, req, now); err != nil {
		return err
	}
	if crash.Fire() {
		return ErrSimulatedCrash
	}
	if err := produce(ctx, req); err != nil {
		_ = deduct.RollbackPreDeduct(ctx, rdb, req.ProductID, req.Quantity)
		entry := Entry{RequestID: req.RequestID, ProductID: req.ProductID, Quantity: req.Quantity, CreatedAt: now, State: LedgerPending}
		if _, sErr := settleEntry(ctx, rdb, entry, 0); sErr != nil {
			return sErr
		}
		return err
	}
	return nil
}

// ReconcileEntry 是 p2 S2：对**一条**台账给出终局判定。
//
// 要实现的形状（三道闸，顺序不能换）：
//  1. e.State 不是 LedgerPending：返回 OutcomeAlreadyReconciled，不做任何事
//  2. now.Sub(e.CreatedAt) < window：返回 OutcomeYoung，不做任何事——
//     消息可能还在 Kafka 里排队，这时候退库存就是把一笔合法订单的货退给了别人
//  3. 查 DB 里有没有 request_id = e.RequestID 的订单：
//     有 → OutcomeSettled，settleEntry(ctx, rdb, e, 0) 结案但**不归还**；
//     没有 → OutcomeRefunded，settleEntry(ctx, rdb, e, e.Quantity) 归还并结案
//
// 时间窗与 DB 查询是**两条**判据，缺一不可：只看时间会把在途订单退掉，
// 只看 DB 会把刚预扣完还没落库的正常请求当成泄漏。
//
// settleEntry 返回 false 表示这条台账刚被另一个对账进程结案了，本次没有产生任何副作用——
// 照实返回 OutcomeAlreadyReconciled，别把它算进退款数。
func ReconcileEntry(ctx context.Context, rdb *redis.Client, db *sqlx.DB, e Entry, window time.Duration, now time.Time) (Outcome, error) {
	panic("TODO: phase p2 · S2 ReconcileEntry 尚未实现（AI 将在 p2 学习时分切片实现）")
}

// ReconcileOnce 是 p2 S3：跑完整一轮对账，返回可以贴进 write-up 的账单。
//
// 要实现的形状：ScanLedger（已就位）取回全部台账 → 逐条 ReconcileEntry → 按 Outcome 累加进 Report
// （Scanned 每条都加；Refunded 时同时把这条的 Quantity 累进 RefundedQty）。
//
// 每条之前先看一眼 ctx.Err()：被取消就带着**已经完成的** Report 和 ctx.Err() 返回。
// 对账 job 挂在 overload.Shutdown 的总 deadline 下，它必须能在中途停手；
// 已完成的部分是安全的，因为每条的归还与结案本身就是原子的。
func ReconcileOnce(ctx context.Context, rdb *redis.Client, db *sqlx.DB, window time.Duration, now time.Time) (Report, error) {
	panic("TODO: phase p2 · S3 ReconcileOnce 尚未实现（AI 将在 p2 学习时分切片实现）")
}
