package deduct

import (
	"context"

	"github.com/jmoiron/sqlx"

	"github.com/nbhaohao/go-seckill/internal/idgen"
	"github.com/nbhaohao/go-seckill/internal/order"
)

// ── m03 p1 · DB 乐观锁 ────────────────────────────────────────────────────────
//
// m01 的做法是悲观锁：SELECT ... FOR UPDATE 先把行锁住，别人只能排队。
// 乐观锁反过来赌"大概率没人和我抢"——不加锁直接改，改的时候再检查"我读到之后有没有
// 人动过这行"，被抢先了就重来。本 phase 用两个切片写两种形态，它们的取舍本身就是面试题。

// PlaceOrderByVersionCAS 是 p1 S1：版本号 CAS 形态的乐观锁（面试问"乐观锁"默认指这个）。
//
// 要实现的形状：
//  1. 不加锁地读出这一行的 stock 和 version
//  2. 在应用层判断 stock 够不够（不够就返回 ErrInsufficientStock，不必重试——货是真没了）
//  3. UPDATE 时把读到的那个 version 写进 WHERE：
//     UPDATE products SET stock = stock - ?, version = version + 1 WHERE id = ? AND version = ?
//     RowsAffected == 1 说明这一行从我读它到我改它之间没被人动过，扣减成立；
//     RowsAffected == 0 说明有人抢先了（version 已经变了），本次作废、回到第 1 步重来
//  4. 扣减与插订单必须在同一个事务里——只扣了库存却没插订单就是凭空少货
//  5. 重试有上限（MaxCASRetries），超了返回 ErrCASRetriesExhausted
//
// 返回值里的 attempts 是本关的证据：它就是"这一单一共尝试了几次 CAS"，
// 测试会把它汇总成冲突率打印出来。
func PlaceOrderByVersionCAS(ctx context.Context, db *sqlx.DB, node *idgen.Node, req order.PlaceOrderRequest) (*order.Order, int, error) {
	panic("TODO: phase p1 — AI 将在 p1 学习时分切片实现版本号 CAS 乐观锁")
}

// PlaceOrderByConditionalUpdate 是 p1 S2：条件更新形态（秒杀实战里更常见的那个写法）。
//
// 要实现的形状：把"判断够不够"和"扣减"压进同一条 UPDATE 的 WHERE 里——
//
//	UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?
//
// 然后靠 RowsAffected 判成败：1 = 扣成功，0 = 库存不足（返回 ErrInsufficientStock）。
// 同样要和插订单包在一个事务里。
//
// 这里没有重试循环，也没有 version 列——为什么它不需要，以及它比 S1 少拿到了什么信息，
// 是本切片的 review 点。
func PlaceOrderByConditionalUpdate(ctx context.Context, db *sqlx.DB, node *idgen.Node, req order.PlaceOrderRequest) (*order.Order, error) {
	panic("TODO: phase p1 — AI 将在 p1 学习时分切片实现条件更新形态")
}
