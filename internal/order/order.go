package order

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"

	"github.com/nbhaohao/go-seckill/internal/idgen"
)

// PlaceOrderTx 是 m01 p3 的核心：把 p1 暴露的超卖 bug 用「一个事务 + 行锁 + 幂等」修好。
// 修法就三件事拼在一起：① SELECT ... FOR UPDATE 把"读库存"和"判断够不够"锁进同一行的
// 排他锁里，堵死 p1 那种"两边都读到旧值"的窗口；② request_id 唯一索引是幂等的最终裁判，
// 查询命中走快路径，并发插入撞 1062 时回查第一次写入的订单；
// ③ 全程包一个事务，中途任何失败都整体回滚，不会出现"扣了库存但没插订单"的半成品状态。
func PlaceOrderTx(ctx context.Context, db *sqlx.DB, node *idgen.Node, req PlaceOrderRequest) (*Order, error) {
	if req.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// 幂等快路径：REPEATABLE READ 下这是快照读，两个并发事务仍可能同时 miss；
	// 它只是减少串行重放的工作量，不提供并发幂等保证——真正兜底的是 request_id 唯一索引。
	var existing Order
	err = tx.GetContext(ctx, &existing, "SELECT id, product_id, user_id, request_id, quantity, status, created_at FROM orders WHERE request_id = ?", req.RequestID)
	if err == nil {
		return &existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	panic("TODO: phase p3 S2 - FOR UPDATE 行锁读库存并判断够不够")
}

// AI 将在 p3 学习时分切片实现（下面是思路与顺序，不是终稿代码）：
// 4. var stock int
//    err = tx.GetContext(ctx, &stock, "SELECT stock FROM products WHERE id = ? FOR UPDATE", req.ProductID)
//    —— FOR UPDATE 是本关的题眼：这一行从此刻到本事务提交/回滚为止，被排他锁独占，
//    别的事务想读同一行的 FOR UPDATE 或想 UPDATE 它，都会被阻塞在这里排队，p1 的并发读窗口被物理堵死。
//    sql.ErrNoRows 时返回 ErrProductNotFound，其他 err 直接返回
// 5. if stock < req.Quantity { return nil, ErrInsufficientStock }（tx 交给 defer 回滚）
// 6. id, err := node.NextID()；err != nil 直接返回
// 7. o := &Order{ID: id, ProductID: req.ProductID, UserID: req.UserID, RequestID: req.RequestID, Quantity: req.Quantity, Status: "created"}
//    _, err = tx.ExecContext(ctx, "INSERT INTO orders (id, product_id, user_id, request_id, quantity, status) VALUES (?,?,?,?,?,?)",
//      o.ID, o.ProductID, o.UserID, o.RequestID, o.Quantity, o.Status)
//    —— 两个并发事务可能都在第 3 步看到不存在；uk_request_id 才是最终并发闸门。
//    INSERT 若返回 *mysql.MySQLError 且 Number == 1062：先 Rollback 当前事务，再用 db.GetContext
//    按 request_id 回查已提交的订单并返回。不要在原 RR 事务里做普通 SELECT：旧快照可能仍看不到赢家事务。
//    其他 INSERT 错误原样返回。
// 8. _, err = tx.ExecContext(ctx, "UPDATE products SET stock = stock - ? WHERE id = ?", req.Quantity, req.ProductID)
// 9. if err := tx.Commit(); err != nil { return nil, err }
// 10. return o, nil
