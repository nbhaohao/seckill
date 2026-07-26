package order

import (
	"context"
	"database/sql"
	"errors"

	"github.com/go-sql-driver/mysql"
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

	// FOR UPDATE 是本关的题眼：这一行从此刻到本事务提交/回滚为止，被排他锁独占，
	// 别的事务想读同一行的 FOR UPDATE 或想 UPDATE 它，都会被阻塞在这里排队，p1 的并发读窗口被物理堵死。
	var stock int
	err = tx.GetContext(ctx, &stock, "SELECT stock FROM products WHERE id = ? FOR UPDATE", req.ProductID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrProductNotFound
		}
		return nil, err
	}

	if stock < req.Quantity {
		return nil, ErrInsufficientStock
	}

	id, err := node.NextID()
	if err != nil {
		return nil, err
	}

	o := &Order{
		ID:        id,
		ProductID: req.ProductID,
		UserID:    req.UserID,
		RequestID: req.RequestID,
		Quantity:  req.Quantity,
		Status:    "created",
	}
	// 两个并发事务可能都在幂等快路径那一步看到不存在；uk_request_id 才是最终并发闸门。
	_, err = tx.ExecContext(ctx, "INSERT INTO orders (id, product_id, user_id, request_id, quantity, status) VALUES (?,?,?,?,?,?)",
		o.ID, o.ProductID, o.UserID, o.RequestID, o.Quantity, o.Status)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			// 我是后到的那个：先结束当前 RR 事务（defer 会做），再用 db（新事务/新快照）
			// 回查赢家已经提交的那行订单——旧快照可能仍看不到赢家事务，不能在 tx 里查。
			_ = tx.Rollback()
			var winner Order
			if err := db.GetContext(ctx, &winner, "SELECT id, product_id, user_id, request_id, quantity, status, created_at FROM orders WHERE request_id = ?", req.RequestID); err != nil {
				return nil, err
			}
			return &winner, nil
		}
		return nil, err
	}

	if _, err := tx.ExecContext(ctx, "UPDATE products SET stock = stock - ? WHERE id = ?", req.Quantity, req.ProductID); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return o, nil
}
