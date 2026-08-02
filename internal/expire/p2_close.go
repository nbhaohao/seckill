package expire

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/deduct"
)

// CloseCreatedOrder is sk-m5a p2 S1. AI 将在 p2 学习时分切片实现。
//  1. 先读取这张订单对应的归还目标，再用 status=created 的条件更新争夺唯一关单权。
//  2. RowsAffected 是 changed rows；只有 created -> closed 才会得到 1。
func CloseCreatedOrder(ctx context.Context, db *sqlx.DB, orderID int64) (CloseResult, error) {
	var compensation struct {
		ProductID int64 `db:"product_id"`
		Quantity  int   `db:"quantity"`
	}
	if err := db.GetContext(ctx, &compensation, "SELECT product_id, quantity FROM orders WHERE id = ?", orderID); err != nil {
		return CloseResult{}, err
	}

	res, err := db.ExecContext(ctx, "UPDATE orders SET status = ? WHERE id = ? AND status = ?", StatusClosed, orderID, StatusCreated)
	if err != nil {
		return CloseResult{}, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return CloseResult{}, err
	}

	return CloseResult{
		OrderID:      orderID,
		ProductID:    compensation.ProductID,
		Quantity:     compensation.Quantity,
		RowsAffected: rowsAffected,
	}, nil
}

// RestoreClosedStock is sk-m5a p2 S2. AI 将在 p2 学习时分切片实现。
//  1. RowsAffected=1 是唯一归还凭证；归还动作复用 deduct.RollbackPreDeduct。
func RestoreClosedStock(ctx context.Context, rdb *redis.Client, result CloseResult) error {
	if result.RowsAffected != 1 {
		return nil
	}
	return deduct.RollbackPreDeduct(ctx, rdb, result.ProductID, result.Quantity)
}

// RemoveProcessedExpiry is sk-m5a p2 S3. AI 将在 p2 学习时分切片实现。
//  1. 状态处理与必要的库存归还都成功后，才从到期索引移除该订单。
func RemoveProcessedExpiry(ctx context.Context, rdb *redis.Client, orderID int64) error {
	return rdb.ZRem(ctx, ExpireZSetKey, fmt.Sprint(orderID)).Err()
}
