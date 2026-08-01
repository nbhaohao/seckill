package expire

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

// CloseCreatedOrder is sk-m5a p2 S1. AI 将在 p2 学习时分切片实现。
//  1. 先读取这张订单对应的归还目标，再用 status=created 的条件更新争夺唯一关单权。
//  2. RowsAffected 是 changed rows；只有 created -> closed 才会得到 1。
func CloseCreatedOrder(ctx context.Context, db *sqlx.DB, orderID int64) (CloseResult, error) {
	// TODO(sk-m5a-p2-s1): load compensation fields and conditionally transition created -> closed.
	panic("TODO: phase p2 close created order")
}

// RestoreClosedStock is sk-m5a p2 S2. AI 将在 p2 学习时分切片实现。
//  1. RowsAffected=1 是唯一归还凭证；归还动作复用 deduct.RollbackPreDeduct。
func RestoreClosedStock(ctx context.Context, rdb *redis.Client, result CloseResult) error {
	// TODO(sk-m5a-p2-s2): restore exactly result.Quantity only for the winning transition.
	panic("TODO: phase p2 restore closed stock")
}

// RemoveProcessedExpiry is sk-m5a p2 S3. AI 将在 p2 学习时分切片实现。
//  1. 状态处理与必要的库存归还都成功后，才从到期索引移除该订单。
func RemoveProcessedExpiry(ctx context.Context, rdb *redis.Client, orderID int64) error {
	// TODO(sk-m5a-p2-s3): remove the successfully processed member from ExpireZSetKey.
	panic("TODO: phase p2 remove processed expiry")
}
