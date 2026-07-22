// 已就位（AI 生成）：下单链路共用的类型与哨兵错误，两个 phase（naive/tx）共用同一份契约。
package order

import (
	"errors"
	"time"
)

// PlaceOrderRequest 是下单请求的入参形状。RequestID 由调用方（HTTP handler）生成或透传，
// 用来在 p3 实现幂等——同一个 RequestID 重复提交只应落一行订单。
type PlaceOrderRequest struct {
	RequestID string
	ProductID int64
	UserID    int64
	Quantity  int
}

// Order 对应 orders 表一行。
type Order struct {
	ID        int64     `db:"id"`
	ProductID int64     `db:"product_id"`
	UserID    int64     `db:"user_id"`
	RequestID string    `db:"request_id"`
	Quantity  int       `db:"quantity"`
	Status    string    `db:"status"`
	CreatedAt time.Time `db:"created_at"`
}

var (
	// ErrInsufficientStock：库存不够，正常业务错误，不是系统故障。
	ErrInsufficientStock = errors.New("order: insufficient stock")
	// ErrProductNotFound：商品行不存在。
	ErrProductNotFound = errors.New("order: product not found")
	// ErrInvalidQuantity：quantity 必须 > 0。
	ErrInvalidQuantity = errors.New("order: quantity must be positive")
)
