package inprocess

import (
	"context"

	"github.com/nbhaohao/go-seckill/internal/cache"
	"github.com/nbhaohao/go-seckill/internal/order"
	"github.com/nbhaohao/go-seckill/internal/ports"
)

// PlaceOrderFunc / FindOrderFunc / GetProductFunc let cmd/api inject the phase-one
// implementation without making the port depend on SQL, Redis, or Gin.
type PlaceOrderFunc func(context.Context, order.PlaceOrderRequest) (*order.Order, error)
type FindOrderFunc func(context.Context, string) (*order.Order, error)
type GetProductFunc func(context.Context, int64) (*cache.Product, error)

type OrderAdapter struct {
	place PlaceOrderFunc
	find  FindOrderFunc
}

func NewOrderAdapter(place PlaceOrderFunc, find FindOrderFunc) *OrderAdapter {
	return &OrderAdapter{place: place, find: find}
}

type InventoryAdapter struct {
	get GetProductFunc
}

func NewInventoryAdapter(get GetProductFunc) *InventoryAdapter {
	return &InventoryAdapter{get: get}
}

// PlaceOrder 是 sk-m06 p1 S1：把进程无关的下单请求映射到一期 order 调用，再把结果映射回来。
// 1. 为什么在 adapter 映射类型：ports 不能反向依赖 internal/order，否则边界只是换了包名。
// 2. 为什么保留错误语义：gateway 仍靠错误类别输出一期冻结的 HTTP status 与 error body。
// AI 将在 p1 学习时分切片实现。
func (a *OrderAdapter) PlaceOrder(ctx context.Context, req ports.PlaceOrderRequest) (*ports.Order, error) {
	panic("TODO: sk-m06 p1 OrderAdapter.PlaceOrder")
}

// GetOrderByRequestID 是 sk-m06 p1 S2：通过注入的查单函数读取一期订单，并区分 pending 与系统错误。
// 1. 为什么用 ErrOrderNotFound：HTTP 层只认识 port 语义，不应认识 sql.ErrNoRows。
// 2. 为什么返回完整 Order：一期成功查询的 JSON 字段与大小写必须逐字保持。
// AI 将在 p1 学习时分切片实现。
func (a *OrderAdapter) GetOrderByRequestID(ctx context.Context, requestID string) (*ports.Order, error) {
	panic("TODO: sk-m06 p1 OrderAdapter.GetOrderByRequestID")
}

// GetProduct 是 sk-m06 p1 S3：把一期缓存读路径包在 inventory port 后，并保持 not-found 分类。
// 1. 为什么仍走注入的 cache.Get：p1 只切边界，不改变 m02 已验证的缓存业务逻辑。
// 2. 为什么映射成 ports.Product：后续换成本地或 RPC adapter 时，gateway 无需跟着改类型。
// AI 将在 p1 学习时分切片实现。
func (a *InventoryAdapter) GetProduct(ctx context.Context, id int64) (*ports.Product, error) {
	panic("TODO: sk-m06 p1 InventoryAdapter.GetProduct")
}
