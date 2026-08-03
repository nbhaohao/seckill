package grpcclient

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inventoryv1 "github.com/nbhaohao/go-seckill/internal/pb/inventoryv1"
	orderv1 "github.com/nbhaohao/go-seckill/internal/pb/orderv1"
	"github.com/nbhaohao/go-seckill/internal/ports"
)

type OrderAdapter struct {
	client orderv1.OrderServiceClient
}

func NewOrderAdapter(client orderv1.OrderServiceClient) *OrderAdapter {
	return &OrderAdapter{client: client}
}

// PlaceOrderAsync 是 sk-m06 p2 S3：gateway 通过 order stub 实现 AsyncOrderService port。
// 1. 为什么把 ctx 原样交给 stub：gRPC 才能把入口 deadline 编码进 RPC，取消也才能越过进程边界。
// 2. 为什么按 status.Code 映射：错误字符串不是协议，FailedPrecondition 才能稳定还原库存不足。
// 3. 为什么 Unavailable/DeadlineExceeded 保持失败：下游结果不确定时不得伪造 AcceptedOrder。
// AI 将在 p2 学习时分切片实现。
func (a *OrderAdapter) PlaceOrderAsync(ctx context.Context, req ports.PlaceOrderRequest) (*ports.AcceptedOrder, error) {
	panic("TODO: sk-m06 p2 grpcclient.OrderAdapter.PlaceOrderAsync")
}

type InventoryAdapter struct {
	client inventoryv1.InventoryServiceClient
}

func NewInventoryAdapter(client inventoryv1.InventoryServiceClient) *InventoryAdapter {
	return &InventoryAdapter{client: client}
}

// Reserve/Restore 已就位（AI 生成）：order-service 的下游 stub 胶水，学习点集中在三段主链函数。
func (a *InventoryAdapter) Reserve(ctx context.Context, req ports.ReservationRequest) (*ports.Reservation, error) {
	got, err := a.client.Reserve(ctx, &inventoryv1.ReserveRequest{RequestId: req.RequestID, ProductId: req.ProductID, Quantity: int32(req.Quantity)})
	if err != nil {
		return nil, inventoryRPCError(err)
	}
	return &ports.Reservation{RequestID: got.GetRequestId(), ProductID: got.GetProductId(), Quantity: int(got.GetQuantity()), Remaining: got.GetRemaining(), AlreadyReserved: got.GetAlreadyReserved()}, nil
}

func (a *InventoryAdapter) Restore(ctx context.Context, req ports.ReservationRequest) error {
	_, err := a.client.Restore(ctx, &inventoryv1.RestoreRequest{RequestId: req.RequestID, ProductId: req.ProductID, Quantity: int32(req.Quantity)})
	return inventoryRPCError(err)
}

func inventoryRPCError(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.FailedPrecondition:
		return ports.ErrInsufficientStock
	case codes.NotFound:
		return ports.ErrReservationNotFound
	case codes.InvalidArgument:
		return ports.ErrReservationConflict
	case codes.Canceled:
		return context.Canceled
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	default:
		return err
	}
}

var _ ports.AsyncOrderService = (*OrderAdapter)(nil)
var _ ports.InventoryReservationService = (*InventoryAdapter)(nil)
