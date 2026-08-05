package grpcserver

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	inventoryv1 "github.com/nbhaohao/go-seckill/internal/pb/inventoryv1"
	orderv1 "github.com/nbhaohao/go-seckill/internal/pb/orderv1"
	"github.com/nbhaohao/go-seckill/internal/ports"
)

type OrderServer struct {
	// PlaceOrder/GetOrderByRequestID stay Unimplemented in p2: POST /orders is
	// the inseparable phase-one local transaction, and the thin query is not
	// needed to prove the async three-process path.
	orderv1.UnimplementedOrderServiceServer
	async ports.AsyncOrderService
}

func NewOrderServer(async ports.AsyncOrderService) *OrderServer {
	return &OrderServer{async: async}
}

// PlaceOrderAsync 是 sk-m06 p2 S2：把 order proto 请求交给异步 port，并把业务错误翻译成 gRPC status。
// 1. 为什么必须传原 ctx：deadline 只有沿同一个 ctx 进入 Reserve/Kafka，下游才会及时停手。
// 2. 为什么库存不足不是 Internal：gateway 必须能把它反向还原为 ports.ErrInsufficientStock 和 HTTP 409。
// 3. 为什么未知错误 fail closed：网络/下游状态不确定时绝不能构造 AcceptedOrder 假装 202。
// AI 将在 p2 学习时分切片实现。
func (s *OrderServer) PlaceOrderAsync(ctx context.Context, req *orderv1.PlaceOrderRequest) (*orderv1.AcceptedOrder, error) {
	a, err := s.async.PlaceOrderAsync(ctx, ports.PlaceOrderRequest{
		RequestID: req.GetRequestId(),
		ProductID: req.GetProductId(),
		UserID:    req.GetUserId(),
		Quantity:  int(req.GetQuantity()),
	})
	if err != nil {
		switch {
		case errors.Is(err, ports.ErrInsufficientStock):
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		case errors.Is(err, ports.ErrInvalidQuantity):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return nil, status.FromContextError(err).Err()
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return acceptedProto(a), nil
}

type InventoryServer struct {
	inventoryv1.UnimplementedInventoryServiceServer
	products     ports.InventoryService
	reservations ports.InventoryReservationService
}

func NewInventoryServer(products ports.InventoryService, reservations ports.InventoryReservationService) *InventoryServer {
	return &InventoryServer{products: products, reservations: reservations}
}

func (s *InventoryServer) GetProduct(ctx context.Context, req *inventoryv1.GetProductRequest) (*inventoryv1.Product, error) {
	p, err := s.products.GetProduct(ctx, req.GetId())
	if err != nil {
		if errors.Is(err, ports.ErrCachedProductMiss) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &inventoryv1.Product{Id: p.ID, Name: p.Name, Stock: int32(p.Stock)}, nil
}

// Reserve 是 sk-m06 p2 S1：把 inventory proto 映射到幂等 reservation port，并锁定 status 语义。
// 1. 为什么 requestID 必须跨 RPC：它是重复调用仍只扣一次的 reservation 身份，不是日志字段。
// 2. 为什么库存不足用 FailedPrecondition：这是确定的业务结果，不是 inventory 进程故障。
// 3. 为什么必须返回实际 remaining/alreadyReserved：测试与调用方需要观察幂等是否真的生效。
// AI 将在 p2 学习时分切片实现。
func (s *InventoryServer) Reserve(ctx context.Context, req *inventoryv1.ReserveRequest) (*inventoryv1.Reservation, error) {
	r, err := s.reservations.Reserve(ctx, ports.ReservationRequest{
		RequestID: req.GetRequestId(),
		ProductID: req.GetProductId(),
		Quantity:  int(req.GetQuantity()),
	})
	if err != nil {
		switch {
		case errors.Is(err, ports.ErrInsufficientStock):
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return nil, status.FromContextError(err).Err()
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &inventoryv1.Reservation{
		RequestId:       r.RequestID,
		ProductId:       r.ProductID,
		Quantity:        int32(r.Quantity),
		Remaining:       r.Remaining,
		AlreadyReserved: r.AlreadyReserved,
	}, nil
}

// Restore 已就位（AI 生成）：补偿映射是 Reserve 失败路径的机械对偶，学习预算留给 Reserve 与跨跳 status。
func (s *InventoryServer) Restore(ctx context.Context, req *inventoryv1.RestoreRequest) (*inventoryv1.RestoreResponse, error) {
	err := s.reservations.Restore(ctx, ports.ReservationRequest{RequestID: req.GetRequestId(), ProductID: req.GetProductId(), Quantity: int(req.GetQuantity())})
	switch {
	case err == nil:
		return &inventoryv1.RestoreResponse{Restored: true}, nil
	case errors.Is(err, ports.ErrReservationNotFound):
		return nil, status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ports.ErrReservationConflict), errors.Is(err, ports.ErrInvalidQuantity):
		return nil, status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return nil, status.FromContextError(err).Err()
	default:
		return nil, status.Error(codes.Internal, err.Error())
	}
}

func acceptedProto(a *ports.AcceptedOrder) *orderv1.AcceptedOrder {
	return &orderv1.AcceptedOrder{RequestId: a.RequestID, Status: int32(a.Status), AcceptedAt: timestamppb.New(a.Accepted)}
}
