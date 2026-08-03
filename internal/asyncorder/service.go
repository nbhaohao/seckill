// 已就位（AI 生成）：复用一期异步链路的“预扣→produce→失败补偿”编排，传输映射留给 p2。
package asyncorder

import (
	"context"
	"errors"
	"time"

	"github.com/nbhaohao/go-seckill/internal/ports"
)

const compensationTimeout = 2 * time.Second

type ProduceFunc func(context.Context, ports.PlaceOrderRequest, time.Time) error

type Service struct {
	inventory ports.InventoryReservationService
	produce   ProduceFunc
	now       func() time.Time
}

func NewService(inventory ports.InventoryReservationService, produce ProduceFunc) *Service {
	return &Service{inventory: inventory, produce: produce, now: time.Now}
}

func (s *Service) PlaceOrderAsync(ctx context.Context, req ports.PlaceOrderRequest) (*ports.AcceptedOrder, error) {
	reservation := ports.ReservationRequest{RequestID: req.RequestID, ProductID: req.ProductID, Quantity: req.Quantity}
	if _, err := s.inventory.Reserve(ctx, reservation); err != nil {
		return nil, err
	}
	acceptedAt := s.now()
	if err := s.produce(ctx, req, acceptedAt); err != nil {
		compensateCtx, cancel := context.WithTimeout(context.Background(), compensationTimeout)
		defer cancel()
		restoreErr := s.inventory.Restore(compensateCtx, reservation)
		return nil, errors.Join(err, restoreErr)
	}
	return &ports.AcceptedOrder{RequestID: req.RequestID, Status: 202, Accepted: acceptedAt}, nil
}
