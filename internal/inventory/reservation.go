// 已就位（AI 生成）：reservation 的 Redis 原子账本复用一期 Lua 预扣语义；p2 学习点是 RPC 边界。
package inventory

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/cache"
	"github.com/nbhaohao/go-seckill/internal/deduct"
	"github.com/nbhaohao/go-seckill/internal/ports"
)

var reserveScript = redis.NewScript(`
local state = redis.call('HGET', KEYS[2], 'state')
if state then
  local product = redis.call('HGET', KEYS[2], 'product_id')
  local quantity = redis.call('HGET', KEYS[2], 'quantity')
  if product ~= ARGV[1] or quantity ~= ARGV[2] then
    return {-2, 0}
  end
  if state == 'reserved' then
    return {1, tonumber(redis.call('GET', KEYS[1]) or '0')}
  end
end
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {-1, 0}
end
local stock = tonumber(redis.call('GET', KEYS[1]))
local qty = tonumber(ARGV[2])
if stock < qty then
  return {-1, stock}
end
local remaining = redis.call('DECRBY', KEYS[1], qty)
redis.call('HSET', KEYS[2], 'state', 'reserved', 'product_id', ARGV[1], 'quantity', ARGV[2])
return {0, remaining}
`)

var restoreScript = redis.NewScript(`
local state = redis.call('HGET', KEYS[2], 'state')
if not state then
  return -1
end
local product = redis.call('HGET', KEYS[2], 'product_id')
local quantity = redis.call('HGET', KEYS[2], 'quantity')
if product ~= ARGV[1] or quantity ~= ARGV[2] then
  return -2
end
if state == 'restored' then
  return 0
end
redis.call('INCRBY', KEYS[1], ARGV[2])
redis.call('HSET', KEYS[2], 'state', 'restored')
return 1
`)

type ProductReader interface {
	Get(context.Context, int64) (*cache.Product, error)
}

type Service struct {
	rdb      *redis.Client
	products ProductReader
}

func NewService(rdb *redis.Client, products ProductReader) *Service {
	return &Service{rdb: rdb, products: products}
}

func reservationKey(requestID string) string { return "seckill:reservation:" + requestID }

func (s *Service) GetProduct(ctx context.Context, id int64) (*ports.Product, error) {
	p, err := s.products.Get(ctx, id)
	if err != nil {
		if errors.Is(err, cache.ErrProductNotFound) {
			return nil, ports.ErrCachedProductMiss
		}
		return nil, err
	}
	return &ports.Product{ID: p.ID, Name: p.Name, Stock: p.Stock}, nil
}

func (s *Service) Reserve(ctx context.Context, req ports.ReservationRequest) (*ports.Reservation, error) {
	if req.RequestID == "" || req.ProductID <= 0 || req.Quantity <= 0 {
		return nil, ports.ErrInvalidQuantity
	}
	result, err := reserveScript.Run(ctx, s.rdb,
		[]string{deduct.StockKey(req.ProductID), reservationKey(req.RequestID)}, req.ProductID, req.Quantity).Slice()
	if err != nil {
		return nil, err
	}
	code, okCode := result[0].(int64)
	remaining, okRemaining := result[1].(int64)
	if !okCode || !okRemaining {
		return nil, fmt.Errorf("inventory: malformed reserve result: %#v", result)
	}
	switch code {
	case -2:
		return nil, ports.ErrReservationConflict
	case -1:
		return nil, ports.ErrInsufficientStock
	case 0, 1:
		return &ports.Reservation{RequestID: req.RequestID, ProductID: req.ProductID, Quantity: req.Quantity, Remaining: remaining, AlreadyReserved: code == 1}, nil
	default:
		return nil, fmt.Errorf("inventory: unknown reserve code=%d result=%#v", code, result)
	}
}

func (s *Service) Restore(ctx context.Context, req ports.ReservationRequest) error {
	code, err := restoreScript.Run(ctx, s.rdb,
		[]string{deduct.StockKey(req.ProductID), reservationKey(req.RequestID)}, req.ProductID, req.Quantity).Int()
	if err != nil {
		return err
	}
	switch code {
	case -2:
		return ports.ErrReservationConflict
	case -1:
		return ports.ErrReservationNotFound
	case 0, 1:
		return nil
	default:
		return fmt.Errorf("inventory: unknown restore code=%d", code)
	}
}
