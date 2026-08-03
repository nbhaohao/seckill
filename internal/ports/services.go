// Package ports defines the process-neutral business boundary introduced in sk-m06 p1.
package ports

import (
	"context"
	"errors"
	"time"
)

type PlaceOrderRequest struct {
	RequestID string
	ProductID int64
	UserID    int64
	Quantity  int
}

// Order deliberately keeps the phase-one JSON field names. Adding json tags
// here would silently change the frozen POST /orders response bytes.
type Order struct {
	ID        int64
	ProductID int64
	UserID    int64
	RequestID string
	Quantity  int
	Status    string
	CreatedAt time.Time
}

type Product struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Stock int    `json:"stock"`
}

var (
	ErrInsufficientStock = errors.New("order: insufficient stock")
	ErrProductNotFound   = errors.New("order: product not found")
	ErrInvalidQuantity   = errors.New("order: quantity must be positive")
	ErrOrderNotFound     = errors.New("order: not found")
	ErrCachedProductMiss = errors.New("cache: product not found")
)

type OrderService interface {
	PlaceOrder(context.Context, PlaceOrderRequest) (*Order, error)
	GetOrderByRequestID(context.Context, string) (*Order, error)
}

type InventoryService interface {
	GetProduct(context.Context, int64) (*Product, error)
}
