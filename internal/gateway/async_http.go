// 已就位（AI 生成）：一期异步 HTTP 契约只换 port 调用，overload gate 仍由 cmd/api 挂载。
package gateway

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/nbhaohao/go-seckill/internal/overload"
	"github.com/nbhaohao/go-seckill/internal/ports"
)

type AsyncOrderFunc func(context.Context, ports.PlaceOrderRequest) (*ports.AcceptedOrder, error)

func (f AsyncOrderFunc) PlaceOrderAsync(ctx context.Context, req ports.PlaceOrderRequest) (*ports.AcceptedOrder, error) {
	return f(ctx, req)
}

type AsyncHTTP struct {
	orders        ports.AsyncOrderService
	nextRequestID func() (string, error)
}

func NewAsyncHTTP(orders ports.AsyncOrderService, nextRequestID func() (string, error)) *AsyncHTTP {
	return &AsyncHTTP{orders: orders, nextRequestID: nextRequestID}
}

func (h *AsyncHTTP) PlaceOrder(c *gin.Context) {
	var body struct {
		RequestID string `json:"request_id"`
		ProductID int64  `json:"product_id" binding:"required"`
		UserID    int64  `json:"user_id"`
		Quantity  int    `json:"quantity"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	requestID := body.RequestID
	if requestID == "" {
		generated, err := h.nextRequestID()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		requestID = generated
	}
	quantity := body.Quantity
	if quantity == 0 {
		quantity = 1
	}
	accepted, err := h.orders.PlaceOrderAsync(c.Request.Context(), ports.PlaceOrderRequest{RequestID: requestID, ProductID: body.ProductID, UserID: body.UserID, Quantity: quantity})
	switch {
	case err == nil:
		c.JSON(http.StatusAccepted, gin.H{"request_id": accepted.RequestID, "status": "accepted"})
	case errors.Is(err, ports.ErrInsufficientStock):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
	default:
		statusCode, payload := overload.WritePathFailure(err)
		c.JSON(statusCode, payload)
	}
}

func Int64RequestID(next func() (int64, error)) func() (string, error) {
	return func() (string, error) {
		id, err := next()
		return strconv.FormatInt(id, 10), err
	}
}
