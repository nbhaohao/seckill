// 已就位（AI 生成）：Gin 只做 HTTP/port 翻译；p1 的学习点在 in-process adapter。
package gateway

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/nbhaohao/go-seckill/internal/metrics"
	"github.com/nbhaohao/go-seckill/internal/ports"
)

type HTTP struct {
	orders    ports.OrderService
	inventory ports.InventoryService
}

func NewHTTP(orders ports.OrderService, inventory ports.InventoryService) *HTTP {
	return &HTTP{orders: orders, inventory: inventory}
}

type placeOrderBody struct {
	RequestID string `json:"request_id" binding:"required"`
	ProductID int64  `json:"product_id" binding:"required"`
	UserID    int64  `json:"user_id" binding:"required"`
	Quantity  int    `json:"quantity" binding:"required"`
}

func (h *HTTP) PlaceOrder(c *gin.Context) {
	var body placeOrderBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	start := time.Now()
	o, err := h.orders.PlaceOrder(c.Request.Context(), ports.PlaceOrderRequest{
		RequestID: body.RequestID,
		ProductID: body.ProductID,
		UserID:    body.UserID,
		Quantity:  body.Quantity,
	})
	switch {
	case err == nil:
		metrics.OrdersPlaced.WithLabelValues("success").Inc()
		metrics.OrderLatency.WithLabelValues("success").Observe(time.Since(start).Seconds())
		c.JSON(http.StatusOK, o)
	case errors.Is(err, ports.ErrInsufficientStock):
		metrics.OrdersPlaced.WithLabelValues("insufficient_stock").Inc()
		metrics.OrderLatency.WithLabelValues("insufficient_stock").Observe(time.Since(start).Seconds())
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
	case errors.Is(err, ports.ErrInvalidQuantity), errors.Is(err, ports.ErrProductNotFound):
		metrics.OrdersPlaced.WithLabelValues("error").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	default:
		metrics.OrdersPlaced.WithLabelValues("error").Inc()
		metrics.OrderLatency.WithLabelValues("error").Observe(time.Since(start).Seconds())
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
}

func (h *HTTP) GetOrderByRequestID(c *gin.Context) {
	requestID := c.Param("requestID")
	found, err := h.orders.GetOrderByRequestID(c.Request.Context(), requestID)
	switch {
	case err == nil:
		c.JSON(http.StatusOK, found)
	case errors.Is(err, ports.ErrOrderNotFound):
		c.JSON(http.StatusAccepted, gin.H{"request_id": requestID, "status": "pending"})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
}

func (h *HTTP) GetProduct(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
		return
	}
	start := time.Now()
	p, err := h.inventory.GetProduct(c.Request.Context(), id)
	metrics.ProductReads.WithLabelValues("cached").Inc()
	metrics.ProductReadLatency.WithLabelValues("cached").Observe(time.Since(start).Seconds())
	switch {
	case err == nil:
		c.JSON(http.StatusOK, p)
	case errors.Is(err, ports.ErrCachedProductMiss):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
}
