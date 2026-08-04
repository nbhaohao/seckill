package gateway

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/nbhaohao/go-seckill/internal/adapter/inprocess"
	"github.com/nbhaohao/go-seckill/internal/cache"
	"github.com/nbhaohao/go-seckill/internal/order"
	"github.com/nbhaohao/go-seckill/internal/ports"
)

func TestM06P1PostOrdersHTTPContractUnchanged(t *testing.T) {
	gin.SetMode(gin.TestMode)
	createdAt := time.Date(2026, 8, 3, 12, 34, 56, 0, time.UTC)
	orders := inprocess.NewOrderAdapter(
		func(_ context.Context, req order.PlaceOrderRequest) (*order.Order, error) {
			return &order.Order{ID: 601, ProductID: req.ProductID, UserID: req.UserID, RequestID: req.RequestID, Quantity: req.Quantity, Status: "created", CreatedAt: createdAt}, nil
		},
		func(context.Context, string) (*order.Order, error) { return nil, nil },
	)
	r := gin.New()
	r.POST("/orders", NewHTTP(orders, inprocess.NewInventoryAdapter(nil)).PlaceOrder)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"request_id":"req-p1","product_id":42,"user_id":7,"quantity":2}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(rec, req)

	wantBody := `{"ID":601,"ProductID":42,"UserID":7,"RequestID":"req-p1","Quantity":2,"Status":"created","CreatedAt":"2026-08-03T12:34:56Z"}`
	gotBody := strings.TrimSpace(rec.Body.String())
	t.Logf("POST /orders status=%d body=%s", rec.Code, rec.Body.String())
	if rec.Code != http.StatusOK || gotBody != wantBody {
		t.Fatalf("POST /orders HTTP contract must stay frozen: request_id=%q want_status=%d got_status=%d want_body=%s got_body=%s", "req-p1", http.StatusOK, rec.Code, wantBody, gotBody)
	}
}

func TestM06P1InsufficientStockKeeps409(t *testing.T) {
	gin.SetMode(gin.TestMode)
	orders := inprocess.NewOrderAdapter(
		func(context.Context, order.PlaceOrderRequest) (*order.Order, error) {
			return nil, order.ErrInsufficientStock
		},
		func(context.Context, string) (*order.Order, error) { return nil, nil },
	)
	r := gin.New()
	r.POST("/orders", NewHTTP(orders, inprocess.NewInventoryAdapter(nil)).PlaceOrder)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"request_id":"req-oos","product_id":42,"user_id":7,"quantity":2}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(rec, req)

	wantBody := `{"error":"order: insufficient stock"}`
	gotBody := strings.TrimSpace(rec.Body.String())
	t.Logf("POST /orders (insufficient stock) status=%d body=%s ports_text=%q order_text=%q", rec.Code, rec.Body.String(), ports.ErrInsufficientStock.Error(), order.ErrInsufficientStock.Error())
	if rec.Code != http.StatusConflict || gotBody != wantBody {
		t.Fatalf("insufficient stock must stay HTTP 409 across the new port boundary: request_id=%q want_status=%d got_status=%d want_body=%s got_body=%s", "req-oos", http.StatusConflict, rec.Code, wantBody, gotBody)
	}
}

func TestM06P1GetOrderByRequestIDHTTPContractUnchanged(t *testing.T) {
	gin.SetMode(gin.TestMode)
	orders := inprocess.NewOrderAdapter(
		func(context.Context, order.PlaceOrderRequest) (*order.Order, error) { return nil, nil },
		func(_ context.Context, requestID string) (*order.Order, error) {
			return nil, sql.ErrNoRows
		},
	)
	r := gin.New()
	r.GET("/orders/requests/:requestID", NewHTTP(orders, inprocess.NewInventoryAdapter(nil)).GetOrderByRequestID)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/orders/requests/req-pending", nil))

	wantBody := `{"request_id":"req-pending","status":"pending"}`
	gotBody := strings.TrimSpace(rec.Body.String())
	t.Logf("GET /orders/requests/:requestID status=%d body=%s", rec.Code, rec.Body.String())
	if rec.Code != http.StatusAccepted || gotBody != wantBody {
		t.Fatalf("pending order HTTP contract must stay frozen: request_id=%q want_status=%d got_status=%d want_body=%s got_body=%s", "req-pending", http.StatusAccepted, rec.Code, wantBody, gotBody)
	}
}

func TestM06P1GetProductHTTPContractUnchanged(t *testing.T) {
	gin.SetMode(gin.TestMode)
	inventory := inprocess.NewInventoryAdapter(func(_ context.Context, id int64) (*cache.Product, error) {
		return &cache.Product{ID: id, Name: "flash keyboard", Stock: 9}, nil
	})
	r := gin.New()
	r.GET("/products/:id", NewHTTP(inprocess.NewOrderAdapter(nil, nil), inventory).GetProduct)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/products/42", nil))

	wantBody := `{"id":42,"name":"flash keyboard","stock":9}`
	gotBody := strings.TrimSpace(rec.Body.String())
	t.Logf("GET /products/:id status=%d body=%s", rec.Code, rec.Body.String())
	if rec.Code != http.StatusOK || gotBody != wantBody {
		t.Fatalf("GET /products/:id HTTP contract must stay frozen: product_id=%d want_status=%d got_status=%d want_body=%s got_body=%s", 42, http.StatusOK, rec.Code, wantBody, gotBody)
	}
}
