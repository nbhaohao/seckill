package grpcclient_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/nbhaohao/go-seckill/internal/adapter/grpcclient"
	grpcadapter "github.com/nbhaohao/go-seckill/internal/adapter/grpcserver"
	"github.com/nbhaohao/go-seckill/internal/asyncorder"
	"github.com/nbhaohao/go-seckill/internal/gateway"
	inventoryv1 "github.com/nbhaohao/go-seckill/internal/pb/inventoryv1"
	orderv1 "github.com/nbhaohao/go-seckill/internal/pb/orderv1"
	"github.com/nbhaohao/go-seckill/internal/ports"
)

func TestM06P2InsufficientStockSurvivesRPCAndHTTPMapping(t *testing.T) {
	h := newRPCHarness(t, &fakeInventory{stock: 0}, nil)
	_, err := h.orderPort.PlaceOrderAsync(context.Background(), ports.PlaceOrderRequest{RequestID: "p2-insufficient", ProductID: 42, UserID: 7, Quantity: 1})
	if !errors.Is(err, ports.ErrInsufficientStock) {
		t.Fatalf("库存不足跨两跳后必须仍可 errors.Is: request_id=%q want=%v got=%v grpc_code=%s", "p2-insufficient", ports.ErrInsufficientStock, err, status.Code(err))
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/debug/orders/async", gateway.NewAsyncHTTP(h.orderPort, func() (string, error) { return "unused", nil }).PlaceOrder)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/debug/orders/async", strings.NewReader(`{"request_id":"p2-http","product_id":42,"user_id":7,"quantity":1}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(rec, req)
	t.Logf("insufficient round-trip: port_error=%v HTTP status=%d body=%s", err, rec.Code, rec.Body.String())
	if rec.Code != http.StatusConflict {
		t.Fatalf("库存不足 HTTP 语义不许从 409 漂成 500: request_id=%q want_status=%d got_status=%d body=%s", "p2-http", http.StatusConflict, rec.Code, rec.Body.String())
	}
}

func TestM06P2DeadlineReachesInventoryAndReturnsDeadlineExceeded(t *testing.T) {
	observed := make(chan time.Duration, 1)
	h := newRPCHarness(t, &fakeInventory{stock: 5, blockReserve: true, observedBudget: observed}, nil)
	const entryBudget = 80 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), entryBudget)
	defer cancel()
	_, err := h.rawOrder.PlaceOrderAsync(ctx, &orderv1.PlaceOrderRequest{RequestId: "p2-deadline", ProductId: 42, UserId: 7, Quantity: 1})
	remaining := <-observed
	t.Logf("deadline round-trip: entry_budget=%s inventory_observed_remaining=%s grpc_code=%s", entryBudget, remaining, status.Code(err))
	if code := status.Code(err); code != codes.DeadlineExceeded {
		t.Fatalf("deadline 必须按 gRPC code 往返: request_id=%q want_code=%s got_code=%s err=%v", "p2-deadline", codes.DeadlineExceeded, code, err)
	}
	if remaining <= 0 || remaining > entryBudget {
		t.Fatalf("inventory 必须观察到入口剩余预算: entry_budget=%s got_remaining=%s", entryBudget, remaining)
	}
}

func TestM06P2StoppedInventoryFailsClosed(t *testing.T) {
	h := newRPCHarness(t, &fakeInventory{stock: 5}, nil)
	h.stopInventory()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/debug/orders/async", gateway.NewAsyncHTTP(h.orderPort, func() (string, error) { return "unused", nil }).PlaceOrder)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/debug/orders/async", strings.NewReader(`{"request_id":"p2-down","product_id":42,"user_id":7,"quantity":1}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(rec, req)
	t.Logf("inventory stopped: HTTP status=%d body=%s", rec.Code, rec.Body.String())
	if rec.Code == http.StatusOK || rec.Code == http.StatusAccepted {
		t.Fatalf("inventory 挂掉必须 fail closed: request_id=%q forbidden_statuses=[%d,%d] got_status=%d body=%s", "p2-down", http.StatusOK, http.StatusAccepted, rec.Code, rec.Body.String())
	}
}

func TestM06P2ProduceFailureRestoresReservation(t *testing.T) {
	inventory := &fakeInventory{stock: 5}
	h := newRPCHarness(t, inventory, errors.New("kafka unavailable"))
	_, err := h.rawOrder.PlaceOrderAsync(context.Background(), &orderv1.PlaceOrderRequest{RequestId: "p2-restore", ProductId: 42, UserId: 7, Quantity: 2})
	remaining := inventory.remaining()
	t.Logf("produce failure compensation: grpc_code=%s err=%v stock_before=%d stock_after=%d", status.Code(err), err, 5, remaining)
	if err == nil || remaining != 5 {
		t.Fatalf("Reserve 成功但 produce 失败必须 Restore: request_id=%q want_error=true got_error=%v want_stock=%d got_stock=%d", "p2-restore", err, 5, remaining)
	}
}

type rpcHarness struct {
	rawOrder      orderv1.OrderServiceClient
	orderPort     *grpcclient.OrderAdapter
	stopInventory func()
}

func newRPCHarness(t *testing.T, inventory *fakeInventory, produceErr error) *rpcHarness {
	t.Helper()
	invListener := bufconn.Listen(1024 * 1024)
	invServer := grpc.NewServer()
	inventoryv1.RegisterInventoryServiceServer(invServer, grpcadapter.NewInventoryServer(inventory, inventory))
	go func() { _ = invServer.Serve(invListener) }()
	t.Cleanup(invServer.Stop)
	invConn := dialBuf(t, invListener)
	t.Cleanup(func() { _ = invConn.Close() })
	invPort := grpcclient.NewInventoryAdapter(inventoryv1.NewInventoryServiceClient(invConn))

	orderListener := bufconn.Listen(1024 * 1024)
	orderServer := grpc.NewServer()
	produce := func(context.Context, ports.PlaceOrderRequest, time.Time) error { return produceErr }
	orderv1.RegisterOrderServiceServer(orderServer, grpcadapter.NewOrderServer(asyncorder.NewService(invPort, produce)))
	go func() { _ = orderServer.Serve(orderListener) }()
	t.Cleanup(orderServer.Stop)
	orderConn := dialBuf(t, orderListener)
	t.Cleanup(func() { _ = orderConn.Close() })
	raw := orderv1.NewOrderServiceClient(orderConn)
	return &rpcHarness{rawOrder: raw, orderPort: grpcclient.NewOrderAdapter(raw), stopInventory: invServer.Stop}
}

func dialBuf(t *testing.T, listener *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatalf("create bufconn client: target=%q got=%v", "passthrough:///bufconn", err)
	}
	return conn
}

type fakeInventory struct {
	mu             sync.Mutex
	stock          int
	reservations   map[string]ports.ReservationRequest
	blockReserve   bool
	observedBudget chan time.Duration
}

func (f *fakeInventory) GetProduct(context.Context, int64) (*ports.Product, error) {
	return &ports.Product{ID: 42, Name: "test", Stock: f.remaining()}, nil
}

func (f *fakeInventory) Reserve(ctx context.Context, req ports.ReservationRequest) (*ports.Reservation, error) {
	if f.blockReserve {
		deadline, ok := ctx.Deadline()
		if !ok {
			f.observedBudget <- 0
		} else {
			f.observedBudget <- time.Until(deadline)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reservations == nil {
		f.reservations = make(map[string]ports.ReservationRequest)
	}
	if existing, ok := f.reservations[req.RequestID]; ok {
		return &ports.Reservation{RequestID: existing.RequestID, ProductID: existing.ProductID, Quantity: existing.Quantity, Remaining: int64(f.stock), AlreadyReserved: true}, nil
	}
	if f.stock < req.Quantity {
		return nil, ports.ErrInsufficientStock
	}
	f.stock -= req.Quantity
	f.reservations[req.RequestID] = req
	return &ports.Reservation{RequestID: req.RequestID, ProductID: req.ProductID, Quantity: req.Quantity, Remaining: int64(f.stock)}, nil
}

func (f *fakeInventory) Restore(_ context.Context, req ports.ReservationRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reservations == nil {
		return ports.ErrReservationNotFound
	}
	if _, ok := f.reservations[req.RequestID]; !ok {
		return ports.ErrReservationNotFound
	}
	f.stock += req.Quantity
	delete(f.reservations, req.RequestID)
	return nil
}

func (f *fakeInventory) remaining() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stock
}
