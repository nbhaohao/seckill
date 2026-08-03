// 已就位（AI 生成）：bufconn 真 gRPC server 让 p4 的次数、status 与 fail-closed 都可观察。
package interceptor_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/nbhaohao/go-seckill/internal/adapter/grpcclient"
	"github.com/nbhaohao/go-seckill/internal/asyncorder"
	rpcinterceptor "github.com/nbhaohao/go-seckill/internal/interceptor"
	"github.com/nbhaohao/go-seckill/internal/overload"
	inventoryv1 "github.com/nbhaohao/go-seckill/internal/pb/inventoryv1"
	orderv1 "github.com/nbhaohao/go-seckill/internal/pb/orderv1"
	"github.com/nbhaohao/go-seckill/internal/ports"
)

func TestM06P4RetryStormBoundedByAttemptsAndDeadline(t *testing.T) {
	t.Run("maximum attempts", func(t *testing.T) {
		inventory := &countingInventoryServer{reserveErr: status.Error(codes.Unavailable, "inventory unavailable")}
		conn := dialBufconn(t,
			rpcinterceptor.UnaryRetry(3, 5*time.Millisecond, map[string]bool{inventoryv1.InventoryService_Reserve_FullMethodName: true}),
			nil,
			func(server *grpc.Server) { inventoryv1.RegisterInventoryServiceServer(server, inventory) },
		)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := inventoryv1.NewInventoryServiceClient(conn).Reserve(ctx, reserveRequest("retry-max"))
		hits := inventory.reserveHits.Load()
		t.Logf("retry max_attempts=3 server_attempts=%d status=%s", hits, status.Code(err))
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("retry terminal status: got %s want %s (attempts=%d)", status.Code(err), codes.Unavailable, hits)
		}
		if hits != 3 {
			t.Fatalf("retry storm bound: got server attempts=%d want 3", hits)
		}
	})

	t.Run("entry deadline", func(t *testing.T) {
		inventory := &countingInventoryServer{reserveErr: status.Error(codes.Unavailable, "inventory unavailable")}
		conn := dialBufconn(t,
			rpcinterceptor.UnaryRetry(10, 100*time.Millisecond, map[string]bool{inventoryv1.InventoryService_Reserve_FullMethodName: true}),
			nil,
			func(server *grpc.Server) { inventoryv1.RegisterInventoryServiceServer(server, inventory) },
		)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		started := time.Now()
		_, err := inventoryv1.NewInventoryServiceClient(conn).Reserve(ctx, reserveRequest("retry-deadline"))
		hits := inventory.reserveHits.Load()
		elapsed := time.Since(started).Round(time.Millisecond)
		t.Logf("retry max_attempts=10 server_attempts=%d status=%s deadline=30ms elapsed=%s", hits, status.Code(err), elapsed)
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("retry deadline status: got %s want %s (attempts=%d elapsed=%s)", status.Code(err), codes.DeadlineExceeded, hits, elapsed)
		}
		if hits != 1 {
			t.Fatalf("retry after deadline: got server attempts=%d want 1", hits)
		}
	})
}

func TestM06P4NonIdempotentCallIsNotRetriedAndServerLimitRejects(t *testing.T) {
	order := &countingOrderServer{err: status.Error(codes.Unavailable, "order unavailable")}
	conn := dialBufconn(t,
		rpcinterceptor.UnaryRetry(5, time.Millisecond, map[string]bool{inventoryv1.InventoryService_Reserve_FullMethodName: true}),
		nil,
		func(server *grpc.Server) { orderv1.RegisterOrderServiceServer(server, order) },
	)
	_, err := orderv1.NewOrderServiceClient(conn).PlaceOrderAsync(context.Background(), orderRequest("non-idempotent"))
	hits := order.hits.Load()
	t.Logf("non_idempotent method=%s server_attempts=%d status=%s", orderv1.OrderService_PlaceOrderAsync_FullMethodName, hits, status.Code(err))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("non-idempotent terminal status: got %s want %s (attempts=%d)", status.Code(err), codes.Unavailable, hits)
	}
	if hits != 1 {
		t.Fatalf("non-idempotent retry count: got server attempts=%d want 1", hits)
	}

	limitedOrder := &countingOrderServer{}
	bucket := overload.NewTokenBucket(1, 0, nil)
	limitedConn := dialBufconn(t,
		nil,
		rpcinterceptor.UnaryRateLimit(bucket, map[string]bool{orderv1.OrderService_PlaceOrderAsync_FullMethodName: true}),
		func(server *grpc.Server) { orderv1.RegisterOrderServiceServer(server, limitedOrder) },
	)
	client := orderv1.NewOrderServiceClient(limitedConn)
	if _, err := client.PlaceOrderAsync(context.Background(), orderRequest("token-1")); err != nil {
		t.Fatalf("first rate-limited method call: got status=%s want OK", status.Code(err))
	}
	_, err = client.PlaceOrderAsync(context.Background(), orderRequest("token-2"))
	t.Logf("server_limit capacity=1 handler_hits=%d second_status=%s", limitedOrder.hits.Load(), status.Code(err))
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("server rate limit status: got %s want %s (handler hits=%d)", status.Code(err), codes.ResourceExhausted, limitedOrder.hits.Load())
	}
}

func TestM06P4OpenBreakerFailsInventoryWriteClosed(t *testing.T) {
	breaker := overload.NewBreaker(overload.BreakerPolicy{FailureThreshold: 1, OpenFor: time.Hour, HalfOpenProbes: 1})
	_ = breaker.Do(context.Background(), func(context.Context) error { return errors.New("seed dependency failure") })
	inventory := &countingInventoryServer{}
	conn := dialBufconn(t,
		rpcinterceptor.UnaryBreaker(breaker),
		nil,
		func(server *grpc.Server) { inventoryv1.RegisterInventoryServiceServer(server, inventory) },
	)
	adapter := grpcclient.NewInventoryAdapter(inventoryv1.NewInventoryServiceClient(conn))
	var produced atomic.Int64
	service := asyncorder.NewService(adapter, func(context.Context, ports.PlaceOrderRequest, time.Time) error {
		produced.Add(1)
		return nil
	})
	accepted, err := service.PlaceOrderAsync(context.Background(), ports.PlaceOrderRequest{RequestID: "breaker-open", ProductID: 1, UserID: 7, Quantity: 1})
	t.Logf("breaker_state=%s accepted=%v status=%s inventory_hits=%d produce_hits=%d", breaker.State(), accepted, status.Code(err), inventory.reserveHits.Load(), produced.Load())
	if status.Code(err) != codes.Unavailable || accepted != nil {
		t.Fatalf("open breaker write result: got accepted=%v status=%s want accepted=nil status=%s", accepted, status.Code(err), codes.Unavailable)
	}
	if inventory.reserveHits.Load() != 0 || produced.Load() != 0 {
		t.Fatalf("open breaker fail-closed side effects: got inventory hits=%d produce hits=%d want both 0", inventory.reserveHits.Load(), produced.Load())
	}
}

type countingInventoryServer struct {
	inventoryv1.UnimplementedInventoryServiceServer
	reserveHits atomic.Int64
	reserveErr  error
}

func (s *countingInventoryServer) Reserve(_ context.Context, req *inventoryv1.ReserveRequest) (*inventoryv1.Reservation, error) {
	s.reserveHits.Add(1)
	if s.reserveErr != nil {
		return nil, s.reserveErr
	}
	return &inventoryv1.Reservation{RequestId: req.GetRequestId(), ProductId: req.GetProductId(), Quantity: req.GetQuantity(), Remaining: 9}, nil
}

type countingOrderServer struct {
	orderv1.UnimplementedOrderServiceServer
	hits atomic.Int64
	err  error
}

func (s *countingOrderServer) PlaceOrderAsync(_ context.Context, req *orderv1.PlaceOrderRequest) (*orderv1.AcceptedOrder, error) {
	s.hits.Add(1)
	if s.err != nil {
		return nil, s.err
	}
	return &orderv1.AcceptedOrder{RequestId: req.GetRequestId(), Status: 202}, nil
}

func dialBufconn(t *testing.T, clientInterceptor grpc.UnaryClientInterceptor, serverInterceptor grpc.UnaryServerInterceptor, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	var serverOptions []grpc.ServerOption
	if serverInterceptor != nil {
		serverOptions = append(serverOptions, grpc.UnaryInterceptor(serverInterceptor))
	}
	server := grpc.NewServer(serverOptions...)
	register(server)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	clientOptions := []grpc.DialOption{grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials())}
	if clientInterceptor != nil {
		clientOptions = append(clientOptions, grpc.WithUnaryInterceptor(clientInterceptor))
	}
	conn, err := grpc.NewClient("passthrough:///bufnet", clientOptions...)
	if err != nil {
		t.Fatalf("new bufconn client: got %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	waitForReady(t, conn)
	return conn
}

func waitForReady(t *testing.T, conn *grpc.ClientConn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return
		}
		if !conn.WaitForStateChange(ctx, state) {
			t.Fatalf("wait bufconn ready: got state=%s err=%v", conn.GetState(), ctx.Err())
		}
	}
}

func reserveRequest(id string) *inventoryv1.ReserveRequest {
	return &inventoryv1.ReserveRequest{RequestId: id, ProductId: 1, Quantity: 1}
}

func orderRequest(id string) *orderv1.PlaceOrderRequest {
	return &orderv1.PlaceOrderRequest{RequestId: id, ProductId: 1, UserId: 7, Quantity: 1}
}
