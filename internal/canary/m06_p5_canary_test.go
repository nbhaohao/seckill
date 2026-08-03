// 已就位（AI 生成）：两个 bufconn order server 让版本命中与回滚增量可直接计数。
package canary_test

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/nbhaohao/go-seckill/internal/canary"
	orderv1 "github.com/nbhaohao/go-seckill/internal/pb/orderv1"
	"github.com/nbhaohao/go-seckill/internal/ports"
)

func TestM06P5SameUserStaysInSameVersion(t *testing.T) {
	const percent = 25
	const repeats = 20
	counts := map[canary.Version]int{canary.VersionV1: 0, canary.VersionV2: 0}
	for userID := int64(1); userID <= 500; userID++ {
		first := canary.SelectVersion(userID, percent)
		counts[first]++
		for repeat := 1; repeat < repeats; repeat++ {
			if got := canary.SelectVersion(userID, percent); got != first {
				t.Fatalf("sticky cohort user_id=%d percent=%d repeat=%d: got %s want %s", userID, percent, repeat, got, first)
			}
		}
	}
	t.Logf("sticky distribution users=500 repeats=%d percent=%d hits={v1:%d v2:%d}", repeats, percent, counts[canary.VersionV1], counts[canary.VersionV2])
	if counts[canary.VersionV1] == 0 || counts[canary.VersionV2] == 0 {
		t.Fatalf("middle percentage cohort coverage: got hits=%v want both versions non-zero", counts)
	}
	for _, userID := range []int64{1, 7, 42, 999} {
		if got := canary.SelectVersion(userID, 0); got != canary.VersionV1 {
			t.Fatalf("rollback boundary user_id=%d percent=0: got %s want %s", userID, got, canary.VersionV1)
		}
		if got := canary.SelectVersion(userID, 100); got != canary.VersionV2 {
			t.Fatalf("full canary boundary user_id=%d percent=100: got %s want %s", userID, got, canary.VersionV2)
		}
	}
}

func TestM06P5NonCanaryUsersNeverReachV2(t *testing.T) {
	v1 := startOrderPool(t, false)
	v2 := startOrderPool(t, false)
	const percent = 20
	router := canary.NewRouter(v1.port, v2.port, percent)
	users := findUsers(t, percent, canary.VersionV1, 40)
	for i, userID := range users {
		accepted, err := router.PlaceOrderAsync(context.Background(), request(userID, i))
		if err != nil || accepted == nil {
			t.Fatalf("non-canary request user_id=%d: got accepted=%v status=%s want accepted non-nil status=OK", userID, accepted, status.Code(err))
		}
	}
	t.Logf("non-canary cohort percent=%d requests=%d hits={v1:%d v2:%d}", percent, len(users), v1.hits.Load(), v2.hits.Load())
	if v2.hits.Load() != 0 {
		t.Fatalf("non-canary traffic leaked to v2: got v2 hits=%d want 0 (v1 hits=%d)", v2.hits.Load(), v1.hits.Load())
	}
}

func TestM06P5PercentZeroRollsBackAfterV2Failure(t *testing.T) {
	v1 := startOrderPool(t, false)
	v2 := startOrderPool(t, true)
	failedRouter := canary.NewRouter(v1.port, v2.port, 100)
	_, err := failedRouter.PlaceOrderAsync(context.Background(), request(7, 0))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("injected v2 failure: got status=%s want %s", status.Code(err), codes.Unavailable)
	}
	v2AtRollback := v2.hits.Load()

	rolledBack := canary.NewRouter(v1.port, v2.port, 0)
	for i := 0; i < 30; i++ {
		accepted, err := rolledBack.PlaceOrderAsync(context.Background(), request(int64(i+1), i+1))
		if err != nil || accepted == nil {
			t.Fatalf("rollback request index=%d: got accepted=%v status=%s want accepted non-nil status=OK", i, accepted, status.Code(err))
		}
	}
	t.Logf("rollback percent_before=100 percent_after=0 v2_failure_status=%s hits={v1:%d v2_before:%d v2_after:%d}", status.Code(err), v1.hits.Load(), v2AtRollback, v2.hits.Load())
	if v2.hits.Load() != v2AtRollback {
		t.Fatalf("v2 hits after rollback: got before=%d after=%d want no growth", v2AtRollback, v2.hits.Load())
	}
}

type orderPool struct {
	port *protoOrderPort
	hits *atomic.Int64
}

func startOrderPool(t *testing.T, fail bool) orderPool {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	service := &countingOrderServer{fail: fail}
	orderv1.RegisterOrderServiceServer(server, service)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///order-pool",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("new order pool client: got %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	waitForReady(t, conn)
	return orderPool{port: &protoOrderPort{client: orderv1.NewOrderServiceClient(conn)}, hits: &service.hits}
}

type countingOrderServer struct {
	orderv1.UnimplementedOrderServiceServer
	hits atomic.Int64
	fail bool
}

func (s *countingOrderServer) PlaceOrderAsync(_ context.Context, req *orderv1.PlaceOrderRequest) (*orderv1.AcceptedOrder, error) {
	s.hits.Add(1)
	if s.fail {
		return nil, status.Error(codes.Unavailable, "injected v2 failure")
	}
	return &orderv1.AcceptedOrder{RequestId: req.GetRequestId(), Status: 202, AcceptedAt: timestamppb.Now()}, nil
}

type protoOrderPort struct {
	client orderv1.OrderServiceClient
}

func (p *protoOrderPort) PlaceOrderAsync(ctx context.Context, req ports.PlaceOrderRequest) (*ports.AcceptedOrder, error) {
	got, err := p.client.PlaceOrderAsync(ctx, &orderv1.PlaceOrderRequest{RequestId: req.RequestID, ProductId: req.ProductID, UserId: req.UserID, Quantity: int32(req.Quantity)})
	if err != nil {
		return nil, err
	}
	return &ports.AcceptedOrder{RequestID: got.GetRequestId(), Status: int(got.GetStatus()), Accepted: got.GetAcceptedAt().AsTime()}, nil
}

func findUsers(t *testing.T, percent int, version canary.Version, count int) []int64 {
	t.Helper()
	users := make([]int64, 0, count)
	for userID := int64(1); len(users) < count && userID < 100_000; userID++ {
		if canary.SelectVersion(userID, percent) == version {
			users = append(users, userID)
		}
	}
	if len(users) != count {
		t.Fatalf("find cohort users percent=%d version=%s: got %d want %d", percent, version, len(users), count)
	}
	return users
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
			t.Fatalf("wait order pool ready: got state=%s err=%v", conn.GetState(), ctx.Err())
		}
	}
}

func request(userID int64, sequence int) ports.PlaceOrderRequest {
	return ports.PlaceOrderRequest{RequestID: "p5-" + strconv.Itoa(sequence), ProductID: 1, UserID: userID, Quantity: 1}
}
