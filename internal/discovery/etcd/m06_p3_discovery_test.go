package etcd_test

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	discovery "github.com/nbhaohao/go-seckill/internal/discovery/etcd"
	"github.com/nbhaohao/go-seckill/internal/testutil"
)

func TestM06P3LeaseExpiryRemovesAddress(t *testing.T) {
	client := testutil.OpenTestEtcd(t)
	service := uniqueService(t, "lease")
	serverA := startHealthServer(t, grpc_health_v1.HealthCheckResponse_SERVING)
	serverB := startHealthServer(t, grpc_health_v1.HealthCheckResponse_SERVING)

	registration, err := discovery.Register(context.Background(), client, service, "a", serverA.addr, 10)
	if err != nil {
		t.Fatalf("register keepalive instance addr=%q: got %v", serverA.addr, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = registration.Close(ctx)
	})
	const expiringTTL = int64(3)
	putWithLease(t, client, service, "b", serverB.addr, expiringTTL, false)

	observer := newAddressObserver()
	cc := &recordingClientConn{observer: observer}
	r, err := discovery.NewBuilder(client, observer.record).Build(mustParseTarget(t, discovery.Scheme+":///"+service), cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("build resolver service=%q: got %v", service, err)
	}
	t.Cleanup(r.Close)
	wantBefore := sorted(serverA.addr, serverB.addr)
	before := observer.waitFor(t, wantBefore, 5*time.Second)
	started := time.Now()
	after := observer.waitFor(t, []string{serverA.addr}, 8*time.Second)
	t.Logf("lease TTL=%ds addresses before=%v after=%v convergence=%s", expiringTTL, before, after, time.Since(started).Round(time.Millisecond))
	if !reflect.DeepEqual(after, []string{serverA.addr}) {
		t.Fatalf("addresses after lease expiry: got %v want %v", after, []string{serverA.addr})
	}
}

func TestM06P3RoundRobinStopsSelectingExpiredInstance(t *testing.T) {
	client := testutil.OpenTestEtcd(t)
	service := uniqueService(t, "round-robin")
	serverA := startHealthServer(t, grpc_health_v1.HealthCheckResponse_SERVING)
	serverB := startHealthServer(t, grpc_health_v1.HealthCheckResponse_SERVING)
	putWithLease(t, client, service, "a", serverA.addr, 10, true)
	putWithLease(t, client, service, "b", serverB.addr, 3, false)

	observer := newAddressObserver()
	conn := dialDiscovered(t, client, service, observer)
	healthClient := grpc_health_v1.NewHealthClient(conn)
	observer.waitFor(t, sorted(serverA.addr, serverB.addr), 5*time.Second)
	for i := 0; i < 10; i++ {
		checkHealth(t, healthClient)
	}
	beforeA, beforeB := serverA.hits.Load(), serverB.hits.Load()
	if beforeA < 1 || beforeB < 1 {
		t.Fatalf("round_robin hits before expiry: got a=%d b=%d want both >=1", beforeA, beforeB)
	}
	started := time.Now()
	afterAddresses := observer.waitFor(t, []string{serverA.addr}, 8*time.Second)
	waitUntilNotSelected(t, healthClient, &serverB.hits)
	deadBefore := serverB.hits.Load()
	for i := 0; i < 10; i++ {
		checkHealth(t, healthClient)
	}
	afterA, afterB := serverA.hits.Load(), serverB.hits.Load()
	deadDelta := afterB - deadBefore
	t.Logf("round_robin hits before={a:%d b:%d} after={a:%d b:%d} addresses=%v convergence=%s dead_delta=%d", beforeA, beforeB, afterA, afterB, afterAddresses, time.Since(started).Round(time.Millisecond), deadDelta)
	if deadDelta != 0 {
		t.Fatalf("expired instance hit delta: got %d want 0 (before=%d after=%d)", deadDelta, deadBefore, afterB)
	}
}

func waitUntilNotSelected(t *testing.T, client grpc_health_v1.HealthClient, deadHits *atomic.Int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	stable := 0
	previous := deadHits.Load()
	for stable < 4 {
		checkHealth(t, client)
		current := deadHits.Load()
		if current == previous {
			stable++
		} else {
			stable = 0
			previous = current
		}
		if time.Now().After(deadline) {
			t.Fatalf("wait balancer to drop expired instance: dead hits still changed, latest=%d", current)
		}
	}
}

func TestM06P3LeaseLivenessDoesNotImplyHealthReadiness(t *testing.T) {
	client := testutil.OpenTestEtcd(t)
	service := uniqueService(t, "health")
	server := startHealthServer(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	putWithLease(t, client, service, "not-ready", server.addr, 10, true)

	observer := newAddressObserver()
	conn := dialDiscovered(t, client, service, observer)
	addresses := observer.waitFor(t, []string{server.addr}, 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true))
	if err != nil {
		t.Fatalf("health Check registered addr=%q: got %v", server.addr, err)
	}
	t.Logf("lease-visible addresses=%v health_status=%s rpc_error=%v", addresses, response.GetStatus(), err)
	if response.GetStatus() != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("health readiness for lease-visible instance: got %s want %s", response.GetStatus(), grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	}
}

type countingHealthServer struct {
	addr   string
	hits   atomic.Int64
	server *grpc.Server
}

func startHealthServer(t *testing.T, status grpc_health_v1.HealthCheckResponse_ServingStatus) *countingHealthServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen test health server: got %v", err)
	}
	result := &countingHealthServer{addr: lis.Addr().String()}
	result.server = grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == grpc_health_v1.Health_Check_FullMethodName {
			result.hits.Add(1)
		}
		return handler(ctx, req)
	}))
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", status)
	grpc_health_v1.RegisterHealthServer(result.server, healthServer)
	go func() { _ = result.server.Serve(lis) }()
	t.Cleanup(result.server.Stop)
	return result
}

func dialDiscovered(t *testing.T, client *clientv3.Client, service string, observer *addressObserver) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(discovery.Scheme+":///"+service,
		grpc.WithResolvers(discovery.NewBuilder(client, observer.record)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
	)
	if err != nil {
		t.Fatalf("new discovered grpc client service=%q: got %v", service, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	conn.Connect()
	return conn
}

func checkHealth(t *testing.T, client grpc_health_v1.HealthClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true))
	if err != nil || response.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("health Check: got status=%s err=%v want status=%s err=nil", response.GetStatus(), err, grpc_health_v1.HealthCheckResponse_SERVING)
	}
}

func putWithLease(t *testing.T, client *clientv3.Client, service, instanceID, addr string, ttl int64, keepalive bool) clientv3.LeaseID {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	lease, err := client.Grant(ctx, ttl)
	if err != nil {
		t.Fatalf("grant lease ttl=%d: got %v", ttl, err)
	}
	key := fmt.Sprintf("/seckill/services/%s/%s", service, instanceID)
	if _, err := client.Put(ctx, key, addr, clientv3.WithLease(lease.ID)); err != nil {
		t.Fatalf("put registration key=%q addr=%q lease=%d: got %v", key, addr, lease.ID, err)
	}
	keepCtx, keepCancel := context.WithCancel(context.Background())
	if keepalive {
		responses, err := client.KeepAlive(keepCtx, lease.ID)
		if err != nil {
			keepCancel()
			t.Fatalf("keepalive lease=%d: got %v", lease.ID, err)
		}
		go func() {
			for range responses {
			}
		}()
	}
	t.Cleanup(func() {
		keepCancel()
		if !keepalive {
			return
		}
		revokeCtx, revokeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer revokeCancel()
		_, _ = client.Revoke(revokeCtx, lease.ID)
	})
	return lease.ID
}

type addressObserver struct {
	mu      sync.Mutex
	latest  []string
	changed chan struct{}
}

func newAddressObserver() *addressObserver {
	return &addressObserver{changed: make(chan struct{}, 1)}
}

func (o *addressObserver) record(addresses []string) {
	o.mu.Lock()
	o.latest = append([]string(nil), addresses...)
	o.mu.Unlock()
	select {
	case o.changed <- struct{}{}:
	default:
	}
}

func (o *addressObserver) waitFor(t *testing.T, want []string, timeout time.Duration) []string {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		o.mu.Lock()
		got := append([]string(nil), o.latest...)
		o.mu.Unlock()
		if reflect.DeepEqual(got, want) {
			return got
		}
		select {
		case <-o.changed:
		case <-deadline.C:
			t.Fatalf("wait resolver addresses: got %v want %v after %s", got, want, timeout)
		}
	}
}

type recordingClientConn struct {
	observer *addressObserver
}

func (c *recordingClientConn) UpdateState(state resolver.State) error {
	addresses := make([]string, len(state.Addresses))
	for i, address := range state.Addresses {
		addresses[i] = address.Addr
	}
	c.observer.record(addresses)
	return nil
}
func (*recordingClientConn) ReportError(error)                                    {}
func (*recordingClientConn) NewAddress([]resolver.Address)                        {}
func (*recordingClientConn) NewServiceConfig(string)                              {}
func (*recordingClientConn) ParseServiceConfig(string) *serviceconfig.ParseResult { return nil }

func uniqueService(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("inventory-%s-%d", suffix, time.Now().UnixNano())
}

func sorted(values ...string) []string {
	sort.Strings(values)
	return values
}

func mustParseTarget(t *testing.T, raw string) resolver.Target {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse resolver target %q: got %v", raw, err)
	}
	return resolver.Target{URL: *parsed}
}
