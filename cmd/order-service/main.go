// 已就位（AI 生成）：p5 同一二进制按 ORDER_VERSION 注册进 v1/v2 前缀，并可给 v2 注入故障。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/nbhaohao/go-seckill/internal/adapter/grpcclient"
	grpcadapter "github.com/nbhaohao/go-seckill/internal/adapter/grpcserver"
	"github.com/nbhaohao/go-seckill/internal/asyncorder"
	discovery "github.com/nbhaohao/go-seckill/internal/discovery/etcd"
	rpcinterceptor "github.com/nbhaohao/go-seckill/internal/interceptor"
	"github.com/nbhaohao/go-seckill/internal/mq"
	"github.com/nbhaohao/go-seckill/internal/overload"
	inventoryv1 "github.com/nbhaohao/go-seckill/internal/pb/inventoryv1"
	orderv1 "github.com/nbhaohao/go-seckill/internal/pb/orderv1"
	"github.com/nbhaohao/go-seckill/internal/ports"
)

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return value
}

// orderPool 已就位（AI 生成）：版本只决定两个固定逻辑服务前缀与本地默认端口。
func orderPool(version string) (service, listenAddr, advertiseAddr string) {
	if version == "v2" {
		return "order-canary", ":9083", "127.0.0.1:9083"
	}
	return "order", ":9081", "127.0.0.1:9081"
}

// v2FaultInterceptor 已就位（AI 生成）：只服务本地灰度回滚实验，不是动态发布控制面。
func v2FaultInterceptor(version, mode string, delay time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if version != "v2" || info.FullMethod != orderv1.OrderService_PlaceOrderAsync_FullMethodName {
			return handler(ctx, req)
		}
		if mode == "delay" && delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return nil, status.FromContextError(ctx.Err()).Err()
			case <-timer.C:
			}
		}
		if mode == "error" {
			return nil, status.Error(codes.Unavailable, "injected order v2 failure")
		}
		return handler(ctx, req)
	}
}

func main() {
	version := envOr("ORDER_VERSION", "v1")
	serviceName, defaultListen, defaultAdvertise := orderPool(version)
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(envOr("ETCD_ENDPOINTS", "127.0.0.1:2379"), ","), DialTimeout: 3 * time.Second})
	if err != nil {
		log.Fatalf("connect etcd: %v", err)
	}
	defer etcdClient.Close()
	resolverBuilder := discovery.NewBuilder(etcdClient, nil)
	inventoryBreaker := overload.NewBreaker(overload.BreakerPolicy{
		FailureThreshold: envIntOr("INVENTORY_RPC_BREAKER_FAILURE_THRESHOLD", 5),
		OpenFor:          time.Duration(envIntOr("INVENTORY_RPC_BREAKER_OPEN_MS", 2000)) * time.Millisecond,
		HalfOpenProbes:   1,
	})
	inventoryConn, err := grpc.NewClient(discovery.Scheme+":///inventory",
		grpc.WithResolvers(resolverBuilder),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
		grpc.WithChainUnaryInterceptor(
			rpcinterceptor.UnaryBreaker(inventoryBreaker),
			rpcinterceptor.UnaryRetry(
				envIntOr("INVENTORY_RPC_MAX_ATTEMPTS", 3),
				time.Duration(envIntOr("INVENTORY_RPC_BACKOFF_MS", 25))*time.Millisecond,
				map[string]bool{
					inventoryv1.InventoryService_Reserve_FullMethodName: true,
					inventoryv1.InventoryService_Restore_FullMethodName: true,
				},
			),
		),
	)
	if err != nil {
		log.Fatalf("inventory grpc client: %v", err)
	}
	defer inventoryConn.Close()
	inventory := grpcclient.NewInventoryAdapter(inventoryv1.NewInventoryServiceClient(inventoryConn))

	brokers := strings.Split(envOr("KAFKA_BROKERS", "127.0.0.1:9092"), ",")
	producer, err := mq.NewProducer(brokers...)
	if err != nil {
		log.Fatalf("new Kafka producer: %v", err)
	}
	defer producer.Close()
	topic := envOr("KAFKA_TOPIC", mq.OrderCreatedTopic)
	produce := func(ctx context.Context, req ports.PlaceOrderRequest, acceptedAt time.Time) error {
		value, err := json.Marshal(mq.OrderCreated{RequestID: req.RequestID, ProductID: req.ProductID, UserID: req.UserID, Quantity: req.Quantity, AcceptedAt: acceptedAt})
		if err != nil {
			return err
		}
		return producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(strconv.FormatInt(req.ProductID, 10)), Value: value}).FirstErr()
	}

	lis, err := net.Listen("tcp", envOr("ORDER_GRPC_ADDR", defaultListen))
	if err != nil {
		log.Fatalf("listen order grpc: %v", err)
	}
	serverBucket := overload.NewTokenBucket(
		envIntOr("ORDER_RPC_RATE_LIMIT_CAPACITY", 500),
		float64(envIntOr("ORDER_RPC_RATE_LIMIT_REFILL_PER_SECOND", 500)),
		nil,
	)
	faultMode := envOr("ORDER_V2_FAULT_MODE", "off")
	faultDelay := time.Duration(envIntOr("ORDER_V2_FAULT_DELAY_MS", 0)) * time.Millisecond
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(
		rpcinterceptor.UnaryRateLimit(serverBucket, map[string]bool{orderv1.OrderService_PlaceOrderAsync_FullMethodName: true}),
		v2FaultInterceptor(version, faultMode, faultDelay),
	))
	orderv1.RegisterOrderServiceServer(server, grpcadapter.NewOrderServer(asyncorder.NewService(inventory, produce)))

	hostname, _ := os.Hostname()
	instanceID := envOr("ORDER_INSTANCE_ID", hostname+"-"+strconv.Itoa(os.Getpid()))
	registration, err := discovery.Register(context.Background(), etcdClient, serviceName, instanceID, envOr("ORDER_ADVERTISE_ADDR", defaultAdvertise), int64(envIntOr("ETCD_LEASE_TTL", 10)))
	if err != nil {
		log.Fatalf("register order version=%s service=%s: %v", version, serviceName, err)
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(lis) }()
	log.Printf("order-service version=%s service=%s gRPC listening on %s fault=%s delay=%s", version, serviceName, lis.Addr(), faultMode, faultDelay)

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case <-sigCtx.Done():
		revokeCtx, revokeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := registration.Close(revokeCtx); err != nil {
			log.Printf("revoke order registration: %v", err)
		}
		revokeCancel()
		force := time.AfterFunc(10*time.Second, server.Stop)
		server.GracefulStop()
		force.Stop()
	case err := <-serveErr:
		if !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("serve order grpc: %v", err)
		}
	}
}
