// 已就位（AI 生成）：静态地址、gRPC server 与优雅关闭是 p2 样板；学习点在 adapter 映射。
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nbhaohao/go-seckill/internal/adapter/grpcclient"
	grpcadapter "github.com/nbhaohao/go-seckill/internal/adapter/grpcserver"
	"github.com/nbhaohao/go-seckill/internal/asyncorder"
	"github.com/nbhaohao/go-seckill/internal/mq"
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

func main() {
	inventoryConn, err := grpc.NewClient(envOr("INVENTORY_GRPC_ADDR", "127.0.0.1:9082"), grpc.WithTransportCredentials(insecure.NewCredentials()))
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

	lis, err := net.Listen("tcp", envOr("ORDER_GRPC_ADDR", ":9081"))
	if err != nil {
		log.Fatalf("listen order grpc: %v", err)
	}
	server := grpc.NewServer()
	orderv1.RegisterOrderServiceServer(server, grpcadapter.NewOrderServer(asyncorder.NewService(inventory, produce)))

	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(lis) }()
	log.Printf("order-service gRPC listening on %s", lis.Addr())

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case <-sigCtx.Done():
		force := time.AfterFunc(10*time.Second, server.Stop)
		server.GracefulStop()
		force.Stop()
	case err := <-serveErr:
		if !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("serve order grpc: %v", err)
		}
	}
}
