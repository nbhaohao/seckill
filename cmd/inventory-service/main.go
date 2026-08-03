// 已就位（AI 生成）：依赖接线、gRPC server 与优雅关闭是 p2 样板；学习点在 adapter 映射。
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"

	grpcadapter "github.com/nbhaohao/go-seckill/internal/adapter/grpcserver"
	"github.com/nbhaohao/go-seckill/internal/cache"
	"github.com/nbhaohao/go-seckill/internal/dbconn"
	"github.com/nbhaohao/go-seckill/internal/inventory"
	inventoryv1 "github.com/nbhaohao/go-seckill/internal/pb/inventoryv1"
	"github.com/nbhaohao/go-seckill/internal/redisconn"
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

func main() {
	db, err := dbconn.Open(dbconn.Config{Host: envOr("DB_HOST", "127.0.0.1"), Port: envIntOr("DB_PORT", 3306), User: envOr("DB_USER", "seckill"), Password: envOr("DB_PASSWORD", "seckill"), DBName: envOr("DB_NAME", "seckill")})
	if err != nil {
		log.Fatalf("connect mysql: %v", err)
	}
	defer db.Close()

	connectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	rdb, err := redisconn.Open(connectCtx, redisconn.Config{Addr: envOr("REDIS_ADDR", "127.0.0.1:6379"), Password: envOr("REDIS_PASSWORD", ""), DB: envIntOr("REDIS_DB", 0)})
	cancel()
	if err != nil {
		log.Fatalf("connect redis: %v", err)
	}
	defer rdb.Close()

	products := cache.New(rdb, cache.NewSQLProductRepo(db), cache.DefaultOptions())
	service := inventory.NewService(rdb, products)
	lis, err := net.Listen("tcp", envOr("INVENTORY_GRPC_ADDR", ":9082"))
	if err != nil {
		log.Fatalf("listen inventory grpc: %v", err)
	}
	server := grpc.NewServer()
	inventoryv1.RegisterInventoryServiceServer(server, grpcadapter.NewInventoryServer(service, service))

	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(lis) }()
	log.Printf("inventory-service gRPC listening on %s", lis.Addr())

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case <-sigCtx.Done():
		force := time.AfterFunc(10*time.Second, server.Stop)
		server.GracefulStop()
		force.Stop()
	case err := <-serveErr:
		if !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("serve inventory grpc: %v", err)
		}
	}
}
