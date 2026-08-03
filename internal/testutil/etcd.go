// 已就位（AI 生成）：p3 集成测试连接 docker-compose 里的真实 etcd。
package testutil

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func OpenTestEtcd(t *testing.T) *clientv3.Client {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{envOr("TEST_ETCD_ENDPOINT", "127.0.0.1:2379")}, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("open test etcd endpoint=%q: got %v", envOr("TEST_ETCD_ENDPOINT", "127.0.0.1:2379"), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := client.Get(ctx, "/m06-p3-readiness"); err != nil {
		_ = client.Close()
		t.Fatalf("ping test etcd endpoint=%q: got %v（先 docker compose up -d etcd）", envOr("TEST_ETCD_ENDPOINT", "127.0.0.1:2379"), err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
