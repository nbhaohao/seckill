package etcd

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const serviceRoot = "/seckill/services/"

type Registration struct {
	client    *clientv3.Client
	leaseID   clientv3.LeaseID
	key       string
	cancel    context.CancelFunc
	drainDone chan struct{}
}

func servicePrefix(service string) string { return serviceRoot + service + "/" }

// Register 是 sk-m06 p3 S1：用 lease 把一个进程地址发布到服务目录，并持续续租。
// 1. 为什么先 Grant 再 Put：key 必须从第一次出现起就绑定 TTL，不能留下永久脏地址窗口。
// 2. 为什么 Put 的 value 是 addr：resolver 只消费可拨号地址，不把 etcd 顺手变成配置中心。
// 3. 为什么必须持续消费 KeepAlive channel：续租响应无人接收会让客户端背压，最终丢掉续租。
// AI 将在 p3 学习时分切片实现。
func Register(ctx context.Context, client *clientv3.Client, service, instanceID, addr string, ttl int64) (*Registration, error) {
	panic("TODO: sk-m06 p3 Register")
}

// Close 已就位（AI 生成）：进程退出时先停 keepalive，再显式 revoke；TTL 仍是崩溃兜底。
func (r *Registration) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.cancel()
	select {
	case <-r.drainDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	_, err := r.client.Revoke(ctx, r.leaseID)
	if err != nil {
		return fmt.Errorf("discovery: revoke key=%s lease=%d: %w", r.key, r.leaseID, err)
	}
	return nil
}

func revokeBestEffort(client *clientv3.Client, leaseID clientv3.LeaseID) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = client.Revoke(ctx, leaseID)
}
