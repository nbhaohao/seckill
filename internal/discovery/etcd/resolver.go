package etcd

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/resolver"
)

const Scheme = "seckill-etcd"

type Builder struct {
	client   *clientv3.Client
	observer func([]string)
}

func NewBuilder(client *clientv3.Client, observer func([]string)) *Builder {
	return &Builder{client: client, observer: observer}
}

func (b *Builder) Scheme() string { return Scheme }

func (b *Builder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	ctx, cancel := context.WithCancel(context.Background())
	service := target.Endpoint()
	buildCtx, buildCancel := context.WithTimeout(ctx, 3*time.Second)
	addresses, revision, err := snapshot(buildCtx, b.client, service)
	buildCancel()
	if err != nil {
		cancel()
		return nil, err
	}
	r := &etcdResolver{client: b.client, cc: cc, service: service, addresses: addresses, observer: b.observer, ctx: ctx, cancel: cancel}
	if err := r.publish(); err != nil {
		cancel()
		return nil, err
	}
	go r.watch(revision + 1)
	return r, nil
}

// snapshot 是 sk-m06 p3 S2：拨号前做一次前缀 Get，返回完整地址表与同一响应的 revision。
// 1. 为什么不是先 Watch 再 Get：首个 UpdateState 必须有当前全量，不能等未来事件碰巧补齐。
// 2. 为什么 revision 与地址来自同一个响应：后续 watch 的起点必须与这份快照严格衔接。
// 3. 为什么 key 作为 map 身份：delete 事件给的是注册 key，不能靠地址文本猜该删谁。
// AI 将在 p3 学习时分切片实现。
func snapshot(ctx context.Context, client *clientv3.Client, service string) (map[string]string, int64, error) {
	panic("TODO: sk-m06 p3 snapshot")
}

type etcdResolver struct {
	client    *clientv3.Client
	cc        resolver.ClientConn
	service   string
	addresses map[string]string
	observer  func([]string)
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
}

func (r *etcdResolver) ResolveNow(resolver.ResolveNowOptions) {}
func (r *etcdResolver) Close()                                { r.cancel() }

// watch 已就位（AI 生成）：从 snapshot 的下一 revision 接事件；错误只上报并停止，恢复题留给 transferChallenge。
func (r *etcdResolver) watch(nextRevision int64) {
	responses := r.client.Watch(r.ctx, servicePrefix(r.service), clientv3.WithPrefix(), clientv3.WithRev(nextRevision))
	for response := range responses {
		if err := response.Err(); err != nil {
			r.cc.ReportError(err)
			return
		}
		r.mu.Lock()
		applyWatchResponse(r.addresses, response)
		err := r.publishLocked()
		r.mu.Unlock()
		if err != nil {
			r.cc.ReportError(err)
		}
	}
}

// applyWatchResponse 是 sk-m06 p3 S3：把一批 put/delete 事件归并进当前完整地址表。
// 1. 为什么按事件 key 更新：同一地址重注册会有新 lease，但它仍对应同一个实例身份。
// 2. 为什么 delete 必须真的移除：round_robin 只会从 UpdateState 的地址里选，摘除靠这里发生。
// 3. 为什么一批事件处理完再 publish：同一 revision 的变化应作为一个完整状态交给 balancer。
// AI 将在 p3 学习时分切片实现。
func applyWatchResponse(addresses map[string]string, response clientv3.WatchResponse) {
	panic("TODO: sk-m06 p3 applyWatchResponse")
}

func (r *etcdResolver) publish() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.publishLocked()
}

func (r *etcdResolver) publishLocked() error {
	values := make([]string, 0, len(r.addresses))
	for _, addr := range r.addresses {
		values = append(values, addr)
	}
	sort.Strings(values)
	state := resolver.State{Addresses: make([]resolver.Address, len(values))}
	for i, addr := range values {
		state.Addresses[i] = resolver.Address{Addr: addr}
	}
	if r.observer != nil {
		r.observer(append([]string(nil), values...))
	}
	if err := r.cc.UpdateState(state); err != nil {
		return fmt.Errorf("discovery: update service=%s addresses=%v: %w", r.service, values, err)
	}
	return nil
}
