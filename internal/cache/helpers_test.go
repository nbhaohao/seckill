// 已就位（AI 生成）：m02 测试共用的接线与两个假 repo（慢回源 / 可阻塞回源），
// 它们只是造出并发窗口的工具，本身不是教学点。
package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/testutil"
)

// newTestCache 连真实 MySQL + 真实 Redis，返回一个干净起跑的 ProductCache
// 以及套在最外层的回源计数器（"DB 被打了几次"全靠它取证）。
// wrap 可以把真实的 SQL repo 再包一层（慢回源/可阻塞回源），传 nil 表示直连。
func newTestCache(t *testing.T, opts Options, wrap func(ProductRepo) ProductRepo, productIDs ...int64) (*ProductCache, *CountingRepo, *sqlx.DB, *redis.Client) {
	t.Helper()
	db := testutil.OpenTestDB(t)
	rdb := testutil.OpenTestRedis(t)

	var inner ProductRepo = NewSQLProductRepo(db)
	if wrap != nil {
		inner = wrap(inner)
	}
	counting := NewCountingRepo(inner)

	keys := make([]string, 0, len(productIDs))
	for _, id := range productIDs {
		keys = append(keys, ProductKey(id))
	}
	testutil.DeleteKeys(t, rdb, keys...)

	return New(rdb, counting, opts), counting, db, rdb
}

// guard 把"这个 phase 还没实现"的 panic 转成当前这条测试的普通失败。
// 不加它的话，一个 panic 会直接把整个测试进程打挂，同一个包里其他 phase 的测试根本跑不到，
// 红态下就看不出"哪些绿了、哪些还红着"。
func guard(t *testing.T) {
	if r := recover(); r != nil {
		t.Fatalf("被测方法 panic（对应 phase 还没实现？）：%v", r)
	}
}

func testOptions() Options {
	return Options{
		TTL:       30 * time.Second,
		TTLJitter: 10 * time.Second,
		MissTTL:   5 * time.Second,
	}
}

// slowRepo 给每次回源加一段固定延迟，用来把"并发都挤在回源那一刻"的窗口拉宽到肉眼可见。
type slowRepo struct {
	inner ProductRepo
	delay time.Duration
}

func (r *slowRepo) LoadProduct(ctx context.Context, id int64) (*Product, error) {
	select {
	case <-time.After(r.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return r.inner.LoadProduct(ctx, id)
}

func (r *slowRepo) UpdateStock(ctx context.Context, id int64, stock int) error {
	return r.inner.UpdateStock(ctx, id, stock)
}

// blockingRepo 的回源会一直挂着，直到测试往 release 里发信号——p4 用它精确制造
// "读请求已经从 DB 拿到旧值，但还没写回缓存"这个瞬间。
type blockingRepo struct {
	inner    ProductRepo
	entered  chan struct{} // 回源已进入（测试可以开始做写操作了）
	release  chan struct{} // 允许回源返回
	loadOnce atomic.Bool   // 只阻塞第一次回源，后续放行
}

func newBlockingRepo(inner ProductRepo) *blockingRepo {
	return &blockingRepo{
		inner:   inner,
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (r *blockingRepo) LoadProduct(ctx context.Context, id int64) (*Product, error) {
	p, err := r.inner.LoadProduct(ctx, id)
	if err != nil {
		return nil, err
	}
	if r.loadOnce.CompareAndSwap(false, true) {
		r.entered <- struct{}{}
		select {
		case <-r.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p, nil
}

func (r *blockingRepo) UpdateStock(ctx context.Context, id int64, stock int) error {
	return r.inner.UpdateStock(ctx, id, stock)
}
