package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nbhaohao/go-seckill/internal/testutil"
)

// m02 · p3 击穿：热点 key 过期瞬间的并发回源必须被合并成一次

func TestM02P3ConcurrentMissCollapsesToOneDBLoad(t *testing.T) {
	defer guard(t)
	const productID = int64(9630)
	const concurrency = 100
	// 给回源加 50ms 延迟，把"大家都发现 key 不在"的窗口拉宽——真实线上这个窗口就是
	// 一次 DB 往返的时间，热点商品每秒几万请求时足够让成千上万个请求同时挤进来。
	wrap := func(inner ProductRepo) ProductRepo { return &slowRepo{inner: inner, delay: 50 * time.Millisecond} }
	c, counting, db, _ := newTestCache(t, testOptions(), wrap, productID)
	testutil.ResetProduct(t, db, productID, 500)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex
	stocks := make([]int, 0, concurrency)
	errs := make([]error, 0)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Get panicked (not implemented yet?): %v", r)
				}
			}()
			got, err := c.Get(ctx, productID)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			stocks = append(stocks, got.Stock)
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("%d/%d 个并发 Get 出错，第一个是：%v", len(errs), concurrency, errs[0])
	}
	if len(stocks) != concurrency {
		t.Fatalf("只有 %d/%d 个并发 Get 拿到结果", len(stocks), concurrency)
	}
	for i, s := range stocks {
		if s != 500 {
			t.Fatalf("第 %d 个调用拿到 stock=%d, want 500（合并回源不能让某些调用方拿到空值/错值）", i, s)
		}
	}
	if got := counting.Loads(); got != 1 {
		t.Fatalf("%d 个并发请求打同一个冷 key，DB 被回源了 %d 次, want 恰好 1 次——"+
			"这正是缓存击穿：热点 key 一过期，所有请求一起穿到 DB 上", concurrency, got)
	}
	t.Logf("击穿被合并：%d 个并发 Get 同一个冷 key，DB 回源 %d 次，全部拿到 stock=%d",
		concurrency, counting.Loads(), stocks[0])
}

func TestM02P3SharedLoadGivesEachCallerItsOwnCopy(t *testing.T) {
	defer guard(t)
	const productID = int64(9631)
	wrap := func(inner ProductRepo) ProductRepo { return &slowRepo{inner: inner, delay: 80 * time.Millisecond} }
	c, counting, db, _ := newTestCache(t, testOptions(), wrap, productID)
	testutil.ResetProduct(t, db, productID, 42)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	results := make([]*Product, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Get panicked (not implemented yet?): %v", r)
				}
			}()
			got, err := c.Get(ctx, productID)
			if err != nil {
				t.Errorf("Get #%d: %v", i, err)
				return
			}
			results[i] = got
		}(i)
	}
	wg.Wait()

	if results[0] == nil || results[1] == nil {
		t.Fatalf("两个并发调用没都拿到结果：%+v / %+v", results[0], results[1])
	}
	if got := counting.Loads(); got != 1 {
		t.Fatalf("两个并发 Get 触发了 %d 次回源, want 1（这一步的前提是回源已经被合并）", got)
	}
	if results[0] == results[1] {
		t.Fatalf("两个调用方拿到的是同一个 *Product 指针——合并回源之后必须给每个调用方一份自己的拷贝，" +
			"否则任何一方改了字段，另一方手里的数据被悄悄改掉")
	}

	// 证据：改动其中一份不影响另一份。
	results[0].Stock = -1
	if results[1].Stock != 42 {
		t.Fatalf("改了第一份拷贝的 Stock，第二份变成了 %d（want 42）——两份数据仍然共享底层对象", results[1].Stock)
	}
	t.Logf("合并回源 + 各自拷贝：DB 回源 %d 次，两个调用方的指针不同（%p vs %p）；把第一份改成 -1 后第二份仍是 %d",
		counting.Loads(), results[0], results[1], results[1].Stock)
}
