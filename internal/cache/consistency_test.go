package cache

import (
	"context"
	"testing"
	"time"

	"github.com/nbhaohao/go-seckill/internal/testutil"
)

// m02 · p4 一致性：先更库再删缓存，以及它留下的那个竞态窗口

func TestM02P4UpdateStockDeletesKeyAndNextReadSeesNewValue(t *testing.T) {
	defer guard(t)
	const productID = int64(9640)
	c, counting, db, rdb := newTestCache(t, testOptions(), nil, productID)
	testutil.ResetProduct(t, db, productID, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := c.Get(ctx, productID); err != nil {
		t.Fatalf("warm-up Get: %v", err)
	}
	if err := c.UpdateStock(ctx, productID, 3); err != nil {
		t.Fatalf("UpdateStock: %v", err)
	}

	exists, err := rdb.Exists(ctx, ProductKey(productID)).Result()
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if exists != 0 {
		raw, _ := rdb.Get(ctx, ProductKey(productID)).Result()
		t.Fatalf("更新之后缓存 key 还在（值=%s）——先更库再删缓存，删这一步不能省", raw)
	}

	var dbStock int
	if err := db.GetContext(ctx, &dbStock, "SELECT stock FROM products WHERE id = ?", productID); err != nil {
		t.Fatalf("read db stock: %v", err)
	}
	if dbStock != 3 {
		t.Fatalf("DB 里的 stock=%d, want 3（新值必须先落库）", dbStock)
	}

	counting.Reset()
	after, err := c.Get(ctx, productID)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if after.Stock != 3 {
		t.Fatalf("更新后再读拿到 stock=%d, want 3", after.Stock)
	}
	if got := counting.Loads(); got != 1 {
		t.Fatalf("更新后第一次读回源 %d 次, want 1（key 被删了，这一次必须 miss 回源）", got)
	}
	t.Logf("先更库再删缓存：DB stock=%d · 缓存 key 数=%d · 更新后首读回源 %d 次拿到 stock=%d",
		dbStock, exists, counting.Loads(), after.Stock)
}

// 这条是证据型测试（和 m01 p1 的超卖复现同一性质）：它要证明的不是代码写错了，
// 而是"先更库再删缓存"这个方案本身留了一个窗口——读请求已经从 DB 拿到旧值、
// 但还没写回缓存时，写请求的删除动作扑了个空，随后旧值被回填，缓存就一直是脏的（直到 TTL 到期）。
func TestM02P4StaleFillRaceWindowIsReproducible(t *testing.T) {
	defer guard(t)
	const productID = int64(9641)
	var blocking *blockingRepo
	wrap := func(inner ProductRepo) ProductRepo {
		blocking = newBlockingRepo(inner)
		return blocking
	}
	c, _, db, rdb := newTestCache(t, testOptions(), wrap, productID)
	testutil.ResetProduct(t, db, productID, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. 读请求先出发：miss → 回源读到旧值 10 → 卡在"还没回填"这一刻
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Get panicked (not implemented yet?): %v", r)
			}
		}()
		if _, err := c.Get(ctx, productID); err != nil {
			t.Errorf("reader Get: %v", err)
		}
	}()

	select {
	case <-blocking.entered:
	case <-time.After(10 * time.Second):
		t.Fatalf("读请求一直没进到回源那一步（Get 是不是还没实现，或者没有走回源分支）")
	}

	// 2. 写请求整段跑完：新值 3 落库 + 删缓存（此刻缓存里本来就没有这个 key，删除扑空）
	if err := c.UpdateStock(ctx, productID, 3); err != nil {
		t.Fatalf("UpdateStock: %v", err)
	}

	// 3. 放行读请求：它把手里那个"过期的 10"写回缓存
	close(blocking.release)
	<-readDone

	raw, err := rdb.Get(ctx, ProductKey(productID)).Result()
	if err != nil {
		t.Fatalf("读缓存里被回填的值: %v", err)
	}
	cached, err := UnmarshalProduct(raw)
	if err != nil {
		t.Fatalf("unmarshal cached: %v", err)
	}
	var dbStock int
	if err := db.GetContext(ctx, &dbStock, "SELECT stock FROM products WHERE id = ?", productID); err != nil {
		t.Fatalf("read db stock: %v", err)
	}

	if dbStock != 3 {
		t.Fatalf("DB stock=%d, want 3（写请求应该已经落库）", dbStock)
	}
	if cached.Stock != 10 {
		t.Fatalf("缓存里的 stock=%d，本关期望复现出脏数据 10——"+
			"如果这里等于 3，说明回填时机和本关设定的窗口不一致，检查 Get 是先回源后回填、UpdateStock 是先更库后删 key",
			cached.Stock)
	}
	t.Logf("脏数据窗口复现：缓存里被回填的旧值 stock=%d（原始值 %s），而 DB 里已经是 stock=%d；"+
		"这条脏记录会一直存活到 TTL 到期为止", cached.Stock, raw, dbStock)
}
