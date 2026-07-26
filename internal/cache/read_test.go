package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nbhaohao/go-seckill/internal/testutil"
)

// m02 · p1 cache-aside 读路径 + 预热

func TestM02P1GetFillsCacheThenSecondReadSkipsDB(t *testing.T) {
	defer guard(t)
	const productID = int64(9601)
	opts := testOptions()
	c, counting, db, rdb := newTestCache(t, opts, nil, productID)
	testutil.ResetProduct(t, db, productID, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first, err := c.Get(ctx, productID)
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	if first.Stock != 10 || first.ID != productID {
		t.Fatalf("first Get returned %+v, want id=%d stock=10", first, productID)
	}
	if got := counting.Loads(); got != 1 {
		t.Fatalf("after first Get: DB loads=%d, want 1（miss 必须回源一次）", got)
	}

	ttl, err := rdb.TTL(ctx, ProductKey(productID)).Result()
	if err != nil {
		t.Fatalf("read TTL: %v", err)
	}
	// TTL 为 -1 表示 key 存在但永不过期，-2 表示 key 不存在——两者都是回填没做对。
	if ttl <= 0 {
		t.Fatalf("cached key TTL=%v，want 一个正的过期时间（回填时必须带 TTL，否则缓存永不过期）", ttl)
	}
	if ttl > opts.TTL+opts.TTLJitter {
		t.Fatalf("cached key TTL=%v 超出 opts.TTL+TTLJitter=%v", ttl, opts.TTL+opts.TTLJitter)
	}

	second, err := c.Get(ctx, productID)
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if second.Stock != 10 {
		t.Fatalf("second Get returned stock=%d, want 10", second.Stock)
	}
	if got := counting.Loads(); got != 1 {
		t.Fatalf("after second Get: DB loads=%d, want 仍是 1（第二次读必须命中缓存，一次 DB 都不打）", got)
	}

	raw, err := rdb.Get(ctx, ProductKey(productID)).Result()
	if err != nil {
		t.Fatalf("read raw cached value: %v", err)
	}
	t.Logf("cache-aside 生效：DB loads=%d（两次 Get）· TTL=%v · 缓存里存的原始值=%s",
		counting.Loads(), ttl, raw)
}

func TestM02P1WarmPrefillsSoFirstGetIsHit(t *testing.T) {
	defer guard(t)
	const idA = int64(9602)
	const idB = int64(9603)
	const idMissing = int64(9699) // 不存在的商品：预热要跳过它，不能让整批失败
	c, counting, db, rdb := newTestCache(t, testOptions(), nil, idA, idB, idMissing)
	testutil.ResetProduct(t, db, idA, 7)
	testutil.ResetProduct(t, db, idB, 8)
	if _, err := db.Exec("DELETE FROM products WHERE id = ?", idMissing); err != nil {
		t.Fatalf("delete missing product: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Warm(ctx, []int64{idA, idB, idMissing}); err != nil {
		t.Fatalf("Warm: %v（不存在的商品应被跳过，不该让整批预热失败）", err)
	}
	warmLoads := counting.Loads()
	counting.Reset()

	for _, tc := range []struct {
		id    int64
		stock int
	}{{idA, 7}, {idB, 8}} {
		got, err := c.Get(ctx, tc.id)
		if err != nil {
			t.Fatalf("Get(%d) after Warm: %v", tc.id, err)
		}
		if got.Stock != tc.stock {
			t.Fatalf("Get(%d) stock=%d, want %d", tc.id, got.Stock, tc.stock)
		}
	}
	if got := counting.Loads(); got != 0 {
		t.Fatalf("预热之后两次 Get 又回源了 %d 次，want 0（预热就是为了让开卖瞬间全部命中）", got)
	}

	existsMissing, err := rdb.Exists(ctx, ProductKey(idMissing)).Result()
	if err != nil {
		t.Fatalf("exists(missing): %v", err)
	}
	t.Logf("预热生效：Warm 期间回源 %d 次，之后两次 Get 回源 %d 次；不存在的商品 %d 在缓存里的 key 数=%d",
		warmLoads, counting.Loads(), idMissing, existsMissing)
}

// m02 · p2 穿透（空值缓存）与雪崩（TTL 抖动）

func TestM02P2MissingIDIsNullCachedAndStopsDBLoads(t *testing.T) {
	defer guard(t)
	const missingID = int64(9610)
	opts := testOptions()
	c, counting, db, rdb := newTestCache(t, opts, nil, missingID)
	if _, err := db.Exec("DELETE FROM products WHERE id = ?", missingID); err != nil {
		t.Fatalf("delete product: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := c.Get(ctx, missingID); !errors.Is(err, ErrProductNotFound) {
		t.Fatalf("first Get(missing) err=%v, want ErrProductNotFound", err)
	}
	if got := counting.Loads(); got != 1 {
		t.Fatalf("after first Get(missing): DB loads=%d, want 1", got)
	}

	raw, err := rdb.Get(ctx, ProductKey(missingID)).Result()
	if err != nil {
		t.Fatalf("空值缓存没写进 Redis（读 key 报 %v）——穿透时也必须往缓存里留下一条『这个 id 不存在』的记录", err)
	}
	if raw != NotFoundPlaceholder {
		t.Fatalf("空值缓存里的原始值=%q, want %q", raw, NotFoundPlaceholder)
	}

	missTTL, err := rdb.TTL(ctx, ProductKey(missingID)).Result()
	if err != nil {
		t.Fatalf("read miss TTL: %v", err)
	}
	if missTTL <= 0 || missTTL > opts.MissTTL {
		t.Fatalf("空值缓存 TTL=%v，want 落在 (0, MissTTL=%v] 内（空值必须短命，否则商品真上架了也要等很久才能被读到）",
			missTTL, opts.MissTTL)
	}

	for i := 0; i < 5; i++ {
		if _, err := c.Get(ctx, missingID); !errors.Is(err, ErrProductNotFound) {
			t.Fatalf("repeat Get(missing) #%d err=%v, want ErrProductNotFound", i, err)
		}
	}
	if got := counting.Loads(); got != 1 {
		t.Fatalf("连打 6 次不存在的 id 之后 DB loads=%d, want 仍是 1（空值缓存要把后续请求全部挡在缓存层）", got)
	}
	t.Logf("穿透被挡住：不存在的 id 连打 6 次，DB 只回源 %d 次；缓存里的原始值=%q，TTL=%v（MissTTL=%v）",
		counting.Loads(), raw, missTTL, opts.MissTTL)
}

func TestM02P2WarmSpreadsTTLWithJitter(t *testing.T) {
	defer guard(t)
	opts := testOptions()
	ids := make([]int64, 0, 12)
	for i := 0; i < 12; i++ {
		ids = append(ids, int64(9620+i))
	}
	c, _, db, rdb := newTestCache(t, opts, nil, ids...)
	for _, id := range ids {
		testutil.ResetProduct(t, db, id, 100)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.Warm(ctx, ids); err != nil {
		t.Fatalf("Warm: %v", err)
	}

	ttls := make([]time.Duration, 0, len(ids))
	distinct := map[time.Duration]struct{}{}
	var minTTL, maxTTL time.Duration
	for i, id := range ids {
		// PTTL 是毫秒精度，抖动量看得见；TTL 只有秒精度会把小抖动抹平。
		pttl, err := rdb.PTTL(ctx, ProductKey(id)).Result()
		if err != nil {
			t.Fatalf("PTTL(%d): %v", id, err)
		}
		if pttl <= 0 {
			t.Fatalf("PTTL(%d)=%v，key 要么不存在要么没设过期时间", id, pttl)
		}
		if pttl > opts.TTL+opts.TTLJitter {
			t.Fatalf("PTTL(%d)=%v 超出 TTL+TTLJitter=%v（抖动只能往上叠，不能超过上限）",
				id, pttl, opts.TTL+opts.TTLJitter)
		}
		ttls = append(ttls, pttl)
		distinct[pttl] = struct{}{}
		if i == 0 || pttl < minTTL {
			minTTL = pttl
		}
		if i == 0 || pttl > maxTTL {
			maxTTL = pttl
		}
	}

	if len(distinct) < 2 {
		t.Fatalf("12 个预热 key 的 TTL 全都一样（%v）——同一批 key 会在同一瞬间集体过期，这就是雪崩；"+
			"回填时必须给 TTL 叠一个随机抖动", ttls[0])
	}
	// 容差留得很宽：只要求抖动真的把过期时刻拉开了，不对具体分布做断言。
	if maxTTL-minTTL < 500*time.Millisecond {
		t.Fatalf("最大最小 TTL 只差 %v（min=%v max=%v），抖动幅度太小，起不到分散过期时刻的作用",
			maxTTL-minTTL, minTTL, maxTTL)
	}
	t.Logf("TTL 抖动生效：12 个 key 有 %d 个不同的过期时刻，min=%v max=%v 极差=%v（base TTL=%v, jitter 上限=%v）",
		len(distinct), minTTL, maxTTL, maxTTL-minTTL, opts.TTL, opts.TTLJitter)
	t.Logf("TTL 采样：%v", ttls[:6])
}
