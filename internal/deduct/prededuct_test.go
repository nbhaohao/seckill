package deduct

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/nbhaohao/go-seckill/internal/order"
)

// m03 · p4 Redis + Lua 原子预扣 ⭐：闸门整个搬到 Redis，热路径上"够不够"这件事
// 一次 EVAL 就答完，DB 只在预扣成功之后被碰一次。

func TestM03P4PreDeductConcurrentNoOversell(t *testing.T) {
	const productID = int64(9808)
	const initialStock = 20
	const concurrency = 60
	db, rdb, node := newTestDeps(t, productID, initialStock)

	if err := WarmStock(context.Background(), rdb, db, productID); err != nil {
		t.Fatalf("预热库存进 Redis：%v", err)
	}

	var mu sync.Mutex
	succeeded, insufficient := 0, 0

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer guardConcurrent(t)
			_, err := PlaceOrderWithPreDeduct(context.Background(), rdb, db, node, order.PlaceOrderRequest{
				RequestID: fmt.Sprintf("m03-lua-%d", i),
				ProductID: productID,
				UserID:    int64(i),
				Quantity:  1,
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, ErrInsufficientStock):
				insufficient++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	finalStock := readStock(t, db, productID)
	redisStock := readRedisStock(t, rdb, productID)
	orders := countOrders(t, db, productID)

	if redisStock < 0 {
		t.Fatalf("Redis 侧超卖了：预扣库存变成负数 %d —— 判断和扣减没被塞进同一段脚本？", redisStock)
	}
	if finalStock < 0 {
		t.Fatalf("DB 侧超卖了：库存变成负数 %d", finalStock)
	}
	if succeeded != initialStock {
		t.Fatalf("库存应该被正好卖完：期望成功 %d，实际 %d", initialStock, succeeded)
	}
	// 恒等式：初始库存 − DB 剩余 ≡ 订单数 ≡ 预扣成功数。
	if initialStock-finalStock != succeeded || orders != succeeded {
		t.Fatalf("恒等式破了：initialStock(%d) - finalStock(%d) = %d，订单 %d 行，成功 %d 次",
			initialStock, finalStock, initialStock-finalStock, orders, succeeded)
	}

	t.Logf("Lua 原子预扣：%d 并发抢 %d 库存 → 成功 %d · 库存不足 %d（这 %d 次一条 SQL 都没打）；"+
		"Redis 剩余 %d、DB 剩余 %d、订单 %d 行，三者闭合",
		concurrency, initialStock, succeeded, insufficient, insufficient, redisStock, finalStock, orders)
}

func TestM03P4PreDeductStopsAtZeroAndFailsFast(t *testing.T) {
	defer guard(t)
	const productID = int64(9809)
	const initialStock = 3
	const rounds = 10
	db, rdb, _ := newTestDeps(t, productID, initialStock)
	ctx := context.Background()

	if err := WarmStock(ctx, rdb, db, productID); err != nil {
		t.Fatalf("预热库存进 Redis：%v", err)
	}

	succeeded, insufficient := 0, 0
	var remainings []int64
	for i := 0; i < rounds; i++ {
		remaining, err := PreDeduct(ctx, rdb, productID, 1)
		switch {
		case err == nil:
			succeeded++
			remainings = append(remainings, remaining)
		case errors.Is(err, ErrInsufficientStock):
			insufficient++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if succeeded != initialStock {
		t.Fatalf("只有 %d 件货，应该恰好成功 %d 次，实际 %d 次", initialStock, initialStock, succeeded)
	}
	if insufficient != rounds-initialStock {
		t.Fatalf("剩下 %d 次都应该是库存不足，实际 %d 次", rounds-initialStock, insufficient)
	}
	redisStock := readRedisStock(t, rdb, productID)
	if redisStock != 0 {
		t.Fatalf("卖光后 Redis 库存应该停在 0（既不为负也不继续扣），实际 %d", redisStock)
	}

	t.Logf("卖光后快速失败：%d 次预扣 → 成功 %d（剩余依次为 %v）· 库存不足 %d；"+
		"Redis 库存停在 %d，没有被扣成负数",
		rounds, succeeded, remainings, insufficient, redisStock)
}

func TestM03P4OrderFailureRollsBackPreDeduct(t *testing.T) {
	defer guard(t)
	const productID = int64(9810)
	const initialStock = 5
	db, rdb, node := newTestDeps(t, productID, initialStock)
	ctx := context.Background()

	if err := WarmStock(ctx, rdb, db, productID); err != nil {
		t.Fatalf("预热库存进 Redis：%v", err)
	}

	req := order.PlaceOrderRequest{
		RequestID: "m03-lua-rollback-same-id",
		ProductID: productID,
		UserID:    1,
		Quantity:  1,
	}
	if _, err := PlaceOrderWithPreDeduct(ctx, rdb, db, node, req); err != nil {
		t.Fatalf("第一次下单应该成功：%v", err)
	}
	beforeRedis := readRedisStock(t, rdb, productID)
	beforeDB := readStock(t, db, productID)
	beforeOrders := countOrders(t, db, productID)

	// 同一个 request_id 再来一次：预扣会成功（Redis 不认识 request_id），
	// 但插订单会撞 orders.uk_request_id 唯一索引而失败——这就是"预扣成功、落库失败"那条路径。
	if _, err := PlaceOrderWithPreDeduct(ctx, rdb, db, node, req); err == nil {
		t.Fatalf("重复 request_id 应该落库失败（撞唯一索引），但返回了成功")
	}

	afterRedis := readRedisStock(t, rdb, productID)
	afterDB := readStock(t, db, productID)
	afterOrders := countOrders(t, db, productID)

	if afterRedis != beforeRedis {
		t.Fatalf("落库失败后 Redis 预扣必须被还回去：失败前 %d，失败后 %d（差值就是凭空蒸发的库存）",
			beforeRedis, afterRedis)
	}
	if afterDB != beforeDB || afterOrders != beforeOrders {
		t.Fatalf("落库失败不该改变 DB：stock %d→%d，orders %d→%d", beforeDB, afterDB, beforeOrders, afterOrders)
	}

	t.Logf("预扣回滚：重复 request_id 撞唯一索引导致落库失败，Redis 库存 %d→%d（已还回）、"+
		"DB 库存 %d→%d、订单 %d→%d 行全部未变；没有这一步，Redis 会说卖光了而 DB 里根本没这单",
		beforeRedis, afterRedis, beforeDB, afterDB, beforeOrders, afterOrders)
}
