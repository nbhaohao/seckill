package deduct

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nbhaohao/go-seckill/internal/order"
)

// m03 · p2 分布式锁的正确性：token 是"这把锁是不是我的"的唯一依据，
// 释放必须原子地比对它——下面第一条测试就是不比对的后果。

func TestM03P2NaiveDeleteRemovesAnotherHoldersLock(t *testing.T) {
	defer guard(t)
	const productID = int64(9803)
	_, rdb, _ := newTestDeps(t, productID, 10)
	ctx := context.Background()
	key := LockKey(productID)

	holderA, err := Acquire(ctx, rdb, key, 5*time.Second)
	if err != nil {
		t.Fatalf("A 抢锁应该成功：%v", err)
	}

	// B 想释放"自己的"锁，但它写的是裸 DEL——不比对 token，直接删。
	// 现实里这就是一句 defer rdb.Del(ctx, key)，看起来人畜无害。
	if err := rdb.Del(ctx, key).Err(); err != nil {
		t.Fatalf("裸 DEL 失败：%v", err)
	}

	// 后果：A 还在临界区里，以为自己持锁；而锁已经空了，C 立刻能抢到。
	holderC, err := Acquire(ctx, rdb, key, 5*time.Second)
	if err != nil {
		t.Fatalf("裸 DEL 之后 C 应该能抢到锁（这正是事故）：%v", err)
	}
	if holderC.Token() == holderA.Token() {
		t.Fatalf("两次持锁的 token 不该相同：%s", holderA.Token())
	}

	t.Logf("误删复现：A 持锁 token=%s，一句不比对 token 的裸 DEL 把它删掉了；"+
		"C 随即抢到同一把锁 token=%s —— 此刻 A 仍以为自己在临界区里，两个人同时在改同一份库存",
		holderA.Token(), holderC.Token())
}

func TestM03P2LuaReleaseRefusesToDeleteAnotherHoldersLock(t *testing.T) {
	defer guard(t)
	const productID = int64(9804)
	_, rdb, _ := newTestDeps(t, productID, 10)
	ctx := context.Background()
	key := LockKey(productID)

	holderA, err := Acquire(ctx, rdb, key, 5*time.Second)
	if err != nil {
		t.Fatalf("A 抢锁应该成功：%v", err)
	}
	// 模拟 A 的锁已经没了（过期/被误删），锁位被 C 占住。
	if err := rdb.Del(ctx, key).Err(); err != nil {
		t.Fatalf("清锁失败：%v", err)
	}
	holderC, err := Acquire(ctx, rdb, key, 5*time.Second)
	if err != nil {
		t.Fatalf("C 抢锁应该成功：%v", err)
	}

	// A 现在来释放。带 token 比对的 Release 必须认出"这把锁不是我的"，什么都不删。
	err = holderA.Release(ctx)
	if !errors.Is(err, ErrLockLost) {
		t.Fatalf("A 释放一把不属于自己的锁，应该返回 ErrLockLost，实际返回：%v", err)
	}

	// C 的锁必须还在。
	got, err := rdb.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("C 的锁不见了（被 A 误删）：%v", err)
	}
	if got != holderC.Token() {
		t.Fatalf("锁里的 token 变了：期望 C 的 %s，实际 %s", holderC.Token(), got)
	}

	t.Logf("Lua 原子释放：A(token=%s) 的 Release 返回 ErrLockLost 且没动 Redis；"+
		"C(token=%s) 的锁原样还在——比对和删除塞进同一段脚本，中间没有窗口",
		holderA.Token(), holderC.Token())
}

func TestM03P2LockedDeductConcurrentIdentityHolds(t *testing.T) {
	const productID = int64(9805)
	const initialStock = 20
	const concurrency = 60
	db, rdb, node := newTestDeps(t, productID, initialStock)

	opts := DefaultLockOptions()
	opts.TTL = 3 * time.Second

	var mu sync.Mutex
	succeeded, notAcquired, insufficient := 0, 0, 0

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer guardConcurrent(t)
			_, err := PlaceOrderWithLock(context.Background(), rdb, db, node, order.PlaceOrderRequest{
				RequestID: fmt.Sprintf("m03-lock-%d", i),
				ProductID: productID,
				UserID:    int64(i),
				Quantity:  1,
			}, opts)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, ErrLockNotAcquired):
				notAcquired++
			case errors.Is(err, ErrInsufficientStock):
				insufficient++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	finalStock := readStock(t, db, productID)
	orders := countOrders(t, db, productID)

	if finalStock < 0 {
		t.Fatalf("超卖了：库存变成负数 %d", finalStock)
	}
	if initialStock-finalStock != succeeded {
		t.Fatalf("恒等式破了：initialStock(%d) - finalStock(%d) = %d，但成功数是 %d",
			initialStock, finalStock, initialStock-finalStock, succeeded)
	}
	if orders != succeeded {
		t.Fatalf("订单数(%d)和成功数(%d)对不上", orders, succeeded)
	}
	if succeeded == 0 {
		t.Fatalf("一单都没成：抢锁全失败，检查 Acquire 是不是根本没写成功过")
	}

	t.Logf("锁内扣减：%d 并发 → 成功 %d · 没抢到锁 %d · 库存不足 %d；剩余库存 %d，订单 %d 行。"+
		"没抢到锁的直接快速失败（秒杀不排队），这是本方案吞吐的代价来源",
		concurrency, succeeded, notAcquired, insufficient, finalStock, orders)
}
