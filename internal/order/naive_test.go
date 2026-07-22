package order

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/nbhaohao/go-seckill/internal/testutil"
)

// m01 · p1 裸下单链路：无锁无事务并发下单，复现超卖

func TestM01P1PlaceOrderNaiveConcurrentOversells(t *testing.T) {
	db := testutil.OpenTestDB(t)
	const productID = int64(9001)
	const initialStock = 5
	const concurrency = 60 // 远超库存的并发量，让"读到旧值再写"的窗口在真实网络往返延迟下必然被撞上
	testutil.ResetProduct(t, db, productID, initialStock)

	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded := 0
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("PlaceOrderNaive panicked (not implemented yet?): %v", r)
				}
			}()
			_, err := PlaceOrderNaive(context.Background(), db, PlaceOrderRequest{
				RequestID: fmt.Sprintf("naive-%d", i),
				ProductID: productID,
				UserID:    int64(i),
				Quantity:  1,
			})
			if err == nil {
				mu.Lock()
				succeeded++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	var finalStock int
	if err := db.Get(&finalStock, "SELECT stock FROM products WHERE id = ?", productID); err != nil {
		t.Fatalf("read final stock: %v", err)
	}

	// 恒等式 initialStock - finalStock == succeeded 在naive实现下应当被打破：
	// 要么卖出的订单数超过初始库存，要么库存被扣成了负数——两者任一发生就证明超卖已复现。
	oversold := succeeded > initialStock || finalStock < 0
	if !oversold {
		t.Fatalf("expected naive implementation to oversell under concurrency, but got succeeded=%d finalStock=%d (initialStock=%d) — "+
			"如果这条测试意外变绿，检查 PlaceOrderNaive 是不是提前加了事务/锁（那是 p3 的事）",
			succeeded, finalStock, initialStock)
	}
	t.Logf("oversell reproduced: succeeded=%d finalStock=%d initialStock=%d", succeeded, finalStock, initialStock)
}
