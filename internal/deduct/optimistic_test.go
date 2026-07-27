package deduct

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/nbhaohao/go-seckill/internal/order"
)

// m03 · p1 乐观锁：两种形态在同样的并发下都必须守住恒等式，
// 但它们付出的代价不一样——CAS 那条会打印出真实的重试次数，条件更新那条是零重试。

func TestM03P1VersionCASConcurrentIdentityHolds(t *testing.T) {
	const productID = int64(9801)
	const initialStock = 20
	const concurrency = 60
	db, _, node := newTestDeps(t, productID, initialStock)

	var mu sync.Mutex
	succeeded, insufficient, exhausted, totalAttempts := 0, 0, 0, 0

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer guardConcurrent(t)
			_, attempts, err := PlaceOrderByVersionCAS(context.Background(), db, node, order.PlaceOrderRequest{
				RequestID: fmt.Sprintf("m03-cas-%d", i),
				ProductID: productID,
				UserID:    int64(i),
				Quantity:  1,
			})
			mu.Lock()
			defer mu.Unlock()
			totalAttempts += attempts
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, ErrInsufficientStock):
				insufficient++
			case errors.Is(err, ErrCASRetriesExhausted):
				exhausted++
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
	// 恒等式：初始库存 − 剩余库存 ≡ 成功下单数（每单 quantity=1）。
	if initialStock-finalStock != succeeded {
		t.Fatalf("恒等式破了：initialStock(%d) - finalStock(%d) = %d，但成功数是 %d",
			initialStock, finalStock, initialStock-finalStock, succeeded)
	}
	if orders != succeeded {
		t.Fatalf("订单数(%d)和成功数(%d)对不上：扣了库存却没插订单，或者反过来", orders, succeeded)
	}

	avg := 0.0
	if succeeded+insufficient+exhausted > 0 {
		avg = float64(totalAttempts) / float64(succeeded+insufficient+exhausted)
	}
	t.Logf("版本号 CAS：%d 并发抢 %d 库存 → 成功 %d · 库存不足 %d · 重试耗尽 %d；"+
		"总 CAS 尝试 %d 次，平均每次调用尝试 %.2f 次（重试上限 %d）",
		concurrency, initialStock, succeeded, insufficient, exhausted, totalAttempts, avg, MaxCASRetries)
}

func TestM03P1ConditionalUpdateConcurrentIdentityHolds(t *testing.T) {
	const productID = int64(9802)
	const initialStock = 20
	const concurrency = 60
	db, _, node := newTestDeps(t, productID, initialStock)

	var mu sync.Mutex
	succeeded, insufficient := 0, 0

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer guardConcurrent(t)
			_, err := PlaceOrderByConditionalUpdate(context.Background(), db, node, order.PlaceOrderRequest{
				RequestID: fmt.Sprintf("m03-cond-%d", i),
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
	// 条件更新形态没有重试循环：库存判断被 WHERE 吸收进那条 UPDATE 里，
	// 所以这里卖光就是卖光，不存在"被抢先了再来一次"。
	if succeeded != initialStock {
		t.Fatalf("条件更新应该把库存正好卖完：期望成功 %d，实际 %d", initialStock, succeeded)
	}

	t.Logf("条件更新：%d 并发抢 %d 库存 → 成功 %d · 库存不足 %d；剩余库存 %d，订单 %d 行；"+
		"零重试（没有读-改-写窗口，判断在 UPDATE 的 WHERE 里）",
		concurrency, initialStock, succeeded, insufficient, finalStock, orders)
}
