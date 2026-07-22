package order

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/nbhaohao/go-seckill/internal/idgen"
	"github.com/nbhaohao/go-seckill/internal/testutil"
)

// m01 · p3 事务 + FOR UPDATE 行锁 + request_id 幂等：恒等式恢复、重复提交不重复下单

func newTestNode(t *testing.T) *idgen.Node {
	t.Helper()
	n, err := idgen.NewNode(1)
	if err != nil {
		t.Fatalf("idgen.NewNode: %v", err)
	}
	return n
}

func TestM01P3PlaceOrderTxConcurrentIdentityHolds(t *testing.T) {
	db := testutil.OpenTestDB(t)
	node := newTestNode(t)
	const productID = int64(9002)
	const initialStock = 20
	const concurrency = 60
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
					t.Errorf("PlaceOrderTx panicked (not implemented yet?): %v", r)
				}
			}()
			_, err := PlaceOrderTx(context.Background(), db, node, PlaceOrderRequest{
				RequestID: fmt.Sprintf("tx-identity-%d", i),
				ProductID: productID,
				UserID:    int64(i),
				Quantity:  1,
			})
			if err == nil {
				mu.Lock()
				succeeded++
				mu.Unlock()
			} else if !errors.Is(err, ErrInsufficientStock) {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	var finalStock int
	if err := db.Get(&finalStock, "SELECT stock FROM products WHERE id = ?", productID); err != nil {
		t.Fatalf("read final stock: %v", err)
	}
	if finalStock < 0 {
		t.Fatalf("stock went negative: %d", finalStock)
	}
	// 恒等式：初始库存 - 剩余库存 == 成功下单的订单数（每单 quantity=1）。
	if initialStock-finalStock != succeeded {
		t.Fatalf("identity broken: initialStock(%d) - finalStock(%d) = %d, want succeeded = %d",
			initialStock, finalStock, initialStock-finalStock, succeeded)
	}
	if succeeded != initialStock {
		t.Fatalf("expected exactly initialStock(%d) orders to succeed when demand >= stock, got %d", initialStock, succeeded)
	}
}

func TestM01P3PlaceOrderTxDuplicateRequestIDIsIdempotent(t *testing.T) {
	db := testutil.OpenTestDB(t)
	node := newTestNode(t)
	const productID = int64(9003)
	testutil.ResetProduct(t, db, productID, 10)

	req := PlaceOrderRequest{RequestID: "dup-req-1", ProductID: productID, UserID: 42, Quantity: 2}

	first, err := PlaceOrderTx(context.Background(), db, node, req)
	if err != nil {
		t.Fatalf("first PlaceOrderTx: %v", err)
	}
	second, err := PlaceOrderTx(context.Background(), db, node, req)
	if err != nil {
		t.Fatalf("second PlaceOrderTx (duplicate request_id): %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("duplicate request_id produced a different order: first=%d second=%d", first.ID, second.ID)
	}

	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM orders WHERE request_id = ?", req.RequestID); err != nil {
		t.Fatalf("count orders: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 order row for request_id %q, got %d", req.RequestID, count)
	}

	var stock int
	if err := db.Get(&stock, "SELECT stock FROM products WHERE id = ?", productID); err != nil {
		t.Fatalf("read stock: %v", err)
	}
	if stock != 10-2 {
		t.Fatalf("stock should only be decremented once (by the first call): got %d, want %d", stock, 10-2)
	}
}

func TestM01P3ConcurrentDuplicateRequestIDReturnsSameOrder(t *testing.T) {
	db := testutil.OpenTestDB(t)
	node := newTestNode(t)
	const productID = int64(9005)
	const initialStock = 10
	const concurrency = 16
	testutil.ResetProduct(t, db, productID, initialStock)

	req := PlaceOrderRequest{RequestID: "concurrent-dup-req-1", ProductID: productID, UserID: 42, Quantity: 2}
	type result struct {
		order *Order
		err   error
	}
	results := make(chan result, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			var res result
			defer func() {
				if recovered := recover(); recovered != nil {
					res.err = fmt.Errorf("PlaceOrderTx panicked (not implemented yet?): %v", recovered)
				}
				results <- res
			}()
			res.order, res.err = PlaceOrderTx(context.Background(), db, node, req)
		}()
	}

	var firstID int64
	for i := 0; i < concurrency; i++ {
		res := <-results
		if res.err != nil {
			t.Fatalf("concurrent duplicate call %d: %v", i, res.err)
		}
		if res.order == nil {
			t.Fatalf("concurrent duplicate call %d returned nil order", i)
		}
		if firstID == 0 {
			firstID = res.order.ID
		} else if res.order.ID != firstID {
			t.Fatalf("concurrent duplicate returned different order: got=%d want=%d", res.order.ID, firstID)
		}
	}

	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM orders WHERE request_id = ?", req.RequestID); err != nil {
		t.Fatalf("count orders: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one order row after concurrent retries, got %d", count)
	}
	var stock int
	if err := db.Get(&stock, "SELECT stock FROM products WHERE id = ?", productID); err != nil {
		t.Fatalf("read stock: %v", err)
	}
	if stock != initialStock-req.Quantity {
		t.Fatalf("stock should be decremented once: got=%d want=%d", stock, initialStock-req.Quantity)
	}
}

func TestM01P3PlaceOrderTxInsufficientStock(t *testing.T) {
	db := testutil.OpenTestDB(t)
	node := newTestNode(t)
	const productID = int64(9004)
	testutil.ResetProduct(t, db, productID, 0)

	_, err := PlaceOrderTx(context.Background(), db, node, PlaceOrderRequest{
		RequestID: "insufficient-1", ProductID: productID, UserID: 1, Quantity: 1,
	})
	if !errors.Is(err, ErrInsufficientStock) {
		t.Fatalf("expected ErrInsufficientStock, got %v", err)
	}
}
