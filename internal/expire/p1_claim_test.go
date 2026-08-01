package expire

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/nbhaohao/go-seckill/internal/testutil"
)

func TestM5AP1ConcurrentWorkersClaimDueOrdersOnce(t *testing.T) {
	rdb := testutil.OpenTestRedis(t)
	testutil.DeleteKeys(t, rdb, ExpireZSetKey)
	clock := NewFakeClock(time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC))
	ctx := context.Background()
	dueOrderIDs := make([]string, 0, 200)
	for i := 0; i < cap(dueOrderIDs); i++ {
		dueOrderIDs = append(dueOrderIDs, fmt.Sprintf("m5a-p1-due-%03d", i))
	}
	futureOrderID := "m5a-p1-future"
	for _, orderID := range dueOrderIDs {
		if err := IndexOrder(ctx, rdb, orderID, clock.Now().Add(-time.Second)); err != nil {
			t.Fatalf("index due order %q: got %v", orderID, err)
		}
	}
	if err := IndexOrder(ctx, rdb, futureOrderID, clock.Now().Add(time.Minute)); err != nil {
		t.Fatalf("index future order %q: got %v", futureOrderID, err)
	}

	// 8 个 worker 各自循环领到空为止,一起去抢同一批 due 订单。
	// 规模是刻意的:实测「先 ZRANGE 拿一批、再 ZADD 推 score」这种两次往返的非原子写法,
	// 在 2 worker × 2 订单下 40 次全绿(窗口太窄撞不上),在这个规模下每轮都能撞出
	// 170–190 个重复领取。领取必须是一次原子执行,这条断言才有意义。
	const workerCount = 8
	const batchSizePerWorker = 10
	claimExtension := time.Minute
	claimedByWorker := make([][]string, workerCount)
	errs := make([]error, workerCount)
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				batch, err := ClaimDue(ctx, rdb, clock, batchSizePerWorker, claimExtension)
				if err != nil {
					errs[worker] = err
					return
				}
				if len(batch) == 0 {
					return
				}
				claimedByWorker[worker] = append(claimedByWorker[worker], batch...)
			}
		}(worker)
	}
	wg.Wait()

	counts := map[string]int{}
	totalClaims := 0
	for worker, claimed := range claimedByWorker {
		if errs[worker] != nil {
			t.Fatalf("worker %d claim with batch=%d: got %v", worker, batchSizePerWorker, errs[worker])
		}
		for _, orderID := range claimed {
			counts[orderID]++
			totalClaims++
		}
	}
	duplicated := make([]string, 0)
	for orderID, n := range counts {
		if n > 1 {
			duplicated = append(duplicated, orderID)
		}
	}
	sort.Strings(duplicated)
	t.Logf("workers=%d due=%d total_claims=%d unique=%d duplicated=%d", workerCount, len(dueOrderIDs), totalClaims, len(counts), len(duplicated))
	if totalClaims != len(dueOrderIDs) || len(counts) != len(dueOrderIDs) || len(duplicated) != 0 {
		t.Fatalf("每个到期订单必须恰好被领一次: due=%d total_claims=%d unique=%d duplicated=%d sample_duplicated=%v",
			len(dueOrderIDs), totalClaims, len(counts), len(duplicated), duplicated[:min(len(duplicated), 5)])
	}
	if counts[futureOrderID] != 0 {
		t.Fatalf("未到期订单不许被领: future_order=%q expires_in=%v got count=%d", futureOrderID, time.Minute, counts[futureOrderID])
	}
	for _, orderID := range dueOrderIDs {
		score, err := rdb.ZScore(ctx, ExpireZSetKey, orderID).Result()
		if err != nil {
			t.Fatalf("read claimed score for order %q: got %v", orderID, err)
		}
		wantScore := float64(clock.Now().Add(claimExtension).UnixMilli())
		if score != wantScore {
			t.Fatalf("claimed score must move forward by %v: order=%q want=%v got %v", claimExtension, orderID, wantScore, score)
		}
	}
}
