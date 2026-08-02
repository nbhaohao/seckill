package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/order"
	"github.com/nbhaohao/go-seckill/internal/testutil"
)

// reconcileWindow 不是拍脑袋的常数：m04 的 write-up 实测「202 到 DB 可见」的端到端
// 窗口是 85.787417ms，压测下 Kafka lag 峰值 7、排空到 0 约 2s。窗口必须盖住排空时间，
// 这里取 3s（≈ 排空时间的 1.5 倍）。窗口比真实积压短会发生什么，是 p3 的 transferChallenge。
const reconcileWindow = 3 * time.Second

// baseTime 让所有时间判断都用显式的 now 推进，不依赖真实时钟，测试因此是确定的。
var baseTime = time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)

// seedLeakScenario 复现 p1 的事故现场，但这一次每笔预扣都留下了台账：
// attempts 发请求里前 crashes 发死在投递之前，其余的正常落库。
func seedLeakScenario(t *testing.T, db *sqlx.DB, rdb *redis.Client, productID int64, prefix string, initialStock int64, attempts, crashes int) {
	t.Helper()
	ctx := context.Background()
	testutil.ResetProduct(t, db, productID, int(initialStock))
	setRedisStock(t, rdb, productID, initialStock)
	resetLedger(t, rdb)

	crash := NewCrashSwitch(int64(crashes))
	for i := 0; i < attempts; i++ {
		requestID := fmt.Sprintf("%s-%02d", prefix, i)
		req := order.PlaceOrderRequest{RequestID: requestID, ProductID: productID, UserID: 7001, Quantity: 1}
		err := EnqueueWithLedger(ctx, rdb, req, baseTime, func(ctx context.Context, r order.PlaceOrderRequest) error {
			landOrder(t, db, productID*1000+int64(i), productID, r.RequestID, r.Quantity)
			return nil
		}, crash)
		if err != nil && !errors.Is(err, ErrSimulatedCrash) {
			t.Fatalf("seed: request_id=%s got %v", requestID, err)
		}
	}
}

// 对账的两条判据合在一起才成立：窗口内一动不动，窗口外才认定泄漏。
func TestM5CP2ReconcileWaitsForWindowThenRestoresIdentity(t *testing.T) {
	db, rdb := openM5CEnv(t)
	ctx := context.Background()
	const productID int64 = 10573
	const initialStock int64 = 20
	const attempts, crashes = 10, 3
	seedLeakScenario(t, db, rdb, productID, "m5c-p2-window", initialStock, attempts, crashes)

	before, err := CheckIdentity(ctx, rdb, db, productID, initialStock)
	if err != nil {
		t.Fatalf("对账前核对恒等式: got %v", err)
	}
	if before.Leaked != crashes {
		t.Fatalf("现场没造对，对账还没跑就应该有 %d 件泄漏: got %+v", crashes, before)
	}

	// 第一轮：还在窗口内，消息可能仍在途，一件都不许退。
	early, err := ReconcileOnce(ctx, rdb, db, reconcileWindow, baseTime.Add(reconcileWindow/2))
	if err != nil {
		t.Fatalf("窗口内对账: got %v", err)
	}
	stockAfterEarly := redisStock(t, rdb, productID)
	t.Logf("窗口内（台账年龄 %v < 窗口 %v）：%+v；库存仍为 %d", reconcileWindow/2, reconcileWindow, early, stockAfterEarly)
	if early.Refunded != 0 || early.Young != attempts || stockAfterEarly != initialStock-int64(attempts) {
		t.Fatalf("窗口内不许归还任何库存: report=%+v stock=%d want_young=%d", early, stockAfterEarly, attempts)
	}

	// 第二轮：过了窗口，只有 DB 里查不到订单的那几条才算泄漏。
	late, err := ReconcileOnce(ctx, rdb, db, reconcileWindow, baseTime.Add(reconcileWindow+time.Second))
	if err != nil {
		t.Fatalf("窗口外对账: got %v", err)
	}
	after, err := CheckIdentity(ctx, rdb, db, productID, initialStock)
	if err != nil {
		t.Fatalf("对账后核对恒等式: got %v", err)
	}
	t.Logf("窗口外：%+v；恒等式 before=%+v after=%+v", late, before, after)

	if late.Refunded != crashes || late.RefundedQty != int64(crashes) || late.Settled != attempts-crashes {
		t.Fatalf("只该退掉没有订单的那几条: want refunded=%d settled=%d got %+v", crashes, attempts-crashes, late)
	}
	if after.Leaked != 0 || !after.Holds() {
		t.Fatalf("对账跑完恒等式必须恢复: before=%+v after=%+v", before, after)
	}
}

// 幂等：对账 job 会被反复调度，第二轮不许再退一遍；
// 已经落库的订单在任何一轮都不许被误判成泄漏。
func TestM5CP2RepeatRunRefundsNothingMore(t *testing.T) {
	db, rdb := openM5CEnv(t)
	ctx := context.Background()
	const productID int64 = 10574
	const initialStock int64 = 20
	const attempts, crashes = 10, 3
	seedLeakScenario(t, db, rdb, productID, "m5c-p2-idem", initialStock, attempts, crashes)
	now := baseTime.Add(reconcileWindow + time.Second)

	first, err := ReconcileOnce(ctx, rdb, db, reconcileWindow, now)
	if err != nil {
		t.Fatalf("第一轮对账: got %v", err)
	}
	stockAfterFirst := redisStock(t, rdb, productID)

	second, err := ReconcileOnce(ctx, rdb, db, reconcileWindow, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("第二轮对账: got %v", err)
	}
	stockAfterSecond := redisStock(t, rdb, productID)
	t.Logf("第一轮 %+v 库存=%d；第二轮 %+v 库存=%d", first, stockAfterFirst, second, stockAfterSecond)

	if second.Refunded != 0 || second.RefundedQty != 0 {
		t.Fatalf("重复对账不许多退: first=%+v second=%+v", first, second)
	}
	if second.AlreadyReconciled != attempts {
		t.Fatalf("第二轮所有台账都该是已结案: want already=%d got %+v", attempts, second)
	}
	if stockAfterSecond != stockAfterFirst {
		t.Fatalf("重复对账改动了库存: after_first=%d after_second=%d", stockAfterFirst, stockAfterSecond)
	}
	// 已落库的那几单：台账被结案，但库存一件都没被退回去（退了就是把别人的货退掉）。
	settledRequestID := fmt.Sprintf("m5c-p2-idem-%02d", attempts-1)
	if got := ledgerState(t, rdb, settledRequestID); got != LedgerReconciled {
		t.Fatalf("已落库订单的台账也要结案，否则每一轮都会重新查一次 DB: request_id=%s got %q", settledRequestID, got)
	}
}

// 投递失败（进程还活着）的那条路径上，库存已经被 m04 的补偿还回去了，
// 台账必须跟着结案；留一条 pending 在那里，对账事后会把同一笔库存再退一次。
func TestM5CP2CompensatedProduceFailureIsNotRefundedTwice(t *testing.T) {
	db, rdb := openM5CEnv(t)
	ctx := context.Background()
	const productID int64 = 10577
	const initialStock int64 = 20
	const attempts, failures = 6, 2
	testutil.ResetProduct(t, db, productID, int(initialStock))
	setRedisStock(t, rdb, productID, initialStock)
	resetLedger(t, rdb)

	produceErr := errors.New("produce: broker unavailable")
	for i := 0; i < attempts; i++ {
		requestID := fmt.Sprintf("m5c-p2-compensated-%02d", i)
		req := order.PlaceOrderRequest{RequestID: requestID, ProductID: productID, UserID: 7001, Quantity: 1}
		err := EnqueueWithLedger(ctx, rdb, req, baseTime, func(ctx context.Context, r order.PlaceOrderRequest) error {
			if i < failures {
				return produceErr
			}
			landOrder(t, db, productID*1000+int64(i), productID, r.RequestID, r.Quantity)
			return nil
		}, NewCrashSwitch(0))
		if err != nil && !errors.Is(err, produceErr) {
			t.Fatalf("seed: request_id=%s got %v", requestID, err)
		}
	}

	stockBefore := redisStock(t, rdb, productID)
	report, err := ReconcileOnce(ctx, rdb, db, reconcileWindow, baseTime.Add(reconcileWindow+time.Second))
	if err != nil {
		t.Fatalf("对账: got %v", err)
	}
	stockAfter := redisStock(t, rdb, productID)
	t.Logf("投递失败 %d 发（已被 m04 补偿回滚）：对账 %+v；库存 %d → %d", failures, report, stockBefore, stockAfter)

	if report.Refunded != 0 || stockAfter != stockBefore {
		t.Fatalf("已经补偿过的预扣不许再退一次: report=%+v stock_before=%d stock_after=%d", report, stockBefore, stockAfter)
	}
	if report.AlreadyReconciled != failures {
		t.Fatalf("投递失败那几条台账应当在下单路径上就结案: want already=%d got %+v", failures, report)
	}
}

// 两个对账进程同时判定**同一批**台账（生产里就是两个副本，或重启后的新旧交叠）：
// 同一条泄漏只能被退一次。这是「判定仍是 pending + 归还 + 改状态」必须压成一步的直接证据。
func TestM5CP2ConcurrentReconcilersRefundEachLeakOnce(t *testing.T) {
	db, rdb := openM5CEnv(t)
	ctx := context.Background()
	const productID int64 = 10575
	const initialStock int64 = 20
	const attempts, crashes = 10, 3
	seedLeakScenario(t, db, rdb, productID, "m5c-p2-race", initialStock, attempts, crashes)
	now := baseTime.Add(reconcileWindow + time.Second)

	// 两个进程扫到的是同一批台账：它们各自的快照里，那 3 条泄漏都还是 pending。
	entries, err := ScanLedger(ctx, rdb, 100)
	if err != nil {
		t.Fatalf("扫描台账: got %v", err)
	}
	if len(entries) != attempts {
		t.Fatalf("台账条数不对: want=%d got=%d", attempts, len(entries))
	}

	const runners = 2
	refunded := make([]int, runners)
	errs := make([]error, runners)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < runners; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // 同时起跑，让两个进程真的撞在同一条台账上
			for _, e := range entries {
				outcome, err := ReconcileEntry(ctx, rdb, db, e, reconcileWindow, now)
				if err != nil {
					errs[i] = err
					return
				}
				if outcome == OutcomeRefunded {
					refunded[i]++
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()

	totalRefunded := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("并发对账 runner=%d: got %v", i, err)
		}
		totalRefunded += refunded[i]
	}
	identity, err := CheckIdentity(ctx, rdb, db, productID, initialStock)
	if err != nil {
		t.Fatalf("并发对账后核对恒等式: got %v", err)
	}
	t.Logf("两个对账进程判定同一批台账：各自退款笔数=%v 合计=%d；恒等式=%+v", refunded, totalRefunded, identity)

	if totalRefunded != crashes {
		t.Fatalf("同一条泄漏被判定退款不止一次: want=%d got=%d per_runner=%v", crashes, totalRefunded, refunded)
	}
	if identity.Leaked != 0 || !identity.Holds() {
		t.Fatalf("并发对账后恒等式必须成立（多退会让 Leaked 变负）: got %+v", identity)
	}
}

// 对账 job 挂在 m05 的 overload.Shutdown 下，停机时必须能中途停手，
// 而且停手不能留下「退了一半」的账。
func TestM5CP2ReconcileOnceHonorsCanceledContext(t *testing.T) {
	db, rdb := openM5CEnv(t)
	const productID int64 = 10576
	const initialStock int64 = 20
	const attempts, crashes = 10, 3
	seedLeakScenario(t, db, rdb, productID, "m5c-p2-ctx", initialStock, attempts, crashes)
	stockBefore := redisStock(t, rdb, productID)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	report, err := ReconcileOnce(canceled, rdb, db, reconcileWindow, baseTime.Add(reconcileWindow+time.Second))
	stockAfter := redisStock(t, rdb, productID)
	t.Logf("ctx 已取消时对账：report=%+v err=%v；库存 %d → %d", report, err, stockBefore, stockAfter)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx 取消必须原样传出去，不能被吞成 nil: got %v report=%+v", err, report)
	}
	if report.Refunded != 0 || stockAfter != stockBefore {
		t.Fatalf("取消之后不该再退任何库存: report=%+v stock_before=%d stock_after=%d", report, stockBefore, stockAfter)
	}
}
