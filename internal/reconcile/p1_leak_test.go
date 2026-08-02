package reconcile

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nbhaohao/go-seckill/internal/order"
	"github.com/nbhaohao/go-seckill/internal/testutil"
)

// 事故复现：进程死在「Redis 预扣成功」与「Kafka 投递完成」之间，
// 那几发请求的库存既没有变成订单，也没有被任何人还回来。
// 这条测试的价值在 -v 输出里的那三个数字，不只在断言绿不绿。
func TestM5CP1CrashBetweenPreDeductAndProduceLeaksStock(t *testing.T) {
	db, rdb := openM5CEnv(t)
	ctx := context.Background()
	const productID int64 = 10571
	const initialStock int64 = 20
	const attempts = 10
	const crashes = 3
	testutil.ResetProduct(t, db, productID, int(initialStock))
	setRedisStock(t, rdb, productID, initialStock)
	resetLedger(t, rdb)

	crash := NewCrashSwitch(crashes)
	crashed, accepted := 0, 0
	for i := 0; i < attempts; i++ {
		requestID := fmt.Sprintf("m5c-p1-leak-%02d", i)
		req := order.PlaceOrderRequest{RequestID: requestID, ProductID: productID, UserID: 7001, Quantity: 1}
		err := EnqueueWithCrash(ctx, rdb, req, func(ctx context.Context, r order.PlaceOrderRequest) error {
			// 投递成功即视为消费者随后把这一单落进了 DB。
			landOrder(t, db, 105710000+int64(i), productID, r.RequestID, r.Quantity)
			return nil
		}, crash)
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, ErrSimulatedCrash):
			crashed++
		default:
			t.Fatalf("下单只应出现成功或注入的崩溃: request_id=%s got %v", requestID, err)
		}
	}

	identity, err := CheckIdentity(ctx, rdb, db, productID, initialStock)
	if err != nil {
		t.Fatalf("核对恒等式: got %v", err)
	}
	t.Logf("注入崩溃 %d / 共 %d 发：受理=%d 崩溃=%d；初始库存=%d 剩余=%d → Redis 扣了 %d 件，DB 订单 %d 件，泄漏 %d 件",
		crashes, attempts, accepted, crashed, identity.InitialStock, identity.RedisRemaining,
		identity.RedisDeducted, identity.DBOrdered, identity.Leaked)

	if crashed != crashes || accepted != attempts-crashes {
		t.Fatalf("崩溃开关必须精确消耗额度: want crashed=%d accepted=%d got crashed=%d accepted=%d remaining=%d",
			crashes, attempts-crashes, crashed, accepted, crash.Remaining())
	}
	// 崩掉的那几发也扣了库存，而且没有任何人把它还回来。
	if identity.RedisDeducted != attempts {
		t.Fatalf("崩溃路径不许回滚预扣（回滚了就复现不出事故）: want deducted=%d got %+v", attempts, identity)
	}
	if identity.DBOrdered != int64(attempts-crashes) {
		t.Fatalf("只有投递成功的请求才该落库: want db_ordered=%d got %+v", attempts-crashes, identity)
	}
	if identity.Leaked != crashes || identity.Holds() {
		t.Fatalf("恒等式此刻必须是破的，泄漏量应等于崩溃发数: want leaked=%d got %+v", crashes, identity)
	}
}

// 对照组：同一条路径上，投递**失败**（进程还活着）时 m04 的补偿是有效的。
// 两条测试摆在一起，才能把「泄漏」的成因精确定位到进程死亡，而不是笼统地怪 Kafka。
func TestM5CP1ProduceFailureIsCompensatedAndKeepsIdentity(t *testing.T) {
	db, rdb := openM5CEnv(t)
	ctx := context.Background()
	const productID int64 = 10572
	const initialStock int64 = 20
	const attempts = 10
	const failures = 4
	testutil.ResetProduct(t, db, productID, int(initialStock))
	setRedisStock(t, rdb, productID, initialStock)
	resetLedger(t, rdb)

	produceErr := errors.New("produce: broker unavailable")
	failed, accepted := 0, 0
	for i := 0; i < attempts; i++ {
		requestID := fmt.Sprintf("m5c-p1-produce-%02d", i)
		req := order.PlaceOrderRequest{RequestID: requestID, ProductID: productID, UserID: 7001, Quantity: 1}
		// 崩溃开关不武装：这一组的失败全部发生在进程还活着的时候。
		err := EnqueueWithCrash(ctx, rdb, req, func(ctx context.Context, r order.PlaceOrderRequest) error {
			if i < failures {
				return produceErr
			}
			landOrder(t, db, 105720000+int64(i), productID, r.RequestID, r.Quantity)
			return nil
		}, NewCrashSwitch(0))
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, produceErr):
			failed++
		default:
			t.Fatalf("投递失败必须原样返回 produce 的错误: request_id=%s got %v", requestID, err)
		}
	}

	identity, err := CheckIdentity(ctx, rdb, db, productID, initialStock)
	if err != nil {
		t.Fatalf("核对恒等式: got %v", err)
	}
	t.Logf("投递失败 %d / 共 %d 发：受理=%d 失败=%d；剩余库存=%d → Redis 扣了 %d 件，DB 订单 %d 件，泄漏 %d 件",
		failures, attempts, accepted, failed, identity.RedisRemaining, identity.RedisDeducted, identity.DBOrdered, identity.Leaked)

	if failed != failures || accepted != attempts-failures {
		t.Fatalf("投递失败发数不符: want failed=%d accepted=%d got failed=%d accepted=%d", failures, attempts-failures, failed, accepted)
	}
	// 进程活着，补偿就跑得了：失败的那几发预扣被还回去，恒等式没有破。
	if identity.RedisDeducted != int64(attempts-failures) || identity.DBOrdered != int64(attempts-failures) {
		t.Fatalf("投递失败的预扣必须被回滚: want deducted=db_ordered=%d got %+v", attempts-failures, identity)
	}
	if identity.Leaked != 0 || !identity.Holds() {
		t.Fatalf("这一组不该有泄漏: got %+v", identity)
	}
}
