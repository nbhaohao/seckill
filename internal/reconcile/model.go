// 已就位（AI 生成）：sk-m5c 两个实现型 phase 共用的契约——预扣台账的编解码、
// 可控崩溃开关、恒等式快照与对账报表，以及那段「归还并结案」的原子脚本。
// 这些是共享脚手架，不是本模块的学习点。
package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/deduct"
	"github.com/nbhaohao/go-seckill/internal/order"
)

// LedgerPrefix 是预扣台账的 key 前缀，沿用 repo 既有的 seckill: 命名风格
// （对照 deduct.StockKey 的 seckill:stock:<productID>）。
// 对账 job 靠这个前缀 SCAN 出全部在途预扣。
const LedgerPrefix = "seckill:ledger:"

// LedgerKey：一次预扣一条台账，主键就是幂等键 requestID——
// 它同时是 orders.uk_request_id 的值，所以「台账里的这条有没有对应订单」
// 是一次主键级别的查询，不需要额外的关联表。
func LedgerKey(requestID string) string { return LedgerPrefix + requestID }

// 台账的两个状态。pending = 预扣已发生、还没确认落地；
// reconciled = 对账 job 已经给出终局判定（归还了，或确认已落库因而不必归还）。
const (
	LedgerPending    = "pending"
	LedgerReconciled = "reconciled"
)

// ErrSimulatedCrash 是 p1 注入的「进程死在这里」信号。
// 它跟别的错误有本质区别：普通错误还能走补偿，进程死亡什么都走不了——
// 这正是 p1 要让你亲眼看见的那条泄漏路径。
var ErrSimulatedCrash = errors.New("reconcile: injected crash between pre-deduct and produce")

// CrashSwitch 是可控崩溃开关：Arm 多少发，前多少发预扣就「死」在投递之前。
// 用计数器而不是随机数，是为了让泄漏条数是一个确定的期望值，
// 否则 p1 的恒等式差额就没法当证据用。
type CrashSwitch struct{ remaining atomic.Int64 }

// NewCrashSwitch 造一个会「崩」n 次的开关；n <= 0 表示永不触发。
func NewCrashSwitch(n int64) *CrashSwitch {
	s := &CrashSwitch{}
	s.remaining.Store(n)
	return s
}

// Fire 返回 true 表示这一发请求被判定为「进程在此刻死亡」。
// 并发安全：压测时多个 goroutine 同时问它，额度也只能被消耗 n 次。
func (s *CrashSwitch) Fire() bool {
	if s == nil {
		return false
	}
	for {
		left := s.remaining.Load()
		if left <= 0 {
			return false
		}
		if s.remaining.CompareAndSwap(left, left-1) {
			return true
		}
	}
}

// Remaining 是剩余崩溃额度，测试与脚本用它自证「确实崩了这么多发」。
func (s *CrashSwitch) Remaining() int64 {
	if s == nil {
		return 0
	}
	return s.remaining.Load()
}

// ProduceFunc 把 m04 的「投递进 Kafka」抽象成一个可注入的动作。
// 生产代码传的是包住 mq.EnqueueOrder 里那段 ProduceSync 的闭包；
// 测试传的是一个可控的桩，于是崩溃点与泄漏可以在没有 Kafka 的情况下复现。
type ProduceFunc func(ctx context.Context, req order.PlaceOrderRequest) error

// Entry 是一条预扣台账。CreatedAt 是对账窗口的唯一时间基准——
// 「这笔预扣发生多久了」决定了它现在是「在途」还是「该判死」。
type Entry struct {
	RequestID string    `json:"request_id"`
	ProductID int64     `json:"product_id"`
	Quantity  int       `json:"quantity"`
	CreatedAt time.Time `json:"created_at"`
	State     string    `json:"state"`
}

func EncodeEntry(e Entry) ([]byte, error) { return json.Marshal(e) }

func DecodeEntry(raw string) (Entry, error) {
	var e Entry
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		return Entry{}, fmt.Errorf("decode ledger entry: %w", err)
	}
	return e, nil
}

// RecordPreDeduct 写一条 pending 台账。已就位：它是一条命令，学习点在「谁在什么时候调它」，
// 那部分留在 p2 的 EnqueueWithLedger 里。
//
// 用 SetNX 而不是 Set：同一个 requestID 重试时台账已经存在，覆盖会把 CreatedAt
// 刷成新的当前时间——这条台账于是永远「刚刚才发生」，永远过不了对账窗口，
// 那笔泄漏就永远退不回来。台账的年龄是它唯一的判据，不许被刷新。
// 已存在时返回 nil：这不是错误，是幂等。
func RecordPreDeduct(ctx context.Context, rdb *redis.Client, req order.PlaceOrderRequest, now time.Time) error {
	payload, err := EncodeEntry(Entry{
		RequestID: req.RequestID,
		ProductID: req.ProductID,
		Quantity:  req.Quantity,
		CreatedAt: now,
		State:     LedgerPending,
	})
	if err != nil {
		return err
	}
	return rdb.SetNX(ctx, LedgerKey(req.RequestID), payload, 0).Err()
}

// ScanLedger 把当前全部台账取回来。已就位：它是一段固定的游标循环，
// 学习点在「为什么是 SCAN 而不是 KEYS」，那部分留在 p2 的讲解与自检里。
//
// KEYS 一次遍历整个 keyspace 且在单线程上跑到底，台账多起来时它会把线上请求一起卡住；
// SCAN 把这件事切成很多批小工作，代价是它只保证「扫描期间自始至终存在的 key 一定被返回」，
// 中途新增的可能扫不到——对账 job 每轮都会重跑，这个代价刚好可以接受。
func ScanLedger(ctx context.Context, rdb *redis.Client, batch int) ([]Entry, error) {
	var entries []Entry
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, LedgerPrefix+"*", int64(batch)).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			raw, err := rdb.Get(ctx, key).Result()
			if err == redis.Nil {
				continue
			}
			if err != nil {
				return nil, err
			}
			entry, err := DecodeEntry(raw)
			if err != nil {
				continue // 一条脏数据不该打断整轮对账
			}
			entries = append(entries, entry)
		}
		cursor = next
		if cursor == 0 {
			return entries, nil
		}
	}
}

// Identity 是一次恒等式核对的快照。
// Leaked = 「Redis 认为卖掉的量」减去「DB 里真实存在的订单量」；
// 它是 p1 的产出，也是 p2 对账是否成功的唯一验收口径。
type Identity struct {
	InitialStock   int64
	RedisRemaining int64
	RedisDeducted  int64
	DBOrdered      int64
	Leaked         int64
}

// Holds 就是那条一期就立下的恒等式：初始库存 − 剩余库存 ≡ 已落库订单量。
func (i Identity) Holds() bool { return i.Leaked == 0 }

// Outcome 是对账对单条台账给出的判定，ReconcileOnce 按它汇总成 Report。
type Outcome string

const (
	// OutcomeRefunded：过了窗口、DB 里仍然没有订单——判定为泄漏，归还库存。
	OutcomeRefunded Outcome = "refunded"
	// OutcomeYoung：还没过窗口，消息可能仍在途，这一轮不做任何判断。
	OutcomeYoung Outcome = "young"
	// OutcomeSettled：DB 里已经有订单，这笔预扣是正常成交，结案但不归还。
	OutcomeSettled Outcome = "settled"
	// OutcomeAlreadyReconciled：这条台账上一轮已经有终局判定，跳过。
	OutcomeAlreadyReconciled Outcome = "already"
)

// Report 是一次对账的账单。分类计数不是为了好看：
// 「退了几笔」和「因为还在窗口内所以没动几笔」必须能分开看，
// 否则窗口设得太短造成的误判会藏在一个笼统的成功数里。
type Report struct {
	Scanned           int
	Refunded          int
	RefundedQty       int64
	Young             int
	Settled           int
	AlreadyReconciled int
}

// settleScript 是「归还库存 + 把台账标成 reconciled」的原子脚本。
//
// KEYS[1] = 台账 key，KEYS[2] = 库存 key；ARGV[1] = 归还数量（0 表示只结案不归还），
// ARGV[2] = 结案后要写回的台账 JSON。
// 返回 1 表示这次调用真的做了事，返回 0 表示这条台账已经不是 pending（别人先做了）。
//
// 为什么必须是一段脚本而不是两条命令：对账 job 自己也会崩。
// 先 INCRBY 再改状态，崩在中间下次重启会再退一次；先改状态再 INCRBY，
// 崩在中间这笔库存就永久蒸发。两条命令怎么排都有一个坏窗口，
// 只有把「判定仍是 pending + 归还 + 改状态」压成一步，重复执行才是安全的。
var settleScript = redis.NewScript(`
local raw = redis.call('GET', KEYS[1])
if not raw then
  return 0
end
if cjson.decode(raw)['state'] ~= '` + LedgerPending + `' then
  return 0
end
local qty = tonumber(ARGV[1])
if qty > 0 then
  redis.call('INCRBY', KEYS[2], qty)
end
redis.call('SET', KEYS[1], ARGV[2])
return 1
`)

// settleEntry 跑一次 settleScript：把 e 结案成 reconciled，refundQty > 0 时同时把库存加回去。
// 返回 false 表示这条台账已经被别人结案，本次没有产生任何副作用。
func settleEntry(ctx context.Context, rdb *redis.Client, e Entry, refundQty int) (bool, error) {
	done := e
	done.State = LedgerReconciled
	payload, err := EncodeEntry(done)
	if err != nil {
		return false, err
	}
	n, err := settleScript.Run(ctx, rdb,
		[]string{LedgerKey(e.RequestID), deduct.StockKey(e.ProductID)},
		refundQty, string(payload),
	).Int64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}
