package shard

import (
	"context"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/nbhaohao/go-seckill/internal/order"
)

const (
	ShardCount            = 4
	snowflakeSequenceBits = 12
)

// Route 是 sk-m06 p7 S1：把 userID 稳定映射到固定的四个订单分片之一。
// 1. 为什么必须是纯函数：同一输入在重试和进程重启后都要得到同一结果，随机数与时间会破坏这种稳定性。
// 2. 为什么先做稳定 hash 再取模：userID 的数值规律不应直接变成某个 schema 的热点规律。
// 3. 为什么返回范围锁在 [0, ShardCount)：调用侧可以安全地把结果直接用作连接池下标。
// AI 将在 p7 学习时分切片实现。
func Route(userID int64) int {
	panic("TODO: sk-m06 p7 Route")
}

// Cursor 是跨分片列表的 keyset 游标；零值表示从最新一页开始。
type Cursor struct {
	CreatedAt time.Time
	OrderID   int64
}

type Store struct {
	dbs   [ShardCount]*sqlx.DB
	probe *AccessProbe
}

// NewStore 已就位（AI 生成）：把四个连接池与可选 DB 访问探针装进薄路由层。
func NewStore(dbs [ShardCount]*sqlx.DB, probe *AccessProbe) *Store {
	return &Store{dbs: dbs, probe: probe}
}

// GetByOrderID 是 sk-m06 p7 S2：从 Snowflake 订单号的 node 位定位唯一分片，再做单库查询。
// 1. 为什么从订单号解路由：调用方只有 orderID 时仍必须知道目标库，不能退化为逐库试探。
// 2. 为什么只记录一次 probe：测试要机械证明一次直查只触碰一个连接池，而不是靠日志猜测。
// 3. 为什么原样返回查询错误：sql.ErrNoRows 与连接故障的语义不能在路由层被混成同一种结果。
// AI 将在 p7 学习时分切片实现。
func (s *Store) GetByOrderID(ctx context.Context, orderID int64) (*order.Order, error) {
	panic("TODO: sk-m06 p7 Store.GetByOrderID")
}

// List 是 sk-m06 p7 S3：从四库并行取候选行，按 (created_at,id) 归并并生成下一页游标。
// 1. 为什么每库都用同一 keyset 游标：各库候选集必须共享严格的“上一页最后一行之后”边界。
// 2. 为什么归并键带 id：created_at 相同时仍要有稳定的全序，翻页才有可验证的边界。
// 3. 为什么并行查询后只截全局 limit：单库各取 limit 只是候选，最终页大小由归并结果统一裁定。
// AI 将在 p7 学习时分切片实现。
func (s *Store) List(ctx context.Context, cursor Cursor, limit int) ([]order.Order, Cursor, error) {
	panic("TODO: sk-m06 p7 Store.List")
}

// AccessProbe 已就位（AI 生成）：只记录每个连接池被路由层访问的次数，供直查测试做机械断言。
type AccessProbe struct {
	mu     sync.Mutex
	counts [ShardCount]int
}

func NewAccessProbe() *AccessProbe { return &AccessProbe{} }

func (p *AccessProbe) record(shard int) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.counts[shard]++
	p.mu.Unlock()
}

func (p *AccessProbe) Snapshot() [ShardCount]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counts
}
