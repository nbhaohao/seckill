package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// Options 是缓存层的三个时间参数。三个都必须显式给值——和 m01 p4 一个道理：
// 「TTL 忘了设」在 Redis 里等于永不过期，是缓存与 DB 长期不一致最常见的来源。
type Options struct {
	// TTL：命中回填时给 key 的基础存活时间。
	TTL time.Duration
	// TTLJitter：在 TTL 之上叠加的随机抖动上限（p2 用它防雪崩）。
	TTLJitter time.Duration
	// MissTTL：空值缓存（"这个 id 不存在"）的存活时间，必须远短于 TTL（p2 讲为什么）。
	MissTTL time.Duration
}

// DefaultOptions 已就位（AI 生成）：具体数值不是教学点（教学点是「必须显式配、且 MissTTL 必须短」），
// 给一组便于观察的保守值。
func DefaultOptions() Options {
	return Options{
		TTL:       10 * time.Minute,
		TTLJitter: 2 * time.Minute,
		MissTTL:   30 * time.Second,
	}
}

// ProductCache 是 m02 的核心对象：商品读路径的 cache-aside 缓存。
// 四个 phase 依次往它身上长本事：
//
//	p1 读路径 cache-aside + 预热
//	p2 穿透（空值缓存）与雪崩（TTL 抖动）
//	p3 击穿（并发回源用 singleflight 合并）
//	p4 一致性（更新库存时先更库再删缓存）
type ProductCache struct {
	rdb  *redis.Client
	repo ProductRepo
	opts Options

	// sf 用于 p3：把同一个 key 上并发的多次回源合并成一次。
	// 声明在这里是因为它必须是 ProductCache 的长期状态（每个 ProductCache 一组飞行中的 key），
	// 不能在方法里临时 new 一个——那样每个调用方都有自己的 Group，谁也合并不了谁。
	sf singleflight.Group
}

// New 已就位（AI 生成）：接线，不是教学点。
func New(rdb *redis.Client, repo ProductRepo, opts Options) *ProductCache {
	return &ProductCache{rdb: rdb, repo: repo, opts: opts}
}

// Get 是整个模块的主入口：读一个商品，优先走缓存。
//
// p1 要实现的形状（cache-aside 三步）：
//  1. 用 ProductKey(id) 去 Redis 取；取到就解 JSON 直接返回（命中，一次 DB 都不打）
//  2. 没取到（go-redis 用 redis.Nil 这个哨兵错误表示 key 不存在）→ 回源 DB
//  3. 回源到的值写回 Redis 并带上 TTL，下一次读就命中了
//
// 注意 ctx 必须一路传给 Redis 与 DB 的 Context API（COURSE_SPEC 连接治理纪律）：
// 缓存层挡在最前面，它要是没有超时上限，Redis 一抖整条链路就跟着挂住。
//
// p2 / p3 会继续在这个方法上加工，届时 AI 会分切片改它——p1 只做上面三步。
func (c *ProductCache) Get(ctx context.Context, id int64) (*Product, error) {
	panic("TODO: phase p1 — AI 将在 p1 学习时分切片实现 cache-aside 读路径")
}

// loadAndFill 是 Get 的"未命中"分支：回源 DB + 回填缓存。
// 单独拆成一个方法是为了让 p1 的读路径保持三步可读，也为后面的 phase 留一个明确的下手位置。
//
// p1 要实现的形状：调 c.repo.LoadProduct 拿到商品 → MarshalProduct → 写 Redis 带 TTL → 返回。
// 回填失败（Redis 挂了）该不该让整个 Get 失败？这是本 phase 的一个 review 点，
// 教案会问你：缓存是加速器还是真相源。
func (c *ProductCache) loadAndFill(ctx context.Context, id int64) (*Product, error) {
	panic("TODO: phase p1 — AI 将在 p1 学习时分切片实现回源 + 回填")
}

// Warm 是预热：秒杀开始前把已知会被打爆的商品 id 先塞进缓存，
// 避免开卖瞬间所有请求一起 miss、一起回源（这就是 p3 要正面处理的击穿场景的极端版本）。
//
// p1 要实现的形状：对每个 id 调一次回源 + 回填（复用 loadAndFill 即可），
// 遇到不存在的商品跳过、不要让整批预热失败。
func (c *ProductCache) Warm(ctx context.Context, ids []int64) error {
	panic("TODO: phase p1 — AI 将在 p1 学习时分切片实现批量预热")
}

// ttlWithJitter 给回填用的 TTL 加一点随机抖动。
// p2 要实现的形状：返回 opts.TTL 加上 [0, opts.TTLJitter) 区间内的一个随机量；
// TTLJitter 为 0 时就退化成 opts.TTL 本身。
// 为什么需要它，见 p2 教案（关键词：同一批 key 同时过期）。
func (c *ProductCache) ttlWithJitter() time.Duration {
	panic("TODO: phase p2 — AI 将在 p2 学习时实现 TTL 抖动")
}

// UpdateStock 是写路径：改库存。
//
// p4 要实现的形状：先把新库存写进 DB（c.repo.UpdateStock），成功之后再把缓存里那个 key 删掉。
// 两个顺序问题都会在 p4 教案里被追问：为什么是"先更库再删缓存"而不是反过来，
// 以及为什么是"删"而不是"把新值写进缓存"。
func (c *ProductCache) UpdateStock(ctx context.Context, id int64, stock int) error {
	panic("TODO: phase p4 — AI 将在 p4 学习时分切片实现先更库再删缓存")
}
