package bucket

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

// ── sk-m5b p2 · 铺桶与总量核对（before/after 与碎片证据的地基）─────────────
//
// p1 解决了「一发请求打哪个桶」；这一关解决「库存一开始怎么摊进 N 个桶」
// 以及「摊完之后我怎么知道总量还对」。压测要对照单 key 版，第一件事就是让两边
// 从同一个总量出发；碎片这件事能不能被看见，也取决于能不能把每个桶单独读出来。

// SpreadStock 是 p2 S1：把 products.stock 读出来，摊进 productID 的 cfg.BucketCount 个桶。
//
// 要实现的形状：
//  1. 从 DB 读这个商品的 stock（对照 deduct.WarmStock 的做法，它读的是同一列）
//  2. 均分：每桶拿 total / N；除不尽的余数 r = total % N 分给**前 r 个桶**，
//     每桶各多 1 件。不许把余数整块塞给某一个桶——那等于人为造一个大桶，
//     热点会重新聚到它上面，before/after 就测不出分桶该有的效果了
//  3. 一次性把 N 个 key 写下去（MSet 或 pipeline 均可），
//     写的是**绝对值**而不是 INCRBY：铺桶是「设定初始状态」，重复铺两次
//     必须得到同一个状态，不能把库存越铺越多
//
// 返回每个桶实际写进去的数量（下标 = 桶号），压测脚本和测试都靠它自证起点。
func SpreadStock(ctx context.Context, rdb *redis.Client, db *sqlx.DB, productID int64, cfg Config) ([]int64, error) {
	panic("TODO: phase p2 · S1 SpreadStock 尚未实现（AI 将在 p2 学习时分切片实现）")
}

// TotalRemaining 是 p2 S2：一次读回 N 个桶的当前剩余，返回总和与逐桶明细。
//
// 要实现的形状：用一次 MGet 把 N 个 key 全取回来（不要 N 次 Get——
// 这个函数在压测每一轮结束时都要跑，它自己不该成为新的开销），
// key 不存在（nil）按 0 计，其余转成整数累加。
//
// 为什么要返回逐桶明细而不只是总和：这一关最后要证明的现象是
// 「总数不是 0，请求却失败了」——只有总和的话，这句话只是个断言；
// 有了逐桶明细，它才是一份能贴进 write-up 的证据。
func TotalRemaining(ctx context.Context, rdb *redis.Client, productID int64, cfg Config) (int64, []int64, error) {
	panic("TODO: phase p2 · S2 TotalRemaining 尚未实现（AI 将在 p2 学习时分切片实现）")
}
