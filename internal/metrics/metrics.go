// 已就位（AI 生成）：/metrics 埋点是样板（照 client_golang README 用法），不是本课教学点。
// 教学点是 p4 讲的"这些数字应该看哪几个"——DB 连接池的 InUse/WaitCount/WaitDuration/
// MaxOpenConnections 由 collectors.NewDBStatsCollector 直接照官方 DB.Stats() 字段搬成 gauge，
// 压测报告拿它们归因瓶颈（COURSE_SPEC「连接治理纪律」）。
package metrics

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var (
	OrdersPlaced = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "seckill_orders_placed_total",
		Help: "下单请求按结果分类的计数（result=success|insufficient_stock|error）。",
	}, []string{"result"})

	OrderLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "seckill_order_latency_seconds",
		Help:    "单次下单请求（含 DB 往返）的耗时分布。",
		Buckets: prometheus.DefBuckets,
	}, []string{"result"})

	// ProductReads / ProductReadLatency（m02）：商品读请求按路径分类（path=cached|nocache）。
	// 命中率不在 handler 里数——handler 分不出命中还是回填，正确的算法是
	// 1 - (seckill_product_db_loads / seckill_product_reads_total{path="cached"})，
	// 分母在这里、分子是下面那个 db_loads gauge。
	ProductReads = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "seckill_product_reads_total",
		Help: "商品读请求计数（path=cached 走缓存层，path=nocache 直连 DB，用于 m02 压测对比）。",
	}, []string{"path"})

	ProductReadLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "seckill_product_read_latency_seconds",
		Help:    "商品读请求耗时分布（path=cached|nocache）。",
		Buckets: prometheus.DefBuckets,
	}, []string{"path"})
)

// Register 把业务指标 + DB 连接池指标一次性挂到给定 registry 上，cmd/api/main.go 启动时调用一次。
func Register(reg prometheus.Registerer, db *sql.DB) error {
	for _, c := range []prometheus.Collector{OrdersPlaced, OrderLatency, ProductReads, ProductReadLatency} {
		if err := reg.Register(c); err != nil {
			return err
		}
	}
	return reg.Register(collectors.NewDBStatsCollector(db, "seckill"))
}

// RegisterCacheDBLoads（m02）把"缓存层累计回源了多少次"暴露成一个指标。
// 数据源是 cache.CountingRepo.Loads()，这里只做 gauge 转接，不引 internal/cache 的类型。
func RegisterCacheDBLoads(reg prometheus.Registerer, loads func() int64) error {
	return reg.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "seckill_product_db_loads",
		Help: "商品缓存层累计回源 DB 的次数（m02 命中率的分子来源）。",
	}, func() float64 { return float64(loads()) }))
}
