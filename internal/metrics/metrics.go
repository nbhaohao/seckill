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
)

// Register 把业务指标 + DB 连接池指标一次性挂到给定 registry 上，cmd/api/main.go 启动时调用一次。
func Register(reg prometheus.Registerer, db *sql.DB) error {
	if err := reg.Register(OrdersPlaced); err != nil {
		return err
	}
	if err := reg.Register(OrderLatency); err != nil {
		return err
	}
	return reg.Register(collectors.NewDBStatsCollector(db, "seckill"))
}
