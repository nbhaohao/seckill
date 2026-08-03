package canary

import (
	"context"

	"github.com/nbhaohao/go-seckill/internal/ports"
)

type Version string

const (
	VersionV1 Version = "v1"
	VersionV2 Version = "v2"
)

// SelectVersion 是 sk-m06 p5 S1：把 user_id 稳定映射到 v1/v2 版本池。
// 1. 为什么只读 userID 与 percent：同一用户在同一显式配置下必须粘在同一版本，不能混入时间或随机数。
// 2. 为什么 0/100 先处理：0 是一键回滚态，100 是全量验证态，边界语义不能靠哈希碰巧成立。
// 3. 为什么只返回版本、不拨号：纯函数才能重复验证 cohort，连接与治理留在 gateway 样板层。
// AI 将在 p5 学习时分切片实现。
func SelectVersion(userID int64, percent int) Version {
	panic("TODO: sk-m06 p5 SelectVersion")
}

type Router struct {
	v1      ports.AsyncOrderService
	v2      ports.AsyncOrderService
	percent int
}

// NewRouter 已就位（AI 生成）：比例是启动快照；回滚用 percent=0 重启，而不是在这里造配置中心。
func NewRouter(v1, v2 ports.AsyncOrderService, percent int) *Router {
	return &Router{v1: v1, v2: v2, percent: percent}
}

// PlaceOrderAsync 是 sk-m06 p5 S2：按稳定决策选择一个版本池，并只调用该池一次。
// 1. 为什么 key 固定取 req.UserID：requestID 会随请求变化，无法让同一用户保持版本粘滞。
// 2. 为什么先选池再调用：一次请求不能同时探测两个池，否则灰度写会产生双重副作用。
// 3. 为什么原样传 ctx/req：deadline、requestID 幂等键与既有 HTTP 契约都不能在路由层漂移。
// AI 将在 p5 学习时分切片实现。
func (r *Router) PlaceOrderAsync(ctx context.Context, req ports.PlaceOrderRequest) (*ports.AcceptedOrder, error) {
	panic("TODO: sk-m06 p5 Router.PlaceOrderAsync")
}

var _ ports.AsyncOrderService = (*Router)(nil)
