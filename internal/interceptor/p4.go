package interceptor

import (
	"time"

	"google.golang.org/grpc"

	"github.com/nbhaohao/go-seckill/internal/overload"
)

// UnaryRetry 是 sk-m06 p4 S1：只给显式列出的幂等 RPC 做有限退避重试。
// 1. 为什么用 full method allowlist：是否能重试是业务幂等契约，不能靠 status code 猜。
// 2. 为什么 MaxAttempts 包含首次调用：总请求数才能被一个明确上界封住，不制造 retry storm。
// 3. 为什么退避等待监听 ctx：入口 deadline 到了必须立即停，不能把剩余预算继续睡掉。
// AI 将在 p4 学习时分切片实现。
func UnaryRetry(maxAttempts int, baseBackoff time.Duration, idempotentMethods map[string]bool) grpc.UnaryClientInterceptor {
	panic("TODO: sk-m06 p4 UnaryRetry")
}

// UnaryBreaker 是 sk-m06 p4 S2：把一期 Breaker 放到 gRPC client 调用边界。
// 1. 为什么 invoker 必须在 Breaker.Do 里面：只有真实下游结果才能推动 closed/open 状态机。
// 2. 为什么 Open 映射成 Unavailable：调用方要收到明确失败，不能把未执行的库存写伪造成成功。
// 3. 为什么保留同一个 ctx：半开探测与正常调用都必须受入口取消和 deadline 约束。
// AI 将在 p4 学习时分切片实现。
func UnaryBreaker(breaker *overload.Breaker) grpc.UnaryClientInterceptor {
	panic("TODO: sk-m06 p4 UnaryBreaker")
}

// UnaryRateLimit 是 sk-m06 p4 S3：把一期 TokenBucket 放到 gRPC server 写入口。
// 1. 为什么用 full method allowlist：health/readiness 不该和库存写争同一批 token。
// 2. 为什么在 handler 前 Allow：拒绝必须发生在任何库存或订单副作用之前。
// 3. 为什么返回 ResourceExhausted：这是可机读的过载信号，client 才能执行受限策略。
// AI 将在 p4 学习时分切片实现。
func UnaryRateLimit(bucket *overload.TokenBucket, limitedMethods map[string]bool) grpc.UnaryServerInterceptor {
	panic("TODO: sk-m06 p4 UnaryRateLimit")
}
