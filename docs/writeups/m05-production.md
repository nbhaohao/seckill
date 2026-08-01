# m05 write-up · 过载治理与一期收官：背压 / 限流 / 熔断降级 / 优雅关闭

> 场景题格式：题目原文 → 我的系统怎么解 → 数字对比（真实数字）→ 权衡与踩坑。
> 数字全部现场跑出来填，不写死目标值（COURSE_SPEC 性能数字纪律）。

## 题目原文

「m01–m04 已经保证了不超卖、不重复、最终一致。可当入口流量超过 DB 与 Kafka 能持续消化的速度，
系统该在哪一层说不？怎么证明拒绝一部分请求，比让所有请求一起慢死更负责任？」

## 我的系统怎么解

- **p1 · 池耗尽 + 有界背压**：`Admission` 用 `chan struct{}`（容量=`capacity`）当槽位，`Acquire`
  三路 `select`——拿到槽位、`time.After(waitBudget)` 超时、`ctx.Done()` 取消——判定必须发生在任何
  业务副作用（Redis 预扣）之前；`Release`/`Stats` 在同一把 `mu` 下维护 `inFlight`/`accepted`/
  `rejected`。实测 `capacity=2, waitBudget=50ms`：第三次 `Acquire` `waited=51.182125ms` 后返回
  `ErrAdmissionFull`，快照 `{Capacity:2 InFlight:2 Accepted:2 Rejected:1}`；取消/释放/再获取一整轮
  循环后 `InFlight` 精确归零，8 条断言全过。
- **p2 · 令牌桶限流**：`NewTokenBucket` 构造即把 `tokens` 钉死为 `capacity`（不依赖首次调用的"惰性
  补全"侥幸），`Allow`/`Tokens` 都在同一把锁内按 `elapsed × refillPerSecond` 补充并封顶
  `capacity`；`Tokens()` 只读不持久化，`Allow()` 才真正写回状态。实测 `capacity=5,
  refillPerSecond=10`：burst 恰好放行 5 个后耗尽（`tokens=0.00`），300ms 后补到 `3.0000`，10s 空闲
  后封顶在 `5.0000` 不溢出；冻结时钟下 200 个 goroutine 并发调 `Allow`，恰好放行 100 个（`-race`
  干净），5 条断言全过。
- **p3 · 熔断 + 诚实降级**：三态机（`closed`/`open`/`half-open`）用连续失败次数（不是失败率）跳
  闸，`State()` 自己检测冷却期是否已过并惰性转态（不依赖下一次 `Do` 调用才发现）；`half-open` 用
  `probesInFlight` 限制并发探针数，`fn` 必须在锁外调用（否则并发探针测试会直接死锁）。写路径
  `WritePathFailure` 只有 `ErrRateLimited` 对应 429，其余一律 503，`Degraded` 硬编码 `false`；
  `ReadPathFallback` 有旧值才允许 `degraded=true`。实测跳闸时间线 `closed closed open`，`open` 期
  间 3 次调用 `fn` 计数不再增加；半开轮 5 个并发请求恰好 2 个（`HalfOpenProbes`）真正探测、3 个被
  拒；完整轮回 `closed→open→half-open→closed→open→half-open→open`；五种错误状态码
  `breaker_open/admission_full/context_deadline_exceeded/custom_downstream_error→503`，
  `rate_limited→429`，`Degraded` 全部 `false`，17 条断言全过。
- **p4 · 优雅关闭**：`Shutdown` 按声明顺序执行 `ShutdownStep`，每步开始前先 `select` 探
  `ctx.Done()`（`default` 分支不阻塞），到期直接记 `Failed`+`ErrShutdownTimeout` 并返回，不调用该
  步的 `Fn`；成功则逐步追加进 `Completed`（不是最后批量复制声明列表）。单元测试：正常轮五步顺序
  `[stop-http drain-inflight stop-consumer flush-producer close-deps]` 全部 `Completed`；超时轮
  `Completed=[stop-http drain-inflight]`，`Failed="stop-consumer"`，`stop-consumer` 的 `Fn`**从未
  被调用**（用 `started` 切片直接验证），7 条断言全过。真实二进制验证：构建出 `go-seckill-api`，
  `curl /healthz` 200 后发真实 `kill -TERM`，日志里五步 `shutdown step start`/`shutdown step done`
  完整顺序正确，`graceful-shutdown transcript: completed=[stop-http drain-inflight stop-consumer
  flush-producer close-deps] failed="" err=<nil> elapsed=8.484167ms`——证明 `Shutdown` 真的被接进
  了 `main.go` 的 `signal.NotifyContext` 路径，不只是单测里的纸面正确。
- **p5 · 全链路压测**：`checks_m05.sh` 前六节（起依赖、迁移建 topic、fmt/vet/build、m05 单测、
  m01–m04 回归）全绿；最后一节因为脚本用了 `mapfile`（bash4+ 内置命令，这台 mac 默认 bash 3.2.57
  没有）跑不动断言逻辑，手动复刻同一套构建-起进程-发 SIGTERM-读日志的步骤，拿到跟上面 p4 一致的真
  实 transcript。`loadtest_m05.sh` 跑了 unbounded（`ADMISSION_CAPACITY=100000`）vs bounded
  （`ADMISSION_CAPACITY=20`）两轮，数字见下表。

单元测试证据（`go test -race ./internal/overload/... -run '^TestM05' -v -count=1`，37 条断言全部
PASS，`-race` 干净）+ `checks_m05.sh` 前六节绿 + 手动补的真实 SIGTERM transcript + m01–m04 回归全
绿。

## 数字对比（`./scripts/loadtest_m05.sh`，同商品同 workload：15s @ 500rps，7500 请求，
`RATE_LIMIT_*` 拉到极大只让 admission 容量当唯一变量）

| | unbounded（cap=100000） | bounded（cap=20） |
|---|---|---|
| Requests / rate / throughput | 7500 / 500.07 / 500.04 | 7500 / 500.07 / 500.05 |
| Latencies min/mean/p50/p90/p95/**p99**/max | 314.791µs / 896.219µs / 694.408µs / 1.265ms / 1.69ms / **3.706ms** / 37.047ms | 268.5µs / 858.7µs / 666.387µs / 1.283ms / 1.73ms / **4.113ms** / 26.99ms |
| Status Codes | 202:7500 | 202:7500 |
| admission accepted/rejected | 7500 / 0 | 7500 / 0 |
| DB 连接池 WaitCount/WaitDuration | 0 / 0 | 0 / 0 |
| Kafka lag 峰值 / 排空秒数 | 3230 / 40s | 3075 / 28s |
| 恒等式（初始库存-DB剩余≡订单数） | OK（7500=7500） | OK（7500=7500） |

**两轮几乎没有差异，`bounded` 轮 admission `rejected` 恒为 0——这是本次实验最重要的真实发现，见
下方权衡与踩坑第一条，不是脚本或实现有 bug。**

## 迁移题

场景：Kafka 变慢导致 `ProduceSync` 的 p99 从毫秒级涨到秒级，同时 DB 连接池 `WaitCount` 快速攀升、
`InUse` 贴着 `MaxOpenConns`，请求进入的速率本身没有明显变化。

1. **哪一道门最有效**：`Admission`（有界并发槽）。它直接限制"同时在途的工作量"，跟处理慢的原因
   是 Kafka 还是 DB 无关；令牌桶只按固定速率放行、不感知处理耗时变长，管不住这类症状；熔断只包
   它所保护的那一次远程调用（比如单独包住 `ProduceSync`），不会在第一时间兜住"同时在途量暴涨"这
   个系统性症状。
2. **给 `ProduceSync` 加熔断，`Open` 期间该返回什么**：503，**不能**返回 202。202 意味着"已受
   理、后台在处理"，但 `Open` 态下 `ProduceSync` 根本没被调用，订单没有进入 Kafka，返回 202 会让
   调用方误以为异步流程已经启动，制造无法对账的幽灵订单。
3. **哪种失败绝对不能降级成成功**：写路径（下单、预扣、投递 Kafka）的任何失败。不管故障原因是
   Kafka 慢还是 DB 满，只要这次写没有真正完成，返回 2xx 就会让调用方相信一笔并不存在的交易发生
   了——这条红线不因故障原因不同而改变。

## 权衡与踩坑

- **异步端点没能激活 admission 的差异化效果，这是实验设计问题，不是实现 bug**：`/debug/orders/
  async` 收到请求后只需要把订单编码丢进 Kafka 生产者队列就返回 202，DB 写入是消费者异步做的，不
  占用 HTTP 请求的处理时间。Admission 槽位占用的时间段是"`Acquire` 到 `Release`"，对应这个 handler
  的全部工作——而这段工作耗时在亚毫秒级（中位数 0.67~0.69ms）。500rps 意味着平均每 2ms 来一个请
  求，每个请求占槽时间远小于 2ms，20 个槽位在这种周转速度下绰绰有余，永远不会同时堆满。真正能让
  admission 饱和的是处理耗时变长（比如走真实同步写 DB 的下单路径）或速率远超这个组合能承受的量
  级——这个教训跟 P2 microThink（限制在途量的门只有在处理耗时变长时才真正生效）完全对应，只是这
  次是在真实压测里撞见的，而不是纸面推演。
- **不可复跑的脚本断言比没有断言更危险**：`checks_m05.sh` 最后一节用了 `mapfile`（bash4+ 内置命
  令），在默认 bash 3.2.57 的 macOS 上直接命令找不到而失败，且这一失败发生在真正有意义的断言（五
  步顺序核验）之前——如果没有意识到这是环境问题就直接判定"P4 实现有问题"，会冤枉一段已经被单元测
  试和真实二进制验证过的正确代码。这条印证了 P5 反复强调的"冻结实验条件"纪律：机器规格、Shell 版
  本这类环境细节不写清楚，同一个脚本换台机器就可能给出误导性的失败信号。
- **`State()` 在读取路径里做状态转移，是这门课里唯一一次"getter 有副作用"**：P2 的 `Tokens()` 刻
  意设计成不持久化（返回值只是连续估算，现算现扔完全没问题），但 P3 的 `state` 是离散状态机节
  点，`open→half-open` 的转变是一个客观已经发生、只是还没被记录的事实，`State()` 必须把它如实写
  回，否则 `Do()` 下次判断时看到的还是过期状态。这个设计选择容易被直觉误判为"违反只读原则"，实际
  上是两种不同性质的返回值（连续估算 vs 离散状态）该有不同的写回规则。
- **写路径/读路径降级规则的分野来自"制造新事实 vs 复述旧事实"**：写操作没有"旧值"可以顶替这次
  操作——不存在一个"过去的下单"能替代"这次的下单"，所以 `WritePathFailure` 只能失败，不能降级成
  带旧数据的成功；读操作查的是"已经存在的状态"，借用旧值只是复述一个曾经真实的事实，不会凭空制
  造新事实，所以 `ReadPathFallback` 允许 `degraded=true`。
- **共享 deadline 不是为了"关闭更完整"，是为了让总时长可预测**：如果每一步各自发明私有超时，几
  步加起来很容易超过运维给的 `terminationGracePeriodSeconds` 这个单一数字，而且每步各自安全不代
  表整体安全。`Shutdown` 用同一个 `ctx` 贯穿所有步骤，前面步骤多花的时间真实压缩后面步骤的预算，
  这样才能诚实反映"关闭总共花了多久、还剩多少"——这也是为什么一个平时只表现为"吞吐慢慢走低"的槽
  位泄漏 bug，到了真实 SIGTERM 那一刻会变成一个指名道姓的失败点（`Failed=drain-inflight`），而不
  是让整个进程莫名其妙挂起。
