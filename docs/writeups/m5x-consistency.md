# m5x write-up · 一期一致性收官：延迟关单 / 分段库存 / 进程外对账

> 数字全部现场跑出来填，不写死目标值（COURSE_SPEC 性能数字纪律）。
> 本篇串联 sk-m5a（延迟关单与库存归还）、sk-m5b（分段库存与热点打散）、sk-m5c（对账补偿），
> 并回指 m04（异步化延迟与 lag 证据）——四组证据共同回答一期"库存生命周期与最终真相"这道题。

## 场景题与结论

「Redis 已预扣、Kafka 尚未收到消息时进程死亡，谁能发现库存没有变成订单，并在不误退正常在途订单的
前提下把账拉平？」

结论：本课选的是**事后对账**路线，不是事前原子（outbox/2PC/TCC/Saga）。下单路径先把"预扣发生过"这
个事实写进 Redis 台账，独立的对账 job 在过了一个基于真实排空数据推出来的时间窗后，凭台账年龄和 DB
订单双判据，用一段 Lua 原子归还库存并结案。sk-m5a 处理的是同一道题在"订单创建后超时未支付"这个分支
上的对称版本，sk-m5b 处理的是同一份库存在高并发下的热点问题，三者共同构成一期库存从预扣到最终归宿
的完整闭环。

## 拓扑与不变量

```
Redis: seckill:stock:<id>（库存真相）+ seckill:ledger:<requestID>（预扣台账）
Kafka: order.created（异步落库）+ order.created.DLT（死信）
MySQL: orders（唯一裁判，request_id 唯一索引）
```

四条不变量（贯穿 sk-m5c）：

1. 进程死在预扣与投递之间，补偿不执行，恒等式必须显式破坏（sk-m5c p1）
2. 台账 key 固定 `seckill:ledger:<requestID>`，`CreatedAt` 不因同 `requestID` 重试而刷新（`SetNX`）
3. 只有过窗口且 DB 不存在对应订单的 pending 台账可以归还；归还与结案不可拆（Lua 原子）
4. 重复或并发对账不多退，`ctx` 取消后不再开始下一条，最终证据同时报告前后恒等式与分类账单

`ReconcileEntry` 三道闸顺序固定：State → 时间窗 → DB，任何一步提前或延后都会引入误判（细节见下）。

## 实验方法

- **sk-m5c P1/P2**：`go test -race ./internal/reconcile/... -run '^TestM5C' -v -count=1`。时间用显式
  `now`/`baseTime` 推进，不依赖真实时钟；每条测试用独立 `productID`（10571–10577）并在开头
  `ResetProduct`/`setRedisStock`/`resetLedger`，互不污染。
- **sk-m5a**：`go test -race ./internal/expire/... -run '^TestM5A' -v -count=1`。P1 并发 claim 用 8
  个 worker 抢 200 条到期订单；P2 重启幂等用真实"claim 后进程重启"两阶段模拟。
- **sk-m5b**：`./scripts/loadtest_m5b.sh`，vegeta 200rps × 10s × 2000 请求，同商品同 workload，唯一
  变量是库存放单 key（`lua`）还是 8 桶分段（`bucket`），每轮开始前脚本自己重置商品与库存状态。
- **m04（回指）**：`./scripts/loadtest_m04.sh`，同样 10s@200rps@2000 请求，比较同步落库（`sync`）与
  预扣后异步落库（`async`）。

## before / after 真实数字

**sk-m5c P1（进程死亡 vs 投递失败，`TestM5CP1*`）**

| | 崩溃组（10 发，3 崩） | 对照组（10 发，4 次 produce 失败） |
|---|---|---|
| RedisDeducted | 10 | 6 |
| DBOrdered | 7 | 6 |
| Leaked | **3** | **0** |
| Holds() | false | true |

**sk-m5c P2（对账前后，`TestM5CP2ReconcileWaitsForWindowThenRestoresIdentity`）**

| | 对账前 | 窗口内（age=1.5s<3s） | 窗口外（age>3s） |
|---|---|---|---|
| Leaked | 3 | 3（不变） | **0** |
| Report | — | `{Refunded:0 Young:10}` | `{Refunded:3 RefundedQty:3 Settled:7}` |

重复轮（幂等，`TestM5CP2RepeatRunRefundsNothingMore`）：第一轮 `{Refunded:3}` 库存=13 → 第二轮
`{Refunded:0 AlreadyReconciled:10}` 库存=13（不变）。

并发对账（`TestM5CP2ConcurrentReconcilersRefundEachLeakOnce`）：两个 goroutine 同批台账，各自退款
`[3 0]`，合计 3，不是 6；`Leaked` 回到 0。

**sk-m5a（延迟关单，`TestM5AP1`/`TestM5AP2`）**

| 场景 | 真实输出 |
|---|---|
| 8 worker 并发 claim 200 条到期订单 | `total_claims=200 unique=200 duplicated=0` |
| claim 后进程重启再关单 | `claimed orderIDs before stop=[10522001] after restart=[10522001]; RowsAffected=[1 0]; Redis stock before=8 after=10` |

**sk-m5b（单 key vs 分桶，`loadtest_m5b.sh`，2000 请求 @ 200rps）**

| | lua（单 key，before） | bucket（8 桶，after） |
|---|---|---|
| Latencies mean/p50/p95/**p99**/max | 4.079ms / 3.44ms / 5.954ms / **28.478ms** / 38.075ms | 15.119ms / 3.619ms / 100.2ms / **237.319ms** / 399.183ms |
| EVALSHA calls / usec_per_call | 2000 / 27.68 | 2000 / 25.80 |
| DB 连接池 wait_count / wait_duration_total | 0 / 0s | **111 / 7.984s** |
| 桶内分布（8 桶，目标各 125000） | — | 124721–124792（波动 ≤0.23%） |
| 恒等式 | OK（DB 剩余=998000 已扣=2000 订单数=2000） | OK（同上） |

**m04（回指，`loadtest_m04.sh`，2000 请求 @ 200rps）**

| | sync | async |
|---|---|---|
| p99 | 71.94ms | **5.254ms** |
| 202→DB 可见 | 不适用 | **85.787417ms** |
| Kafka lag | 不适用 | 峰值 **7**，排空到 0 约 **2s** |

## 归因与证据指向

- **sk-m5c P1 的差额只能归因于进程死亡，不是笼统的"投递失败"**：两组测试唯一的自变量是进程是否
  存活。崩溃组 `crash.Fire()` 卡在 `PreDeduct` 与 `produce` 之间，`RedisDeducted` 精确等于全部尝试
  数（不许回滚）；对照组进程存活，`produce` 报错后 `RollbackPreDeduct` 生效，两个数字重新相等。
- **窗口存在的意义由 P2 的两轮对账直接证明**：窗口内（age=1.5s）`Refunded=0`，全部 10 条落在
  `Young`；过窗口后才会真正判定并归还——如果窗口判断被挪到 DB 查询之后，一条刚预扣完、消息还在
  Kafka 排队的合法请求会被 DB 查询的"此刻无订单"结果直接误判成泄漏，这不是本次跑出的数字，是这次
  教学环节里推导过的必然结论。
- **重复与并发不多退，证据分别指向 `settleEntry` 的两个不同保护**：重复轮 `AlreadyReconciled=10`
  依赖 `e.State` 快照判断（跳过已结案台账，省一次窗口与 DB 查询）；并发轮合计只退 3 笔依赖的是
  `settleEntry` 内部 Lua 对 Redis **当前**状态的重新判断——两者不是同一层保护，快照判断挡不住并发，
  真正的并发安全阀门在 Lua 脚本的原子性上。
- **sk-m5b 分桶轮的延迟反而更差，证据指向 DB 连接池，不是分桶机制本身**：`bucket` 轮
  `go_sql_wait_count_total=111`、累计等待 `7.984s`，`go_sql_open_connections` 从 8 涨到 20——这是这
  次真实环境下观测到的现象，不是"分桶必然更慢"的结论。桶内库存分布本身是均匀的（8 桶波动 ≤0.23%，
  探桶的 hash 打散有效），Redis 侧 `EVALSHA` 单次耗时（27.68μs vs 25.80μs）两轮几乎相同，说明代价
  发生在 DB 连接层而不是 Redis 层——具体是这次压测期间的连接池争用还是分桶路径本身多了一次 DB 往
  返，本篇没有继续拆解，留在下一步。
- **m04 的窗口来源，sk-m5c 的 3 秒窗口才不是拍脑袋**：async 轮 lag 峰值 7、排空到 0 约 2s，sk-m5c
  测试取 3 秒（≈ 1.5 倍余量）；这次 sk-m5b 与 m04 的压测都在同一台机器上跑，量级可以互相印证。

## 权衡与踩坑

- **残留窗口没有被消灭，只是被压缩**：`EnqueueWithLedger` 把泄漏窗口从"预扣到投递完成"压缩到"预扣
  到写台账"两条相邻的 Redis 命令之间，进程仍可能死在这两条命令中间。彻底消除需要把台账与预扣塞进
  同一段 Lua，但预扣脚本是 m03 已冻结的东西，本课不改。
- **窗口设短了会反向造成超卖**：如果窗口短于真实积压（比如这次压测显示的 lag 排空需要 2 秒，窗口却
  设成 1ms），仍在预扣到落库之间的合法订单会被误判成泄漏、库存被提前归还；消息随后落库，同一份库
  存已经被系统卖给了两个人。止血手段是拉长窗口（代价：真实泄漏恢复更慢）或对账前检查实时 lag/
  backlog（代价：对账依赖队列观测的准确性与可用性），这条链路目前没有做后者。
- **outbox 用不了，不是没做，是做不了**：outbox 的原子性来自"业务写 + 待发事件写"进同一个本地数据
  库事务；这条链路预扣在 Redis、投递目标在 Kafka、落库在 MySQL，三个独立系统没有共同的事务边界，
  outbox 无法覆盖这次预扣动作。
- **不上 2PC/TCC/Saga 是规模判断，不是图省事**：TCC 要给每个参与者设计 Try/Confirm/Cancel 并各自
  保证幂等，Saga 适合更长的多服务补偿编排——当前链路只有两三个参与者、已有 DLT 加事后对账覆盖失败
  面，引入这些框架是在已覆盖的问题上重复建设，还是消除不了三个存储没有共同事务边界这个根本限制。
- **手动集成脚本 `checks_m5c.sh` 本次没有产出可信数字**：脚本传的 `?stock=` 参数被
  `/debug/deduct/warm/:id` 忽略（该 handler 直接从 MySQL `products.stock` 读值，不接受覆盖），跑出
  来的 `Leaked`/`RedisDeducted` 因为起始库存假设错误而失真，没有采信，本篇 before/after 全部改用
  `go test -race` 的确定性输出与 `loadtest_m5b.sh`/`loadtest_m04.sh` 的真实压测数字。

## 下一步（只列不做）

- 不落地 outbox 或本地消息表，不引入 Seata、2PC、TCC、Saga 框架、跨库事务、告警平台或工单系统
- 不把台账与 m03 冻结的预扣脚本合并，不改 m05 冻结的下单 HTTP 契约
- 不修 `scripts/checks_m5c.sh` 与 `/debug/deduct/warm/:id` 的 `?stock=` 参数不匹配问题（不在
  sk-m5c 的 `forbiddenScope` 允许范围内，需要单独立项）
- 不拆解 sk-m5b 分桶轮 DB 连接池争用的具体根因（连接池争用 vs 分桶路径多一次 DB 往返），留给后续
  单独的性能剖析
- 不对账前检查实时 lag/backlog 再下终局判断——当前对账只依赖固定时间窗，这是本课明确保留的简化
