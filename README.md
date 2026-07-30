# go-seckill

自编生产级秒杀系统（Go + Gin + sqlx + MySQL 8 + Redis 7 + Kafka）。每个模块对应一道经典高并发
场景题，答案是自己压测出来的数字——不是刷题库。写作背景与模块路线图见配套课程仓库的
`courses/seckill-capstone/COURSE_SPEC.md`。

## m01 · 正确性基线 + 连接层地基

- p1 裸下单复现超卖 → p2 手写雪花 ID → p3 事务+行锁+幂等修好超卖 → p4 server/DB 连接治理 → p5 压测归因。

## m02 · 缓存层：穿透 / 击穿 / 雪崩 + 一致性

- p1 cache-aside 读路径 + 预热 → p2 穿透（空值缓存）与雪崩（TTL 抖动）→ p3 击穿（singleflight 合并回源）
  → p4 先更库再删缓存（并复现旧值回填的脏数据窗口）→ p5 缓存 on/off 压测 + write-up。
- 核心对象 `internal/cache.ProductCache`；回源计数器 `cache.CountingRepo` 是每个 phase 的取证工具。

## m03 · 原子扣减防超卖

- p1 DB 乐观锁两种形态 → p2 token + Lua 安全释放分布式锁 → p3 TTL 失效与看门狗
  → p4 Redis Lua 原子预扣 → p5 四条路径同 workload 对比。
- 证据统一进入 `docs/writeups/m03-oversell.md`，性能数字不写进单元测试断言。

## m04 · Kafka 削峰异步化（当前）

- p1 预扣后投 Kafka、异步落库 → p2 手动提交与 at-least-once 幂等 → p3 有界重试与 DLT
  → p4 lag/分区并行度 → p5 同步与异步链路对比。
- 202 只表示 Redis 预扣和 broker ack；最终恒等式必须等 consumer lag 归零后再检查。

## 快速开始

```bash
cp .env.example .env
docker compose up -d mysql redis kafka
./scripts/checks_m01.sh             # 迁移 + EXPLAIN 前后 + go test -race + 死锁复现
./scripts/checks_m02.sh             # m02 红测试 + Redis 侧证据（缓存原始值 / TTL 分布 / 空值占位符）
./scripts/checks_m03.sh             # 三种防超卖机制 + 恒等式与事故证据
./scripts/checks_m04.sh             # Kafka smoke + 幂等重放 + DLT + lag
go run ./cmd/api                    # 另开终端起 API（:8080，m02 起需要 redis 在跑）
./scripts/loadtest_m01.sh           # keep-alive on/off 对比 + DB 池指标
./scripts/loadtest_m02.sh           # 商品读路径 缓存 on/off 对比 + 命中率
./scripts/loadtest_m03.sh           # 悲观锁 / 两种乐观锁 / 分布式锁 / Lua 预扣
./scripts/loadtest_m04.sh           # 同步落库 vs Kafka 异步受理
```

结果填进对应的 `docs/writeups/m01-baseline.md`、`m02-cache.md`、`m03-oversell.md`、`m04-mq.md`。
m04 write-up 会在当前模块 p5 完成时产生，因此学习中途不存在是正常状态。
