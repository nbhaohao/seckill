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

## 快速开始

```bash
cp .env.example .env
docker compose up -d mysql redis    # m01 只要 mysql；m02 起要加 redis（kafka 仍是 m04 的占位）
./scripts/checks_m01.sh             # 迁移 + EXPLAIN 前后 + go test -race + 死锁复现
./scripts/checks_m02.sh             # m02 红测试 + Redis 侧证据（缓存原始值 / TTL 分布 / 空值占位符）
go run ./cmd/api                    # 另开终端起 API（:8080，m02 起需要 redis 在跑）
./scripts/loadtest_m01.sh           # keep-alive on/off 对比 + DB 池指标
./scripts/loadtest_m02.sh           # 商品读路径 缓存 on/off 对比 + 命中率
```

结果填进 `docs/writeups/m01-baseline.md`、`docs/writeups/m02-cache.md`。
