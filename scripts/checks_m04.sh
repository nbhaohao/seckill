#!/usr/bin/env bash
# m04 一键验证：基础设施 → kadm 固定 topic → 四步 Kafka 冒烟 → m04 正确性/证据测试 → m01–m03 回归。
# 性能/QPS 不进断言；异步收敛由 Go 测试中的 deadline 轮询负责。
set -euo pipefail
cd "$(dirname "$0")/.."

MYSQL_EXEC_DB="docker compose exec -T mysql mysql -uroot -proot seckill"
log() { printf '\n\033[1;34m== %s ==\033[0m\n' "$1"; }

log "1/7 docker compose up -d mysql redis kafka"
docker compose up -d mysql redis kafka

log "2/7 等 MySQL / Redis healthy，Kafka Ping 由 kadm 命令实核"
for svc in mysql redis; do
  for i in $(seq 1 40); do
    status="$(docker compose ps "$svc" --format json 2>/dev/null | grep -o '"Health":"[a-z]*"' || true)"
    if echo "$status" | grep -q healthy; then echo "$svc healthy"; break; fi
    sleep 2
    if [ "$i" -eq 40 ]; then docker compose logs "$svc" | tail -30; exit 1; fi
  done
done

log "3/7 应用既有迁移 + kadm 建 order.created / DLT（各 3 分区）"
$MYSQL_EXEC_DB < migrations/0003_products_version.sql
GOCACHE="${GOCACHE:-/tmp/go-seckill-build}" go run ./cmd/kafka-bootstrap

log "4/7 Kafka 四步冒烟：建 topic → 投一条 → 消费一条 → 读 lag"
GOCACHE="${GOCACHE:-/tmp/go-seckill-build}" go run ./cmd/kafka-smoke

log "5/7 go vet + go build"
GOCACHE="${GOCACHE:-/tmp/go-seckill-build}" go vet ./...
GOCACHE="${GOCACHE:-/tmp/go-seckill-build}" go build ./...

log "6/7 m04 正确性与证据测试（最终一致/回滚/重放幂等/DLT/lag）"
export TEST_DB_HOST=127.0.0.1 TEST_DB_PORT=3306 TEST_DB_USER=seckill TEST_DB_PASSWORD=seckill TEST_DB_NAME=seckill
export TEST_REDIS_ADDR=127.0.0.1:6379 TEST_KAFKA_BROKERS=127.0.0.1:9092
GOCACHE="${GOCACHE:-/tmp/go-seckill-build}" go test -race ./internal/mq/... -run '^TestM04' -v -count=1

log "7/7 m01–m03 回归（m04 只新增异步并行路径）"
GOCACHE="${GOCACHE:-/tmp/go-seckill-build}" go test -race ./... -run '^TestM0[123]' -count=1

cat <<'EOF'

全部通过。另开终端 `go run ./cmd/api` 后运行 `./scripts/loadtest_m04.sh`，
把同步/异步 p99、DB 写入速率、lag 峰值与排空时间填进 docs/writeups/m04-mq.md。
EOF
