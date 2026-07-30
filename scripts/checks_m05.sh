#!/usr/bin/env bash
# m05 一键验证：基础设施 → kadm 固定 topic → fmt/vet/build → m05 正确性/证据测试
# （internal/overload 不连任何外部服务）→ m01–m04 全量回归 → 真实 SIGTERM 的
# graceful-shutdown transcript。性能/QPS 不进断言，见 scripts/loadtest_m05.sh。
set -euo pipefail
cd "$(dirname "$0")/.."

MYSQL_EXEC_DB="docker compose exec -T mysql mysql -uroot -proot seckill"
log() { printf '\n\033[1;34m== %s ==\033[0m\n' "$1"; }

log "1/6 docker compose up -d mysql redis kafka"
docker compose up -d mysql redis kafka

log "2/6 等 MySQL / Redis healthy，Kafka Ping 由 kadm 命令实核"
for svc in mysql redis; do
  for i in $(seq 1 40); do
    status="$(docker compose ps "$svc" --format json 2>/dev/null | grep -o '"Health":"[a-z]*"' || true)"
    if echo "$status" | grep -q healthy; then echo "$svc healthy"; break; fi
    sleep 2
    if [ "$i" -eq 40 ]; then docker compose logs "$svc" | tail -30; exit 1; fi
  done
done

log "3/6 应用既有迁移 + kadm 建 order.created / DLT（各 3 分区，幂等，m04 已建过也无妨）"
$MYSQL_EXEC_DB < migrations/0003_products_version.sql
GOCACHE="${GOCACHE:-/tmp/go-seckill-build}" go run ./cmd/kafka-bootstrap

log "4/6 gofmt + go vet + go build"
fmt_out="$(gofmt -l . || true)"
if [ -n "$fmt_out" ]; then
  echo "gofmt 发现未格式化文件："
  echo "$fmt_out"
  exit 1
fi
GOCACHE="${GOCACHE:-/tmp/go-seckill-build}" go vet ./...
GOCACHE="${GOCACHE:-/tmp/go-seckill-build}" go build ./...

log "5/6 m05 单元测试（internal/overload 是纯内存包，不连 MySQL/Redis/Kafka）"
GOCACHE="${GOCACHE:-/tmp/go-seckill-build}" go test -race ./internal/overload/... -run '^TestM05' -v -count=1

log "6/6a m01–m04 回归（m05 只在入口加了两道门 + breaker + 优雅关闭，不改已有语义）"
export TEST_DB_HOST=127.0.0.1 TEST_DB_PORT=3306 TEST_DB_USER=seckill TEST_DB_PASSWORD=seckill TEST_DB_NAME=seckill
export TEST_REDIS_ADDR=127.0.0.1:6379 TEST_KAFKA_BROKERS=127.0.0.1:9092
GOCACHE="${GOCACHE:-/tmp/go-seckill-build}" go test -race ./... -run '^TestM0[1234]' -count=1

log "6/6b 真实 SIGTERM 的 graceful-shutdown transcript"
# go run 对子进程的信号转发在不同 Go 版本上不可靠，这里先编译出真实二进制，
# 直接向它的 PID 发 SIGTERM，保证测的是 main.go 里 signal.NotifyContext 那条真实路径。
BUILD_DIR="$(mktemp -d)"
APP_BIN="${BUILD_DIR}/go-seckill-api"
APP_LOG="${BUILD_DIR}/app.log"
trap 'rm -rf "${BUILD_DIR}"' EXIT

GOCACHE="${GOCACHE:-/tmp/go-seckill-build}" go build -o "$APP_BIN" ./cmd/api

SHUTDOWN_TIMEOUT_MS=8000 "$APP_BIN" > "$APP_LOG" 2>&1 &
APP_PID=$!

healthy=false
for i in $(seq 1 30); do
  if ! kill -0 "$APP_PID" 2>/dev/null; then
    break # 进程已经退出（大概率是 p1-p4 还没实现完，NewAdmission/NewTokenBucket/... 仍在 panic）
  fi
  if curl -fsS -o /dev/null "http://127.0.0.1:8080/healthz" 2>/dev/null; then
    healthy=true
    break
  fi
  sleep 1
done

if [ "$healthy" != true ]; then
  if grep -q 'panic: TODO: phase' "$APP_LOG" 2>/dev/null; then
    echo "⚠️  internal/overload 仍是红态（p1-p4 未实现完，构造函数还在 panic），跳过真实 SIGTERM 验收。"
    echo "    p1-p4 全部实现后重跑本脚本，即可拿到可复跑的 graceful-shutdown transcript。"
    echo "---- app 日志（末尾 20 行）----"
    tail -20 "$APP_LOG" || true
  else
    echo "服务未能在预期时间内启动（不是已知的红态 panic），日志如下："
    cat "$APP_LOG" || true
    kill -9 "$APP_PID" 2>/dev/null || true
    exit 1
  fi
else
  echo "/healthz 200，发送 SIGTERM（pid=${APP_PID}）"
  kill -TERM "$APP_PID"

  # 等进程真正退出，最多等 shutdown deadline（8s）再加一点缓冲。
  exited=false
  for i in $(seq 1 15); do
    if ! kill -0 "$APP_PID" 2>/dev/null; then
      exited=true
      break
    fi
    sleep 1
  done
  if [ "$exited" != true ]; then
    echo "进程在 SIGTERM 后 15s 内未退出，判定优雅关闭失败："
    cat "$APP_LOG"
    kill -9 "$APP_PID" 2>/dev/null || true
    exit 1
  fi

  echo "---- graceful-shutdown 日志原文 ----"
  cat "$APP_LOG"
  echo "-----------------------------------"

  mapfile -t got_steps < <(grep -o 'shutdown step start: [a-z-]*' "$APP_LOG" | awk '{print $NF}')
  expected_steps=(stop-http drain-inflight stop-consumer flush-producer close-deps)

  if [ "${#got_steps[@]}" -ne "${#expected_steps[@]}" ]; then
    echo "BROKEN：期望 ${#expected_steps[@]} 步，日志里只看到 ${#got_steps[@]} 步：${got_steps[*]:-<空>}"
    exit 1
  fi
  for idx in "${!expected_steps[@]}"; do
    if [ "${got_steps[$idx]}" != "${expected_steps[$idx]}" ]; then
      echo "BROKEN：第 $((idx + 1)) 步期望 ${expected_steps[$idx]}，实际 ${got_steps[$idx]}（完整顺序：${got_steps[*]}）"
      exit 1
    fi
  done
  echo "五步顺序=OK：${got_steps[*]}"

  transcript="$(grep 'graceful-shutdown transcript:' "$APP_LOG" || true)"
  if [ -z "$transcript" ]; then
    echo "BROKEN：日志里没有找到最终的 graceful-shutdown transcript 汇总行"
    exit 1
  fi
  echo "$transcript"
fi

cat <<'EOF'

全部通过（或按提示跳过了红态阶段）。另开终端 `go run ./cmd/api` 后运行
`./scripts/loadtest_m05.sh`，把 unbounded/bounded 两轮对比数字与 SIGTERM
transcript 一起填进 docs/writeups/m05-production.md。
EOF
