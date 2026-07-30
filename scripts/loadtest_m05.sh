#!/usr/bin/env bash
# m05 p5：同一 workload 下 unbounded（admission 容量放到极大，等价"无限等资源"）
# vs bounded（admission 容量明显小于并发，队列满快速失败）两轮对比，这是 p1 的核心证据。
# 脚本只采集/打印原始数字（vegeta Latencies/Status Codes、DB 写入、admission 结果、
# DB 连接池等待、Kafka lag 峰值与排空时间），不判断也不硬编码任何"应该多快"。
set -euo pipefail
cd "$(dirname "$0")/.."

ADDR="${ADDR:-http://127.0.0.1:8080}"
RATE="${RATE:-500}"
DURATION="${DURATION:-15s}"
PRODUCT_ID="${PRODUCT_ID:-10590}"
STOCK="${STOCK:-1000000}"
# 明显高于 DB 能稳定消化的速度才谈得上"真实过载"；两轮用同一个 rate/duration，
# 唯一变量是下面这两个 admission 容量。
UNBOUNDED_ADMISSION_CAPACITY="${UNBOUNDED_ADMISSION_CAPACITY:-100000}"
BOUNDED_ADMISSION_CAPACITY="${BOUNDED_ADMISSION_CAPACITY:-20}"
DRAIN_TIMEOUT_S="${DRAIN_TIMEOUT_S:-60}"
VEGETA="go run github.com/tsenart/vegeta/v12@v12.13.0"
MYSQL_EXEC_DB="docker compose exec -T mysql mysql -uroot -proot seckill"
REDIS_EXEC="docker compose exec -T redis redis-cli"
log() { printf '\n\033[1;34m== %s ==\033[0m\n' "$1"; }

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
ARTIFACT_DIR="artifacts/m05/${TIMESTAMP}"
mkdir -p "$ARTIFACT_DIR"

BODY="$(mktemp)"
printf '{"product_id":%s,"user_id":1,"quantity":1}' "$PRODUCT_ID" > "$BODY"
BODY_HASH="$(shasum -a 256 "$BODY" 2>/dev/null | awk '{print $1}')"
if [ -z "$BODY_HASH" ]; then BODY_HASH="$(sha256sum "$BODY" | awk '{print $1}')"; fi
BODY_ESCAPED="$(sed 's/"/\\"/g' "$BODY")"

APP_PID=""
BUILD_DIR=""
cleanup() {
  if [ -n "$APP_PID" ] && kill -0 "$APP_PID" 2>/dev/null; then
    kill -9 "$APP_PID" 2>/dev/null || true
  fi
  [ -n "$BUILD_DIR" ] && rm -rf "$BUILD_DIR"
  rm -f "$BODY"
}
trap cleanup EXIT

log "冻结实验条件 -> ${ARTIFACT_DIR}/workload.json"
cat > "${ARTIFACT_DIR}/workload.json" <<EOF
{
  "seed": "m05-overload-comparison-${TIMESTAMP}",
  "endpoint": "POST /debug/orders/async",
  "request_body": "${BODY_ESCAPED}",
  "request_body_sha256": "${BODY_HASH}",
  "rate_per_second": ${RATE},
  "duration": "${DURATION}",
  "product_id": ${PRODUCT_ID},
  "initial_stock": ${STOCK},
  "unbounded_admission_capacity": ${UNBOUNDED_ADMISSION_CAPACITY},
  "bounded_admission_capacity": ${BOUNDED_ADMISSION_CAPACITY},
  "rate_limiter": "capacity/refill 拉到 1000000，本轮对比只让 admission 容量当变量",
  "go_version": "$(go version)",
  "uname": "$(uname -a)",
  "git_commit": "$(git rev-parse HEAD 2>/dev/null || echo unknown)"
}
EOF
cat "${ARTIFACT_DIR}/workload.json"

reset_round() {
  $MYSQL_EXEC_DB -e "DELETE FROM orders WHERE product_id=${PRODUCT_ID}; INSERT INTO products (id,name,stock,version) VALUES (${PRODUCT_ID},'m05-load',${STOCK},0) ON DUPLICATE KEY UPDATE stock=${STOCK},version=0;" >/dev/null
  $REDIS_EXEC DEL "seckill:stock:${PRODUCT_ID}" >/dev/null
  curl -fsS -X POST "${ADDR}/debug/deduct/warm/${PRODUCT_ID}" >/dev/null
}

identity() {
  $MYSQL_EXEC_DB -N -e "SELECT CONCAT('DB剩余=',stock,' DB已扣=',${STOCK}-stock,' 订单=',(SELECT COUNT(*) FROM orders WHERE product_id=${PRODUCT_ID}),' 恒等式=',IF(${STOCK}-stock=(SELECT COUNT(*) FROM orders WHERE product_id=${PRODUCT_ID}),'OK','BROKEN')) FROM products WHERE id=${PRODUCT_ID};"
}

get_orders_count() {
  $MYSQL_EXEC_DB -N -e "SELECT COUNT(*) FROM orders WHERE product_id=${PRODUCT_ID};" 2>/dev/null || echo unknown
}

get_lag() {
  curl -fsS "${ADDR}/metrics" 2>/dev/null | awk '/^seckill_kafka_consumer_group_lag/{print $2; exit}'
}

# start_app 每轮起一个全新的进程——admission 容量是构造函数参数，运行时改不了，
# 只能通过重启换配置；这也顺便保证每轮的 InFlight/accepted/rejected 计数从零开始。
start_app() {
  local admission_capacity="$1" log_file="$2"
  ADMISSION_CAPACITY="$admission_capacity" \
    RATE_LIMIT_CAPACITY="1000000" \
    RATE_LIMIT_REFILL_PER_SECOND="1000000" \
    "$APP_BIN" > "$log_file" 2>&1 &
  APP_PID=$!
  for _ in $(seq 1 30); do
    if curl -fsS -o /dev/null "${ADDR}/healthz" 2>/dev/null; then return 0; fi
    if ! kill -0 "$APP_PID" 2>/dev/null; then
      echo "app 启动即退出，日志："
      cat "$log_file" || true
      return 1
    fi
    sleep 1
  done
  echo "app 30s 内未 healthy，日志："
  cat "$log_file" || true
  return 1
}

stop_app() {
  [ -z "$APP_PID" ] && return 0
  if kill -0 "$APP_PID" 2>/dev/null; then
    kill -TERM "$APP_PID"
    for _ in $(seq 1 20); do
      kill -0 "$APP_PID" 2>/dev/null || break
      sleep 1
    done
    kill -9 "$APP_PID" 2>/dev/null || true
  fi
  APP_PID=""
}

run_round() {
  local name="$1" admission_capacity="$2"
  local round_dir="${ARTIFACT_DIR}/${name}"
  mkdir -p "$round_dir"
  log "轮 ${name}：ADMISSION_CAPACITY=${admission_capacity}"

  if ! start_app "$admission_capacity" "${round_dir}/app.log"; then
    echo "⚠️  轮 ${name} 未能启动，跳过（大概率 internal/overload 的 p1-p4 还没实现完，构造函数仍在 panic）。"
    return 1
  fi
  reset_round

  local timeline="${round_dir}/timeline.txt"
  local vegeta_bin="${round_dir}/vegeta.bin"
  local lag_peak=0 started=$SECONDS lag orders

  echo "-- 攻击中：每秒采样 DB 累计订单 / Kafka lag --" | tee "$timeline"
  echo "POST ${ADDR}/debug/orders/async" | $VEGETA attack -rate="$RATE" -duration="$DURATION" -keepalive=true -header 'Content-Type: application/json' -body "$BODY" > "$vegeta_bin" &
  local attack_pid=$!
  while kill -0 "$attack_pid" 2>/dev/null; do
    orders="$(get_orders_count)"
    lag="$(get_lag)"
    if [[ "$lag" =~ ^[0-9]+$ ]] && [ "$lag" -gt "$lag_peak" ]; then lag_peak="$lag"; fi
    printf 't=%ss db_orders=%s lag=%s\n' "$((SECONDS - started))" "$orders" "${lag:-unknown}" | tee -a "$timeline"
    sleep 1
  done
  wait "$attack_pid" || true

  $VEGETA report < "$vegeta_bin" | tee "${round_dir}/vegeta_report.txt"
  $VEGETA report -type=json < "$vegeta_bin" > "${round_dir}/vegeta_report.json" || true

  echo "-- 攻击结束，等 backlog 排空（lag=0），超时 ${DRAIN_TIMEOUT_S}s --" | tee -a "$timeline"
  local drained=false drain_deadline=$((SECONDS + DRAIN_TIMEOUT_S))
  while [ "$SECONDS" -lt "$drain_deadline" ]; do
    orders="$(get_orders_count)"
    lag="$(get_lag)"
    if [[ "$lag" =~ ^[0-9]+$ ]] && [ "$lag" -gt "$lag_peak" ]; then lag_peak="$lag"; fi
    printf 'drain t=%ss db_orders=%s lag=%s\n' "$((SECONDS - started))" "$orders" "${lag:-unknown}" | tee -a "$timeline"
    if [ "${lag:-1}" = "0" ]; then drained=true; break; fi
    sleep 1
  done
  echo "lag 峰值=${lag_peak} 排空秒数=$((SECONDS - started)) 已排空=${drained}" | tee -a "$timeline"

  curl -fsS "${ADDR}/metrics" > "${round_dir}/metrics_final.txt" 2>/dev/null || true
  echo "-- admission / ratelimit / breaker / DB pool 关键指标 --"
  grep -E '^seckill_admission_outcomes_total|^seckill_ratelimit_outcomes_total|^seckill_breaker_state|^seckill_admission_inflight|^go_sql_wait_count_total|^go_sql_wait_duration_seconds_total' "${round_dir}/metrics_final.txt" | tee "${round_dir}/metrics_key.txt" || true

  echo "-- 恒等式（初始库存 - DB 剩余库存 ≡ 订单数） --"
  identity | tee "${round_dir}/identity.txt"

  stop_app
}

GOCACHE="${GOCACHE:-/tmp/go-seckill-build}" go run ./cmd/kafka-bootstrap

BUILD_DIR="$(mktemp -d)"
APP_BIN="${BUILD_DIR}/go-seckill-api"
GOCACHE="${GOCACHE:-/tmp/go-seckill-build}" go build -o "$APP_BIN" ./cmd/api

run_round unbounded "$UNBOUNDED_ADMISSION_CAPACITY"
run_round bounded "$BOUNDED_ADMISSION_CAPACITY"

cat <<EOF

产物目录：${ARTIFACT_DIR}（workload.json + 每轮 app.log/timeline.txt/vegeta_report.{txt,json}/metrics_final.txt/identity.txt）
把两轮的 Latencies、Status Codes、DB 写入速率、admission outcomes、DB 连接池 wait、
Kafka lag 峰值与排空时间横向摆进 docs/writeups/m05-production.md 的对比表——
本脚本不判断也不预设哪一轮"更好"，只负责采集同 workload 下的原始证据。
EOF
