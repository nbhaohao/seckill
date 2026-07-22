#!/usr/bin/env bash
# m01 p4/p5：keep-alive 开/关同 workload 对比 + DB 池指标前后对照。
# 前置：另开一个终端跑 `go run ./cmd/api`（默认监听 :8080）。
# vegeta 不预装，直接 `go run` 官方模块跑（版本锁定 v12.13.0，README 核过 -format=json/-keepalive/-rate 用法）。
set -euo pipefail
cd "$(dirname "$0")/.."

ADDR="${ADDR:-http://127.0.0.1:8080}"
RATE="${RATE:-100}"       # 每秒请求数，按机器调整——报告只看相对提升，不写死绝对值
DURATION="${DURATION:-10s}"
PRODUCT_ID="${PRODUCT_ID:-9500}"
N="${N:-3000}"            # 目标条数上限，够盖过 RATE*DURATION 即可

VEGETA="go run github.com/tsenart/vegeta/v12@v12.13.0"

log() { printf '\n\033[1;34m== %s ==\033[0m\n' "$1"; }

targets_file="$(mktemp)"
trap 'rm -f "$targets_file"' EXIT

run_attack() {
  local keepalive="$1" label="$2" request_prefix="$3"
  log "复位 ${label} 的商品与订单（两轮都从相同库存、完整写路径开始）"
  docker compose exec -T mysql mysql -uroot -proot seckill -e \
    "DELETE FROM orders WHERE product_id=${PRODUCT_ID}; INSERT INTO products (id,name,stock) VALUES (${PRODUCT_ID},'loadtest',1000000) ON DUPLICATE KEY UPDATE stock=1000000;"
  python3 - "$N" "$ADDR" "$PRODUCT_ID" "$request_prefix" > "$targets_file" <<'PY'
import base64, json, sys
n, addr, pid, prefix = int(sys.argv[1]), sys.argv[2], int(sys.argv[3]), sys.argv[4]
for i in range(1, n + 1):
    body = json.dumps({"request_id": f"loadtest-{prefix}-{i}", "product_id": pid, "user_id": 1, "quantity": 1}).encode()
    print(json.dumps({
        "method": "POST",
        "url": f"{addr}/orders",
        "header": {"Content-Type": ["application/json"]},
        "body": base64.b64encode(body).decode(),
    }))
PY
  log "vegeta attack keepalive=${keepalive} (${label})"
  echo "-- pool stats before --"
  curl -s "${ADDR}/metrics" | grep '^go_sql_' || true
  $VEGETA attack -format=json -targets="$targets_file" -rate="$RATE" -duration="$DURATION" -keepalive="$keepalive" \
    | $VEGETA report
  echo "-- pool stats after --"
  curl -s "${ADDR}/metrics" | grep '^go_sql_' || true
}

run_attack true  "keep-alive ON"  "on"
run_attack false "keep-alive OFF" "off"

cat <<'EOF'

把上面两组 vegeta report（Latencies / Success / Throughput）和 pool stats before/after
一起贴进 docs/writeups/m01-baseline.md ——报告只比较同机 on/off 的相对差异，不写死绝对 QPS。
EOF
