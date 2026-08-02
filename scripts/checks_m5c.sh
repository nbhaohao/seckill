#!/usr/bin/env bash
# sk-m5c 证据台：注入泄漏 → 看恒等式破 → 跑对账 → 看恒等式恢复 → 再跑一次证明不多退。
#
# 用的是真进程与真 HTTP，不是单元测试：崩溃点由 /debug/reconcile/crash 武装，
# 「崩掉」的那几发请求库存已扣、消息没发、没人回滚，正是 p1 要复现的事故。
# 数字（泄漏条数、退款笔数、前后恒等式）全部打印出来，贴进 docs/writeups/m5x-consistency.md。
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${BASE:-http://127.0.0.1:8080}"
PRODUCT_ID="${PRODUCT_ID:-1}"
INITIAL_STOCK="${INITIAL_STOCK:-200}"
REQUESTS="${REQUESTS:-40}"
CRASHES="${CRASHES:-6}"
# 窗口不是拍脑袋的常数：docs/writeups/m04-mq.md 实测「202 到 DB 可见」端到端 85.787417ms，
# 压测下 Kafka lag 峰值 7、排空到 0 约 2s。窗口必须盖住排空时间，这里取 3s（≈1.5 倍）。
WINDOW="${WINDOW:-3s}"

log() { printf '\n\033[1;34m== %s ==\033[0m\n' "$1"; }

log "0/6 前置：docker compose up -d mysql redis kafka，另开一个终端跑 go run ./cmd/api"
curl -fsS "$BASE/healthz" >/dev/null || { echo "API 没起来：$BASE"; exit 1; }

log "1/6 预热库存到 $INITIAL_STOCK（单 key 版，m03 的 deduct.StockKey）"
curl -fsS -X POST "$BASE/debug/deduct/warm/$PRODUCT_ID?stock=$INITIAL_STOCK" | tee /dev/stderr
docker compose exec -T redis redis-cli --scan --pattern 'seckill:ledger:*' | xargs -r docker compose exec -T redis redis-cli DEL >/dev/null || true

log "2/6 武装崩溃开关：前 $CRASHES 发死在预扣与投递之间"
curl -fsS -X POST "$BASE/debug/reconcile/crash?n=$CRASHES" | tee /dev/stderr

log "3/6 打 $REQUESTS 发带台账的下单（/debug/orders/async-ledger）"
crashed=0
for i in $(seq 1 "$REQUESTS"); do
  code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/debug/orders/async-ledger" \
    -H 'content-type: application/json' \
    -d "{\"product_id\":$PRODUCT_ID,\"user_id\":7001,\"quantity\":1}")"
  [ "$code" = "500" ] && crashed=$((crashed + 1))
done
echo "HTTP 500（注入崩溃）发数=$crashed（期望 $CRASHES）"
echo "台账条数：$(docker compose exec -T redis redis-cli --scan --pattern 'seckill:ledger:*' | wc -l | tr -d ' ')"

log "4/6 对账前的恒等式（Leaked 应等于崩溃发数）"
curl -fsS "$BASE/debug/reconcile/identity/$PRODUCT_ID?initial=$INITIAL_STOCK"
echo

log "5/6 等窗口过去再跑对账（窗口=$WINDOW；不等就跑会把在途订单误判成泄漏）"
sleep 4
curl -fsS -X POST "$BASE/debug/reconcile/run?window=$WINDOW"
echo
curl -fsS "$BASE/debug/reconcile/identity/$PRODUCT_ID?initial=$INITIAL_STOCK"
echo

log "6/6 再跑一次对账：refunded 必须是 0，库存不许再动（幂等）"
curl -fsS -X POST "$BASE/debug/reconcile/run?window=$WINDOW"
echo
curl -fsS "$BASE/debug/reconcile/identity/$PRODUCT_ID?initial=$INITIAL_STOCK"
echo

cat <<'EOF'

读数方法：
- identity.Leaked = RedisDeducted - DBOrdered。第 4 步它应该等于注入的崩溃发数——
  这就是「m04 那句投递失败回滚预扣救不了进程死亡」的量化证据。
- 第 5 步 report.Refunded 应等于泄漏条数、report.Settled 是已落库因而只结案不归还的那些，
  跑完 identity.holds 必须是 true。
- 第 6 步 report.AlreadyReconciled 应等于台账总条数、Refunded=0：对账 job 会被反复调度，
  多退一次就是超卖。
- 想看误判：把 WINDOW 调到 1ms 重跑第 3–5 步（别 sleep），在途订单会被当成泄漏退掉，
  随后消息落库，同一笔库存被用了两次——这就是 p3 transferChallenge 的现场。
EOF
