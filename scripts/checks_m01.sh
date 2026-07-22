#!/usr/bin/env bash
# m01 一键验证：起 MySQL → 迁移前后 EXPLAIN 对比 → 单测+集成测试(-race) → 死锁(1213)+INNODB STATUS。
# 用法：./scripts/checks_m01.sh
set -euo pipefail
cd "$(dirname "$0")/.."

MYSQL_EXEC="docker compose exec -T mysql mysql -uroot -proot"
MYSQL_EXEC_DB="docker compose exec -T mysql mysql -uroot -proot seckill"

log() { printf '\n\033[1;34m== %s ==\033[0m\n' "$1"; }

log "1/7 docker compose up -d mysql"
docker compose up -d mysql

log "2/7 等 MySQL healthy"
for i in $(seq 1 40); do
  status="$(docker compose ps mysql --format json 2>/dev/null | grep -o '"Health":"[a-z]*"' || true)"
  if echo "$status" | grep -q healthy; then
    echo "mysql healthy"
    break
  fi
  sleep 2
  if [ "$i" -eq 40 ]; then
    echo "mysql 一直没 healthy，退出" >&2
    docker compose logs mysql | tail -50
    exit 1
  fi
done

log "3/7 重建 orders 并应用 0001（保证脚本可重复，得到无 request_id 索引的基线 schema）"
$MYSQL_EXEC_DB -e "DROP TABLE IF EXISTS orders;"
$MYSQL_EXEC_DB < migrations/0001_init.sql
$MYSQL_EXEC_DB -e "INSERT INTO orders (id, product_id, user_id, request_id, quantity, status) VALUES (1, 1, 1, 'probe-explain', 1, 'created');"

log "4/7 EXPLAIN before（无唯一索引，预期 type=ALL 全表扫）"
$MYSQL_EXEC_DB -e "EXPLAIN SELECT id FROM orders WHERE request_id='probe-explain'\G"

log "5/7 应用 migrations/0002_orders_request_id_unique.sql"
$MYSQL_EXEC_DB < migrations/0002_orders_request_id_unique.sql

log "6/7 EXPLAIN after（同一条已存在的探针记录，有唯一索引，预期 type=const/key=uk_request_id）"
$MYSQL_EXEC_DB -e "EXPLAIN SELECT id FROM orders WHERE request_id='probe-explain'\G"
$MYSQL_EXEC_DB -e "DELETE FROM orders WHERE id=1;"

log "7/7 go test（-race，覆盖 idgen/order/server 全部 phase）"
export TEST_DB_HOST=127.0.0.1 TEST_DB_PORT=3306 TEST_DB_USER=seckill TEST_DB_PASSWORD=seckill TEST_DB_NAME=seckill
go build ./...
go test -race ./... -v

log "死锁复现 + SHOW ENGINE INNODB STATUS 的 LATEST DETECTED DEADLOCK 段"
go test -race ./internal/order/... -run TestM01P3ReverseLockOrderCausesDeadlock1213 -v
$MYSQL_EXEC -e "SHOW ENGINE INNODB STATUS\G" | awk '/LATEST DETECTED DEADLOCK/{f=1} f{print; c++} c==45{exit}'

log "DB 连接数观察（macOS 宿主用 netstat，容器内 iproute2 用 ss；本机核实 ss 不存在，见 COURSE_SPEC 开工 checklist 第4条）"
echo "netstat -an -p tcp | grep '\\.3306 ' | grep -c ESTABLISHED  ——  当前建立的 MySQL 连接数"
netstat -an -p tcp 2>/dev/null | grep '\.3306 ' | grep -c ESTABLISHED || true

cat <<'EOF'

全部通过。接下来（不进本脚本，手动跑）：
  1. 起 API: go run ./cmd/api   （另开一个终端）
  2. keep-alive on/off 对比 + 池指标：./scripts/loadtest_m01.sh
  3. 把上面的 EXPLAIN / 死锁 / 压测数字填进 docs/writeups/m01-baseline.md
EOF
