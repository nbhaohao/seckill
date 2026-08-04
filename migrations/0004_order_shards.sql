-- 已就位（AI 生成）：sk-m06 p7 在同一个 MySQL 实例创建四个订单 schema；迁移需用有 CREATE/GRANT 权限的账号执行。
CREATE DATABASE IF NOT EXISTS seckill_order_0 CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE DATABASE IF NOT EXISTS seckill_order_1 CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE DATABASE IF NOT EXISTS seckill_order_2 CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE DATABASE IF NOT EXISTS seckill_order_3 CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS seckill_order_0.orders (
  id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
  product_id BIGINT UNSIGNED NOT NULL,
  user_id BIGINT UNSIGNED NOT NULL,
  request_id VARCHAR(64) NOT NULL,
  quantity INT NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'created',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_request_id (request_id),
  KEY idx_product_id (product_id),
  KEY idx_created_id (created_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS seckill_order_1.orders LIKE seckill_order_0.orders;
CREATE TABLE IF NOT EXISTS seckill_order_2.orders LIKE seckill_order_0.orders;
CREATE TABLE IF NOT EXISTS seckill_order_3.orders LIKE seckill_order_0.orders;

GRANT ALL PRIVILEGES ON seckill_order_0.* TO 'seckill'@'%';
GRANT ALL PRIVILEGES ON seckill_order_1.* TO 'seckill'@'%';
GRANT ALL PRIVILEGES ON seckill_order_2.* TO 'seckill'@'%';
GRANT ALL PRIVILEGES ON seckill_order_3.* TO 'seckill'@'%';
