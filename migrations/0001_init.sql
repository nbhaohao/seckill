-- m01 p1: 裸下单链路需要的最小表结构。
-- orders.request_id 此时刻意不建索引/唯一约束——p3 会补一条 ALTER 加 UNIQUE KEY，
-- 用同一条查询在两次迁移前后的 EXPLAIN 输出做对比（type/key/rows 的变化）。
CREATE TABLE IF NOT EXISTS products (
  id         BIGINT UNSIGNED NOT NULL PRIMARY KEY,
  name       VARCHAR(64)     NOT NULL,
  stock      INT             NOT NULL,
  created_at DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS orders (
  id         BIGINT UNSIGNED NOT NULL PRIMARY KEY,  -- snowflake 生成（p2），非自增
  product_id BIGINT UNSIGNED NOT NULL,
  user_id    BIGINT UNSIGNED NOT NULL,
  request_id VARCHAR(64)     NOT NULL,
  quantity   INT             NOT NULL,
  status     VARCHAR(16)     NOT NULL DEFAULT 'created',
  created_at DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  KEY idx_product_id (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO products (id, name, stock) VALUES
  (1, '秒杀款 · 限量球鞋', 100),
  (2, '秒杀款 · 联名卫衣', 100)
ON DUPLICATE KEY UPDATE name = VALUES(name);
