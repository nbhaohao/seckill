-- m03 p1: 乐观锁（version CAS）需要一个版本号列。
-- 语义：每次成功扣减把 version 加一；UPDATE 的 WHERE 里带上读到的那个 version，
-- 于是"我读到之后有没有别人改过这行"这件事就变成了 RowsAffected 是 0 还是 1。
--
-- 写成 information_schema 守卫的动态 DDL 而不是裸 ALTER，是为了让 scripts/checks_m03.sh
-- 可以重复跑：MySQL 8.4 没有 ADD COLUMN IF NOT EXISTS，裸 ALTER 第二次就报 1060。
SET @has_version := (
  SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'products' AND COLUMN_NAME = 'version'
);
SET @ddl := IF(@has_version = 0,
  'ALTER TABLE products ADD COLUMN version BIGINT UNSIGNED NOT NULL DEFAULT 0',
  'DO 0');
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
