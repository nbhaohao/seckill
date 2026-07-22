-- m01 p3: 幂等改造——同一 request_id 只能落一行订单。
-- 这条 UNIQUE KEY 同时是「幂等」和「EXPLAIN 前后对比」两件事的落点：
-- 迁移前 `EXPLAIN SELECT id FROM orders WHERE request_id=?` 是 type=ALL/key=NULL 全表扫；
-- 迁移后同一条查询变成 type=const 或 ref、key=uk_request_id、rows≈1。
ALTER TABLE orders ADD UNIQUE KEY uk_request_id (request_id);
