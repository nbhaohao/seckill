// 已就位（AI 生成）：p7 的四个 schema 连接池接线，不包含路由决策。
package shard

import (
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/nbhaohao/go-seckill/internal/dbconn"
)

func SchemaName(shard int) string {
	return fmt.Sprintf("seckill_order_%d", shard)
}

// OpenPools 沿用一期 DB 配置，只替换 DBName，连接同一个 MySQL 实例中的四个 schema。
func OpenPools(base dbconn.Config) ([ShardCount]*sqlx.DB, error) {
	var pools [ShardCount]*sqlx.DB
	for shard := 0; shard < ShardCount; shard++ {
		cfg := base
		cfg.DBName = SchemaName(shard)
		db, err := dbconn.Open(cfg)
		if err != nil {
			ClosePools(pools)
			return pools, fmt.Errorf("open order shard %d: %w", shard, err)
		}
		pools[shard] = db
	}
	return pools, nil
}

func ClosePools(pools [ShardCount]*sqlx.DB) {
	for _, db := range pools {
		if db != nil {
			_ = db.Close()
		}
	}
}
