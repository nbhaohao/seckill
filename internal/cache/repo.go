// 已就位（AI 生成）：DB 回源那一侧的端口 + MySQL 实现 + 回源计数装饰器。
// 抽成接口的唯一目的是让"DB 被回源了几次"这件事可观察——m02 每个 phase 的证据都靠它：
// p1 证明二次读不打 DB、p2 证明空值缓存挡住了穿透、p3 证明 100 个并发只回源 1 次。
package cache

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"

	"github.com/jmoiron/sqlx"
)

// ProductRepo 是缓存层看到的"数据库"：读一行商品 + 改一行库存，仅此两件事。
type ProductRepo interface {
	LoadProduct(ctx context.Context, id int64) (*Product, error)
	UpdateStock(ctx context.Context, id int64, stock int) error
}

// SQLProductRepo 是 ProductRepo 的真实实现（sqlx + MySQL）。
type SQLProductRepo struct{ db *sqlx.DB }

func NewSQLProductRepo(db *sqlx.DB) *SQLProductRepo { return &SQLProductRepo{db: db} }

func (r *SQLProductRepo) LoadProduct(ctx context.Context, id int64) (*Product, error) {
	var p Product
	err := r.db.GetContext(ctx, &p, "SELECT id, name, stock FROM products WHERE id = ?", id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrProductNotFound
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (r *SQLProductRepo) UpdateStock(ctx context.Context, id int64, stock int) error {
	res, err := r.db.ExecContext(ctx, "UPDATE products SET stock = ? WHERE id = ?", stock, id)
	if err != nil {
		return err
	}
	// RowsAffected==0 有两种可能：行不存在，或者新值和旧值相同。这里用一次额外的存在性检查
	// 把两者分开，免得把"值没变"误报成"商品不存在"。
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		var exists int
		if err := r.db.GetContext(ctx, &exists, "SELECT COUNT(1) FROM products WHERE id = ?", id); err != nil {
			return err
		}
		if exists == 0 {
			return ErrProductNotFound
		}
	}
	return nil
}

// CountingRepo 包住任意 ProductRepo，数它被回源了多少次。测试与 /metrics 都用它取证。
type CountingRepo struct {
	inner ProductRepo
	loads atomic.Int64
}

func NewCountingRepo(inner ProductRepo) *CountingRepo { return &CountingRepo{inner: inner} }

func (c *CountingRepo) LoadProduct(ctx context.Context, id int64) (*Product, error) {
	c.loads.Add(1)
	return c.inner.LoadProduct(ctx, id)
}

func (c *CountingRepo) UpdateStock(ctx context.Context, id int64, stock int) error {
	return c.inner.UpdateStock(ctx, id, stock)
}

// Loads 返回累计回源次数；Reset 归零，方便一个测试里分段计数。
func (c *CountingRepo) Loads() int64 { return c.loads.Load() }
func (c *CountingRepo) Reset()       { c.loads.Store(0) }
