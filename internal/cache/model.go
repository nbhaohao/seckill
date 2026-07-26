// 已就位（AI 生成）：m02 缓存层共用的类型、key 拼法、JSON 编解码与哨兵错误。
// 这些都是四个 phase 共用的契约，本身不是教学点——教学点在 product.go 的四个方法怎么用它们。
package cache

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Product 是被缓存的商品视图：秒杀读路径只需要这三列，缓存里存的就是它的 JSON。
type Product struct {
	ID    int64  `db:"id" json:"id"`
	Name  string `db:"name" json:"name"`
	Stock int    `db:"stock" json:"stock"`
}

var (
	// ErrProductNotFound：商品不存在。p2 的空值缓存要缓存的就是"这个 id 不存在"这个事实，
	// 命中空值缓存时返回的也是这个错误——调用方分不出"刚查过库"还是"缓存里记着不存在"，这是设计意图。
	ErrProductNotFound = errors.New("cache: product not found")
)

// NotFoundPlaceholder 是 p2 空值缓存写进 Redis 的那个占位值。
// 用一个不可能与合法 JSON 商品混淆的字符串，读到它就等于读到"这个 id 确定不存在"。
const NotFoundPlaceholder = "__NULL__"

// ProductKey 拼 Redis key。带前缀是为了和后面模块（m03 预扣库存的 key）不打架。
func ProductKey(id int64) string {
	return fmt.Sprintf("seckill:product:%d", id)
}

// MarshalProduct / UnmarshalProduct：缓存里存 JSON 字符串。编解码是样板，不占教学预算。
func MarshalProduct(p *Product) (string, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("cache: marshal product: %w", err)
	}
	return string(b), nil
}

func UnmarshalProduct(raw string) (*Product, error) {
	var p Product
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return nil, fmt.Errorf("cache: unmarshal product: %w", err)
	}
	return &p, nil
}
