package order

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// PlaceOrderNaive 是 m01 p1 的核心：最直觉的下单写法——查库存、判断够不够、扣库存、插订单，
// 四步分开、不加事务也不加锁。这是真实世界里"第一版能跑的代码"常见的样子：单请求测试完全正确，
// 一旦并发就会超卖。本关的目标就是亲手验证这句话，而不是提前把它写对（p3 才修）。
//
// ID 用 time.Now().UnixNano() 顶一下——p2 才引入真正的分布式 ID，这里先不依赖它。
func PlaceOrderNaive(ctx context.Context, db *sqlx.DB, req PlaceOrderRequest) (*Order, error) {
	if req.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}

	var stock int
	if err := db.GetContext(ctx, &stock, "SELECT stock FROM products WHERE id = ?", req.ProductID); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrProductNotFound
		}
		return nil, err
	}

	if stock < req.Quantity {
		return nil, ErrInsufficientStock
	}
	// 关键：判断和下一步的写，中间完全没有加锁，这条 if 通过之后到 UPDATE 真正执行之前，
	// 别的 goroutine 完全可能也读到同一个 stock、也判断"够"，这就是超卖窗口。

	panic("TODO: phase p1 S2 - 扣库存 + 插订单，用来复现超卖")
}

// AI 将在 p1 学习时分切片实现（下面是思路与顺序，不是终稿代码）：
// 4. _, err = db.ExecContext(ctx, "UPDATE products SET stock = stock - ? WHERE id = ?", req.Quantity, req.ProductID)
//    —— 注意这条 UPDATE 本身没有 WHERE stock >= quantity 兜底，纯粹翻译"扣库存"这个意图，
//    这正是本关要暴露的 bug，先别急着补它。
// 5. o := &Order{ ID: time.Now().UnixNano(), ProductID: req.ProductID, UserID: req.UserID,
//       RequestID: req.RequestID, Quantity: req.Quantity, Status: "created" }
//    _, err = db.ExecContext(ctx, "INSERT INTO orders (id, product_id, user_id, request_id, quantity, status) VALUES (?,?,?,?,?,?)",
//       o.ID, o.ProductID, o.UserID, o.RequestID, o.Quantity, o.Status)
// 6. 出错就返回 (nil, err)；全部成功返回 (o, nil)
