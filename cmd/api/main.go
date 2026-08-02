// 已就位（AI 生成）：HTTP 胶水/环境变量解析——把各 internal 包拼起来，本身不是教学点。
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"

	stockbucket "github.com/nbhaohao/go-seckill/internal/bucket"
	"github.com/nbhaohao/go-seckill/internal/cache"
	"github.com/nbhaohao/go-seckill/internal/dbconn"
	"github.com/nbhaohao/go-seckill/internal/deduct"
	"github.com/nbhaohao/go-seckill/internal/idgen"
	"github.com/nbhaohao/go-seckill/internal/metrics"
	"github.com/nbhaohao/go-seckill/internal/mq"
	"github.com/nbhaohao/go-seckill/internal/order"
	"github.com/nbhaohao/go-seckill/internal/overload"
	"github.com/nbhaohao/go-seckill/internal/reconcile"
	"github.com/nbhaohao/go-seckill/internal/redisconn"
	"github.com/nbhaohao/go-seckill/internal/server"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// envFloatOr 只服务 m05 的 token bucket 补充速率——它是浮点数，envIntOr 解析不了，
// 单独加一个小helper 比让 envIntOr 承担双重职责更清楚。
func envFloatOr(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

type placeOrderBody struct {
	RequestID string `json:"request_id" binding:"required"`
	ProductID int64  `json:"product_id" binding:"required"`
	UserID    int64  `json:"user_id" binding:"required"`
	Quantity  int    `json:"quantity" binding:"required"`
}

// deductOrderBody 是 m03 压测端点的入参：只有 product_id 必填。
// request_id 留空表示"让服务端生成一个新的"，quantity 留空按 1 算——
// 压测器重放同一份 body 时才不会退化成幂等重放。
type deductOrderBody struct {
	RequestID string `json:"request_id"`
	ProductID int64  `json:"product_id" binding:"required"`
	UserID    int64  `json:"user_id"`
	Quantity  int    `json:"quantity"`
}

func main() {
	dbCfg := dbconn.Config{
		Host:     envOr("DB_HOST", "127.0.0.1"),
		Port:     envIntOr("DB_PORT", 3306),
		User:     envOr("DB_USER", "seckill"),
		Password: envOr("DB_PASSWORD", "seckill"),
		DBName:   envOr("DB_NAME", "seckill"),
	}
	db, err := dbconn.Open(dbCfg)
	if err != nil {
		log.Fatalf("connect mysql: %v", err)
	}

	node, err := idgen.NewNode(int64(envIntOr("NODE_ID", 1)))
	if err != nil {
		log.Fatalf("idgen.NewNode: %v", err)
	}

	if err := metrics.Register(prometheus.DefaultRegisterer, db.DB); err != nil {
		log.Fatalf("register metrics: %v", err)
	}

	// m02 缓存层接线：Redis 客户端 + 回源计数器 + ProductCache。
	// 连不上 Redis 就直接退出——m02 起，商品读路径依赖它（m01 的下单接口不受影响，但服务启动是同一个进程）。
	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	rdb, err := redisconn.Open(rctx, redisconn.Config{
		Addr:     envOr("REDIS_ADDR", "127.0.0.1:6379"),
		Password: envOr("REDIS_PASSWORD", ""),
		DB:       envIntOr("REDIS_DB", 0),
	})
	rcancel()
	if err != nil {
		log.Fatalf("connect redis: %v（先 docker compose up -d redis）", err)
	}
	// rdb/producer/consumer/db 的关闭全部挪进 m05 p4 的 overload.Shutdown 的
	// close-deps 步骤——不再各自 defer，避免 shutdown 顺序完成后再被 defer 重复关一次。

	// m04 Kafka 胶水（已就位，AI 生成）：核心机制都留在 internal/mq 的 phase TODO。
	// API 只负责把最终形态接起来：202 接单、manual commit、有限重试与 lag gauge。
	brokers := strings.Split(envOr("KAFKA_BROKERS", "127.0.0.1:9092"), ",")
	orderTopic := envOr("KAFKA_TOPIC", mq.OrderCreatedTopic)
	dltTopic := envOr("KAFKA_DLT_TOPIC", mq.OrderCreatedDLT)
	consumerGroup := envOr("KAFKA_GROUP", "seckill-order-writer")
	producer, err := mq.NewProducer(brokers...)
	if err != nil {
		log.Fatalf("new Kafka producer: %v", err)
	}
	consumer, err := mq.NewManualCommitConsumer(consumerGroup, orderTopic, brokers...)
	if err != nil {
		log.Fatalf("new Kafka consumer: %v", err)
	}
	if err := mq.RegisterLagGauge(prometheus.DefaultRegisterer, producer, consumerGroup); err != nil {
		log.Fatalf("register Kafka lag gauge: %v", err)
	}

	// consumerLoopCtx 是 m05 p4「stop-consumer」步骤的钩子：shutdown 时取消它，
	// 循环在处理完手头这一批 record（含 commit）之后、下一次 poll 之前退出，
	// consumerLoopDone 让 shutdown 步骤知道循环真的已经停了，而不是靠 Sleep 猜。
	consumerLoopCtx, stopConsumerLoop := context.WithCancel(context.Background())
	consumerLoopDone := make(chan struct{})
	go func() {
		defer close(consumerLoopDone)
		for {
			select {
			case <-consumerLoopCtx.Done():
				return
			default:
			}
			pollCtx, pollCancel := context.WithTimeout(consumerLoopCtx, 5*time.Second)
			records, err := mq.PollBatch(pollCtx, consumer)
			pollCancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				if errors.Is(err, context.Canceled) {
					continue // 顶部的 select 会在下一圈捕到 consumerLoopCtx.Done() 并返回。
				}
				log.Printf("poll order.created: %v", err)
				continue
			}
			for _, record := range records {
				processCtx, processCancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, outcome, err := mq.ProcessWithRetry(processCtx, producer, dltTopic, record, mq.RetryPolicy{MaxAttempts: 3, BaseBackoff: 100 * time.Millisecond}, func(ctx context.Context, record *kgo.Record) (*order.Order, error) {
					return mq.PlaceRecord(ctx, db, node, record)
				})
				processCancel()
				if err != nil {
					log.Printf("process order.created topic=%s partition=%d offset=%d: %v", record.Topic, record.Partition, record.Offset, err)
					continue
				}
				if outcome.DeadLettered {
					log.Printf("order.created moved to DLT partition=%d offset=%d attempts=%d", record.Partition, record.Offset, outcome.Attempts)
				}
				commitCtx, commitCancel := context.WithTimeout(context.Background(), 3*time.Second)
				err = mq.CommitProcessed(commitCtx, consumer, record)
				commitCancel()
				if err != nil {
					log.Printf("commit order.created partition=%d offset=%d: %v", record.Partition, record.Offset, err)
				}
			}
		}
	}()

	productRepo := cache.NewCountingRepo(cache.NewSQLProductRepo(db))
	productCache := cache.New(rdb, productRepo, cache.DefaultOptions())
	if err := metrics.RegisterCacheDBLoads(prometheus.DefaultRegisterer, productRepo.Loads); err != nil {
		log.Fatalf("register cache metrics: %v", err)
	}

	// ===== m05 过载治理层（p1 有界并发槽 + p2 令牌桶 + p3 熔断）=====
	// admission 与 bucket 是两道独立的门（COURSE_SPEC 拍板）：bucket 管"允许多快进"，
	// admission 管"系统同时敢答应做多少活"；两者都必须在 Redis 预扣/DB 写/Kafka produce
	// 之类的业务副作用之前判定，否则快速失败就救不回已经发生的副作用。
	admission := overload.NewAdmission(
		envIntOr("ADMISSION_CAPACITY", 64),
		time.Duration(envIntOr("ADMISSION_WAIT_BUDGET_MS", 50))*time.Millisecond,
	)
	bucket := overload.NewTokenBucket(
		envIntOr("RATE_LIMIT_CAPACITY", 200),
		envFloatOr("RATE_LIMIT_REFILL_PER_SECOND", 200),
		nil, // nil 时 TokenBucket 内部退回 time.Now，生产环境不用操心传时钟。
	)
	breaker := overload.NewBreaker(overload.BreakerPolicy{
		FailureThreshold: envIntOr("BREAKER_FAILURE_THRESHOLD", 5),
		OpenFor:          time.Duration(envIntOr("BREAKER_OPEN_FOR_MS", 2000)) * time.Millisecond,
		HalfOpenProbes:   envIntOr("BREAKER_HALFOPEN_PROBES", 1),
	})

	admissionOutcomes := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "seckill_admission_outcomes_total",
		Help: "m05 p1 有界并发槽的结果计数（outcome=accepted|rejected）。",
	}, []string{"outcome"})
	rateLimitOutcomes := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "seckill_ratelimit_outcomes_total",
		Help: "m05 p2 令牌桶限流的结果计数（outcome=accepted|rejected）。",
	}, []string{"outcome"})
	breakerStateGauge := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "seckill_breaker_state",
		Help: "m05 p3 熔断器当前状态（0=closed 1=open 2=half-open）。",
	}, func() float64 { return float64(breaker.State()) })
	admissionInflightGauge := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "seckill_admission_inflight",
		Help: "m05 p1 有界并发槽当前占用数（drain-inflight 关闭步骤等它归零）。",
	}, func() float64 { return float64(admission.Stats().InFlight) })
	for _, c := range []prometheus.Collector{admissionOutcomes, rateLimitOutcomes, breakerStateGauge, admissionInflightGauge} {
		if err := prometheus.DefaultRegisterer.Register(c); err != nil {
			log.Fatalf("register m05 overload metrics: %v", err)
		}
	}

	// overloadGate 只挂在写路径（/orders、/debug/orders/*）：先令牌桶限流，
	// 再有界并发槽，最后才进业务 handler；/healthz、/metrics、只读的 /products/:id
	// 不挂这两道门。Acquire 成功后立刻 defer Release，保证 handler 无论怎么退出
	// （成功/业务错误/ctx 取消）都会还槽，不会让 InFlight 只涨不跌。
	overloadGate := func(c *gin.Context) {
		if !bucket.Allow() {
			rateLimitOutcomes.WithLabelValues("rejected").Inc()
			status, payload := overload.WritePathFailure(overload.ErrRateLimited)
			c.Header("Retry-After", strconv.Itoa(payload.RetryAfter))
			c.AbortWithStatusJSON(status, payload)
			return
		}
		rateLimitOutcomes.WithLabelValues("accepted").Inc()

		if err := admission.Acquire(c.Request.Context()); err != nil {
			admissionOutcomes.WithLabelValues("rejected").Inc()
			status, payload := overload.WritePathFailure(err)
			c.AbortWithStatusJSON(status, payload)
			return
		}
		admissionOutcomes.WithLabelValues("accepted").Inc()
		defer admission.Release()
		c.Next()
	}

	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusOK) })
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// /debug/products/:id 是给 scripts/checks_m01.sh 做恒等式校验用的最小读接口，不是业务 API。
	r.GET("/debug/products/:id", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
			return
		}
		var stock int
		if err := db.GetContext(c.Request.Context(), &stock, "SELECT stock FROM products WHERE id = ?", id); err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"id": id, "stock": stock})
	})

	// ===== m02 商品读路径：/products/:id 走缓存，/debug/products/:id/nocache 直连 DB =====
	// 两个 handler 的 SQL 完全相同（都走 cache.SQLProductRepo.LoadProduct），
	// 唯一差别就是前面有没有那层缓存——这才能让 p5 的 on/off 对比只剩一个变量。
	r.GET("/products/:id", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
			return
		}
		start := time.Now()
		p, err := productCache.Get(c.Request.Context(), id)
		metrics.ProductReads.WithLabelValues("cached").Inc()
		metrics.ProductReadLatency.WithLabelValues("cached").Observe(time.Since(start).Seconds())
		switch {
		case err == nil:
			c.JSON(http.StatusOK, p)
		case errors.Is(err, cache.ErrProductNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	})

	r.GET("/debug/products/:id/nocache", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
			return
		}
		start := time.Now()
		p, err := cache.NewSQLProductRepo(db).LoadProduct(c.Request.Context(), id)
		metrics.ProductReads.WithLabelValues("nocache").Inc()
		metrics.ProductReadLatency.WithLabelValues("nocache").Observe(time.Since(start).Seconds())
		switch {
		case err == nil:
			c.JSON(http.StatusOK, p)
		case errors.Is(err, cache.ErrProductNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	})

	// /debug/cache/warm/:id 预热单个商品；/debug/products/:id/stock 改库存（先更库再删缓存）。
	// 两个都是给 m02 的观察脚本用的最小接口，不是业务 API。
	r.POST("/debug/cache/warm/:id", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
			return
		}
		if err := productCache.Warm(c.Request.Context(), []int64{id}); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"warmed": id})
	})

	r.POST("/debug/products/:id/stock", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
			return
		}
		stock, err := strconv.Atoi(c.Query("stock"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad stock"})
			return
		}
		if err := productCache.UpdateStock(c.Request.Context(), id, stock); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"id": id, "stock": stock})
	})

	// /debug/slow 用来演示 p4 的 context deadline：故意跑一条慢查询，客户端超时取消后
	// 连接应该最终归还连接池（DB.Stats().InUse 会掉回去），而不是被这条慢查询永久占住。
	r.GET("/debug/slow", func(c *gin.Context) {
		seconds := 0
		if v := c.Query("seconds"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				seconds = n
			}
		}
		var out int
		err := db.GetContext(c.Request.Context(), &out, "SELECT SLEEP(?)", seconds)
		if err != nil {
			c.JSON(http.StatusGatewayTimeout, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"slept": seconds})
	})

	r.POST("/orders", overloadGate, func(c *gin.Context) {
		var body placeOrderBody
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		start := time.Now()
		o, err := order.PlaceOrderTx(c.Request.Context(), db, node, order.PlaceOrderRequest{
			RequestID: body.RequestID,
			ProductID: body.ProductID,
			UserID:    body.UserID,
			Quantity:  body.Quantity,
		})
		switch {
		case err == nil:
			metrics.OrdersPlaced.WithLabelValues("success").Inc()
			metrics.OrderLatency.WithLabelValues("success").Observe(time.Since(start).Seconds())
			c.JSON(http.StatusOK, o)
		case errors.Is(err, order.ErrInsufficientStock):
			metrics.OrdersPlaced.WithLabelValues("insufficient_stock").Inc()
			metrics.OrderLatency.WithLabelValues("insufficient_stock").Observe(time.Since(start).Seconds())
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		case errors.Is(err, order.ErrInvalidQuantity), errors.Is(err, order.ErrProductNotFound):
			metrics.OrdersPlaced.WithLabelValues("error").Inc()
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		default:
			metrics.OrdersPlaced.WithLabelValues("error").Inc()
			metrics.OrderLatency.WithLabelValues("error").Observe(time.Since(start).Seconds())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	})

	// ===== m03 防超卖四方案：/debug/orders/:approach =====
	// 四条路径做的是同一件业务（下一单），唯一差别是"扣库存"那一步用什么机制护住，
	// p5 的对比表才只剩一个变量。approach=pessimistic 就是 m01 那条 FOR UPDATE 基线。
	// /debug/deduct/warm/:id 把 DB 库存灌进 Redis，是 lua 那条路径的前提。
	r.POST("/debug/deduct/warm/:id", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
			return
		}
		if err := deduct.WarmStock(c.Request.Context(), rdb, db, id); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"warmed": id})
	})

	// ===== sk-m5b 分段库存：/debug/bucket/warm/:id 与 approach=bucket =====
	// 单 key 版（approach=lua）一个字不动，分桶版是并排的第二条路径，
	// loadtest_m5b.sh 用同一份 workload 轮流打这两条，before/after 才只剩一个变量。
	// N 与 k 由查询参数覆盖，默认走 stockbucket.DefaultConfig()。
	bucketCfgFrom := func(c *gin.Context) stockbucket.Config {
		cfg := stockbucket.DefaultConfig()
		if n, err := strconv.Atoi(c.Query("n")); err == nil && n > 0 {
			cfg.BucketCount = n
		}
		if k, err := strconv.Atoi(c.Query("k")); err == nil && k >= 0 {
			cfg.MaxProbes = k
		}
		return cfg
	}
	r.POST("/debug/bucket/warm/:id", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
			return
		}
		cfg := bucketCfgFrom(c)
		planned, err := stockbucket.SpreadStock(c.Request.Context(), rdb, db, id, cfg)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"warmed": id, "buckets": planned, "n": cfg.BucketCount, "k": cfg.MaxProbes})
	})
	r.GET("/debug/bucket/remaining/:id", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
			return
		}
		total, perBucket, err := stockbucket.TotalRemaining(c.Request.Context(), rdb, id, bucketCfgFrom(c))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"total": total, "buckets": perBucket})
	})

	// ===== sk-m5c 对账补偿：注入泄漏的下单入口 + 恒等式核对 + 手动触发对账 =====
	// m05 冻结的 /debug/orders/async 一个字不动；带台账与崩溃开关的是并排的第二条路径，
	// checks_m5c.sh 先用它压出泄漏，再调 /debug/reconcile/run 把账拉平。
	leakSwitch := reconcile.NewCrashSwitch(0)
	r.POST("/debug/reconcile/crash", func(c *gin.Context) {
		n, err := strconv.ParseInt(c.DefaultQuery("n", "0"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad n"})
			return
		}
		leakSwitch = reconcile.NewCrashSwitch(n)
		c.JSON(http.StatusOK, gin.H{"armed": n})
	})
	r.POST("/debug/orders/async-ledger", overloadGate, func(c *gin.Context) {
		var body deductOrderBody
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		requestID := body.RequestID
		if requestID == "" {
			id, err := node.NextID()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			requestID = strconv.FormatInt(id, 10)
		}
		quantity := body.Quantity
		if quantity == 0 {
			quantity = 1
		}
		req := order.PlaceOrderRequest{RequestID: requestID, ProductID: body.ProductID, UserID: body.UserID, Quantity: quantity}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		err := reconcile.EnqueueWithLedger(ctx, rdb, req, time.Now(), func(ctx context.Context, r order.PlaceOrderRequest) error {
			value, err := json.Marshal(mq.OrderCreated{
				RequestID: r.RequestID, ProductID: r.ProductID, UserID: r.UserID,
				Quantity: r.Quantity, AcceptedAt: time.Now(),
			})
			if err != nil {
				return err
			}
			return producer.ProduceSync(ctx, &kgo.Record{
				Topic: orderTopic,
				Key:   []byte(strconv.FormatInt(r.ProductID, 10)),
				Value: value,
			}).FirstErr()
		}, leakSwitch)

		switch {
		case err == nil:
			c.JSON(http.StatusAccepted, gin.H{"request_id": requestID, "status": "accepted"})
		case errors.Is(err, reconcile.ErrSimulatedCrash):
			// 注入的「进程死在这里」：库存已扣、消息没发、没人回滚，这一发就是泄漏本身。
			c.JSON(http.StatusInternalServerError, gin.H{"request_id": requestID, "status": "crashed", "leaked": true})
		case errors.Is(err, order.ErrInsufficientStock):
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	})
	r.GET("/debug/reconcile/identity/:id", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
			return
		}
		initial, err := strconv.ParseInt(c.DefaultQuery("initial", "0"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad initial"})
			return
		}
		identity, err := reconcile.CheckIdentity(c.Request.Context(), rdb, db, id, initial)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"identity": identity, "holds": identity.Holds()})
	})
	r.POST("/debug/reconcile/run", func(c *gin.Context) {
		window, err := time.ParseDuration(c.DefaultQuery("window", "3s"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad window"})
			return
		}
		report, err := reconcile.ReconcileOnce(c.Request.Context(), rdb, db, window, time.Now())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "report": report})
			return
		}
		c.JSON(http.StatusOK, gin.H{"window": window.String(), "report": report})
	})

	r.POST("/debug/orders/:approach", overloadGate, func(c *gin.Context) {
		var body deductOrderBody
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		approach := c.Param("approach")
		// 压测时 vegeta 反复重放同一份 body，request_id 只能由服务端现生成——
		// 否则每一发都撞 orders.uk_request_id，测到的就成了 m01 的幂等快路径而不是扣减机制。
		requestID := body.RequestID
		if requestID == "" {
			id, err := node.NextID()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			requestID = strconv.FormatInt(id, 10)
		}
		quantity := body.Quantity
		if quantity == 0 {
			quantity = 1
		}
		req := order.PlaceOrderRequest{
			RequestID: requestID,
			ProductID: body.ProductID,
			UserID:    body.UserID,
			Quantity:  quantity,
		}

		ctx := c.Request.Context()
		start := time.Now()
		var o *order.Order
		var err error
		switch approach {
		case "pessimistic":
			o, err = order.PlaceOrderTx(ctx, db, node, req)
		case "cas":
			var attempts int
			o, attempts, err = deduct.PlaceOrderByVersionCAS(ctx, db, node, req)
			metrics.CASAttempts.Add(float64(attempts))
		case "conditional":
			o, err = deduct.PlaceOrderByConditionalUpdate(ctx, db, node, req)
		case "lock":
			o, err = deduct.PlaceOrderWithLock(ctx, rdb, db, node, req, deduct.DefaultLockOptions())
		case "lua":
			o, err = deduct.PlaceOrderWithPreDeduct(ctx, rdb, db, node, req)
		case "bucket":
			o, err = stockbucket.PlaceOrderWithBucketDeduct(ctx, rdb, db, node, bucketCfgFrom(c), req)
		default:
			c.JSON(http.StatusBadRequest, gin.H{"error": "unknown approach: " + approach})
			return
		}
		metrics.DeductLatency.WithLabelValues(approach).Observe(time.Since(start).Seconds())

		switch {
		case err == nil:
			metrics.DeductOutcomes.WithLabelValues(approach, "success").Inc()
			c.JSON(http.StatusOK, o)
		case errors.Is(err, order.ErrInsufficientStock):
			metrics.DeductOutcomes.WithLabelValues(approach, "insufficient").Inc()
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		case errors.Is(err, deduct.ErrCASRetriesExhausted), errors.Is(err, deduct.ErrLockNotAcquired):
			// 这两个不是"卖光了"，是"竞争太激烈，这一发没轮上"——单独一类，
			// 否则对比表里会把方案的退化伪装成正常的售罄。
			metrics.DeductOutcomes.WithLabelValues(approach, "conflict").Inc()
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		default:
			metrics.DeductOutcomes.WithLabelValues(approach, "error").Inc()
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	})

	// m04 异步接单：Redis 预扣 + Kafka broker ack 后立刻 202，DB 落单由上面的 consumer 完成。
	// m05 p3：EnqueueOrder 套 breaker.Do——它包的是"Kafka 这个远程依赖调用"，
	// 不包业务结果，所以 ErrInsufficientStock 在 fn 内部被拦下来单独记，不喂给
	// breaker 的失败计数（拍板：卖光了不是依赖故障）。Breaker Open 时必须走
	// WritePathFailure 返回失败状态码，绝不能把"没敢发 Kafka"伪造成 202。
	r.POST("/debug/orders/async", overloadGate, func(c *gin.Context) {
		var body deductOrderBody
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		requestID := body.RequestID
		if requestID == "" {
			id, err := node.NextID()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			requestID = strconv.FormatInt(id, 10)
		}
		quantity := body.Quantity
		if quantity == 0 {
			quantity = 1
		}
		produceCtx, produceCancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer produceCancel()

		var accepted *mq.AcceptedOrder
		var bizErr error
		breakerErr := breaker.Do(produceCtx, func(ctx context.Context) error {
			a, err := mq.EnqueueOrder(ctx, rdb, producer, orderTopic, order.PlaceOrderRequest{RequestID: requestID, ProductID: body.ProductID, UserID: body.UserID, Quantity: quantity})
			if errors.Is(err, order.ErrInsufficientStock) {
				bizErr = err
				return nil // 业务结果，不算 Kafka 调用失败，不喂给熔断器计数。
			}
			accepted = a
			return err
		})

		switch {
		case errors.Is(breakerErr, overload.ErrBreakerOpen):
			status, payload := overload.WritePathFailure(overload.ErrBreakerOpen)
			c.JSON(status, payload)
		case bizErr != nil:
			c.JSON(http.StatusConflict, gin.H{"error": bizErr.Error()})
		case breakerErr != nil:
			status, payload := overload.WritePathFailure(breakerErr)
			c.JSON(status, payload)
		default:
			c.JSON(http.StatusAccepted, gin.H{"request_id": accepted.RequestID, "status": "accepted"})
		}
	})

	// 202 之后的薄查询端点：没有完整订单状态机，DB 尚不可见时只返回 pending。
	r.GET("/orders/requests/:requestID", func(c *gin.Context) {
		var found order.Order
		err := db.GetContext(c.Request.Context(), &found, "SELECT id, product_id, user_id, request_id, quantity, status, created_at FROM orders WHERE request_id = ?", c.Param("requestID"))
		switch {
		case err == nil:
			c.JSON(http.StatusOK, found)
		case errors.Is(err, sql.ErrNoRows):
			c.JSON(http.StatusAccepted, gin.H{"request_id": c.Param("requestID"), "status": "pending"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	})

	cfg := server.DefaultServerConfig()
	srv := server.NewProductionServer(envOr("HTTP_ADDR", ":8080"), r, db.DB, cfg)

	go func() {
		log.Printf("go-seckill listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	// ===== m05 p4：SIGTERM/SIGINT 触发优雅关闭 =====
	// signal.NotifyContext 拿到信号就取消 ctx，主 goroutine 从这里往下走，
	// 与处理请求的 goroutine 完全分开——这样 ListenAndServe 的阻塞不会挡住关闭流程。
	sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	<-sigCtx.Done()
	stopSignals()
	log.Printf("shutdown signal received, starting graceful shutdown")

	shutdownTimeout := time.Duration(envIntOr("SHUTDOWN_TIMEOUT_MS", 15000)) * time.Millisecond
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	// loggedStep 把每一步的开始/结束打成日志——这是 scripts/checks_m05.sh 用来
	// 断言"五步顺序、一步不落"的唯一证据来源，比读 ShutdownReport 更早、更细。
	loggedStep := func(name string, fn func(context.Context) error) overload.ShutdownStep {
		return overload.ShutdownStep{
			Name: name,
			Fn: func(ctx context.Context) error {
				log.Printf("shutdown step start: %s", name)
				err := fn(ctx)
				log.Printf("shutdown step done: %s err=%v", name, err)
				return err
			},
		}
	}

	report := overload.Shutdown(shutdownCtx, []overload.ShutdownStep{
		// 1. stop-http：不再接新请求，等在途 HTTP 请求处理完（或超时）。
		loggedStep("stop-http", func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		}),
		// 2. drain-inflight：等 p1 有界并发槽的 InFlight 归零，受同一个 deadline 约束。
		loggedStep("drain-inflight", func(ctx context.Context) error {
			for {
				if admission.Stats().InFlight == 0 {
					return nil
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(50 * time.Millisecond):
				}
			}
		}),
		// 3. stop-consumer：取消 consumer 循环的 ctx，等它处理完手头这一批
		//    （含 commit）后自己退出，不再拉新消息。
		loggedStep("stop-consumer", func(ctx context.Context) error {
			stopConsumerLoop()
			select {
			case <-consumerLoopDone:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
		// 4. flush-producer：把 producer 缓冲里还没发出去的记录发完。
		loggedStep("flush-producer", func(ctx context.Context) error {
			return producer.Flush(ctx)
		}),
		// 5. close-deps：consumer/producer/Redis/DB 全部关闭，err 用 errors.Join
		//    汇总而不是只报第一个，方便一次看全。
		loggedStep("close-deps", func(ctx context.Context) error {
			consumer.Close()
			producer.Close()
			var errs []error
			if err := rdb.Close(); err != nil {
				errs = append(errs, err)
			}
			if err := db.Close(); err != nil {
				errs = append(errs, err)
			}
			return errors.Join(errs...)
		}),
	})

	log.Printf("graceful-shutdown transcript: completed=%v failed=%q err=%v elapsed=%s",
		report.Completed, report.Failed, report.Err, report.Elapsed)
}
