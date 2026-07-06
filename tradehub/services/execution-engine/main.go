package main


import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/streadway/amqp"
)

// ─── Global
variables ────────────────────────────────────────────────────────

var (
	db          *sql.DB
	redisClient *redis.Client
	rabbitConn  *amqp.Connection
	rabbitCh    *amqp.Channel
	ctx         = context.Background()
	mu          sync.Mutex

	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tradehub_execution_requests_total",
			Help: "Total requests to Execution Engine",
		},
		[]string{"method", "endpoint", "status"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "tradehub_execution_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)
	tradesExecuted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tradehub_trades_executed_total",
			Help: "Total trades executed",
		},
		[]string{"side", "symbol"},
	)
	executionLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "tradehub_execution_latency_ms",
			Help:    "Trade execution latency in milliseconds",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		},
	)
	totalTradeVolume = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tradehub_trade_volume_total",
			Help: "Total trade volume in dollars",
		},
		[]string{"symbol"},
	)
)

func init() {
	prometheus.MustRegister(requestsTotal)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(tradesExecuted)
	prometheus.MustRegister(executionLatency)
	prometheus.MustRegister(totalTradeVolume)
}

// ─── Models ──────────────────────────────────────────────────────────────────

type Trade struct {
	ID         int       `json:"id"`
	TradeID    string    `json:"trade_id"`
	OrderID    string    `json:"order_id"`
	AccountID  string    `json:"account_id"`
	Symbol     string    `json:"symbol"`
	Side       string    `json:"side"`
	Quantity   float64   `json:"quantity"`
	Price      float64   `json:"price"`
	Commission float64   `json:"commission"`
	NetAmount  float64   `json:"net_amount"`
	ExecutedAt time.Time `json:"executed_at"`
}

type ExecuteOrderRequest struct {
	OrderID   string  `json:"order_id" binding:"required"`
	AccountID string  `json:"account_id" binding:"required"`
	Symbol    string  `json:"symbol" binding:"required"`
	Side      string  `json:"side" binding:"required"`
	OrderType string  `json:"order_type" binding:"required"`
	Quantity  float64 `json:"quantity" binding:"required"`
	Price     float64 `json:"price"`
}

type OrderFromQueue struct {
	OrderID     string  `json:"order_id"`
	AccountID   string  `json:"account_id"`
	Symbol      string  `json:"symbol"`
	Side        string  `json:"side"`
	OrderType   string  `json:"order_type"`
	Quantity    float64 `json:"quantity"`
	Price       float64 `json:"price"`
	TimeInForce string  `json:"time_in_force"`
}

// Simulated market prices
var marketPrices = map[string]float64{
	"AAPL": 178.50, "GOOGL": 141.80, "MSFT": 378.90, "AMZN": 178.25,
	"TSLA": 248.50, "META": 505.75, "NVDA": 875.30, "JPM": 196.40,
	"V": 279.15, "JNJ": 156.80, "WMT": 165.30, "PG": 160.25,
	"UNH": 527.40, "HD": 370.80, "MA": 458.60, "DIS": 112.45,
	"BAC": 34.60, "NFLX": 605.20, "ADBE": 570.15, "CRM": 275.80,
	"AMD": 175.40, "INTC": 43.25, "PYPL": 63.80, "COIN": 235.60,
	"SQ": 78.90, "ROKU": 68.45, "SNAP": 17.30, "UBER": 72.15,
	"LYFT": 16.80, "ABNB": 155.40,
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	log.Println("🚀 TradeHub Execution Engine starting...")

	// Database
	var err error
	dbURL := getEnv("DATABASE_URL", "postgres://fintech:fintech123@payflow-postgres:5432/tradehub?sslmode=disable")
	db, err = sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()
	if err = db.Ping(); err != nil {
		log.Println("⚠ Database not available:", err)
	} else {
		log.Println("✓ Database connected")
	}

	// Redis
	redisClient = redis.NewClient(&redis.Options{
		Addr:     getEnv("REDIS_URL", "payflow-redis:6379"),
		Password: "",
		DB:       3,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Println("⚠ Redis not available:", err)
	} else {
		log.Println("✓ Redis connected")
	}

	// RabbitMQ
	rabbitURL := getEnv("RABBITMQ_URL", "amqp://fintech:rabbit123@payflow-rabbitmq:5672/")
	rabbitConn, err = amqp.Dial(rabbitURL)
	if err != nil {
		log.Println("⚠ RabbitMQ not available:", err)
	} else {
		rabbitCh, err = rabbitConn.Channel()
		if err != nil {
			log.Println("⚠ RabbitMQ channel error:", err)
		} else {
			log.Println("✓ RabbitMQ connected")
			defer rabbitConn.Close()
			defer rabbitCh.Close()
			rabbitCh.QueueDeclare("execution_queue", true, false, false, false, nil)
			rabbitCh.QueueDeclare("trade_events", true, false, false, false, nil)
			rabbitCh.QueueDeclare("clearing_queue", true, false, false, false, nil)

			// Start consuming from execution queue
			go consumeExecutionQueue()
		}
	}

	// Initialize tables
	initializeTables()

	// Initialize market prices in Redis
	initMarketPrices()

	// Start simulated price feed
	go simulatePriceFeed()

	// Gin - with request logging
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.Use(prometheusMiddleware())

	// Health & metrics
	router.GET("/health", healthCheck)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	router.GET("/api/v1/ping",
func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong", "service": "execution-engine"})
	})

	// API routes
	v1 := router.Group("/api/v1")
	{
		v1.POST("/execute", executeOrder)
		v1.GET("/trades", listTrades)
		v1.GET("/trades/:id", getTrade)
		v1.GET("/trades/order/:order_id", getTradesByOrder)
		v1.GET("/trades/account/:account_id", getTradesByAccount)
		v1.GET("/trades/symbol/:symbol", getTradesBySymbol)
		v1.GET("/market-price/:symbol", getMarketPrice)
		v1.GET("/stats", getExecutionStats)
	}

	// Start server
	port := getEnv("PORT", "8204")
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go
func() {
		log.Printf("🚀 Execution Engine running on port %s\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down Execution Engine...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
	log.Println("Execution Engine exited")
}

// ─── Database Initialization ─────────────────────────────────────────────────

func initializeTables() {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS trades (
			id SERIAL PRIMARY KEY,
			trade_id
varCHAR(50) UNIQUE NOT NULL,
			order_id
varCHAR(50) NOT NULL,
			account_id
varCHAR(50) NOT NULL,
			symbol
varCHAR(20) NOT NULL,
			side
varCHAR(10) NOT NULL,
			quantity DECIMAL(18,6) NOT NULL,
			price DECIMAL(18,4) NOT NULL,
			commission DECIMAL(18,4) DEFAULT 0,
			net_amount DECIMAL(18,4) NOT NULL,
			executed_at TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_trades_trade_id ON trades(trade_id)`,
		`CREATE INDEX IF NOT EXISTS idx_trades_order_id ON trades(order_id)`,
		`CREATE INDEX IF NOT EXISTS idx_trades_account_id ON trades(account_id)`,
		`CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)`,
		`CREATE INDEX IF NOT EXISTS idx_trades_executed_at ON trades(executed_at)`,
	}

	if db != nil {
		for _, q := range queries {
			if _, err := db.Exec(q); err != nil {
				log.Println("⚠ Table creation error:", err)
			}
		}
		log.Println("✓ Trades table ready")
	}
}

func initMarketPrices() {
	for symbol, price := range marketPrices {
		redisClient.Set(ctx, "market:price:"+symbol, price, 0)
	}
	log.Println("✓ Market prices initialized in Redis")
}

// Simulate small price movements every second
func simulatePriceFeed() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		mu.Lock()
		for symbol, price := range marketPrices {
			// Random walk: ±0.5%
			change := price * (rand.Float64()*0.01 - 0.005)
			newPrice := math.Round((price+change)*100) / 100
			if newPrice > 0 {
				marketPrices[symbol] = newPrice
				redisClient.Set(ctx, "market:price:"+symbol, newPrice, 0)
			}
		}
		mu.Unlock()
	}
}

// ─── Queue Consumer ──────────────────────────────────────────────────────────

func consumeExecutionQueue() {
	msgs, err := rabbitCh.Consume("execution_queue", "", false, false, false, false, nil)
	if err != nil {
		log.Println("⚠ Failed to consume execution queue:", err)
		return
	}

	log.Println("✓ Listening on execution_queue...")

	for msg := range msgs {
		var order OrderFromQueue
		if err := json.Unmarshal(msg.Body, &order); err != nil {
			log.Println("⚠ Invalid message in execution queue:", err)
			msg.Ack(false)
			continue
		}

		log.Printf("📥 Received order for execution: %s %s %s x%.2f", order.OrderID, order.Side, order.Symbol, order.Quantity)
		trade, err := processExecution(order)
		if err != nil {
			log.Printf("⚠ Execution failed for order %s: %v", order.OrderID, err)
			msg.Nack(false, true) // Requeue
			continue
		}

		log.Printf("✅ Trade executed: %s @ $%.2f", trade.TradeID, trade.Price)
		msg.Ack(false)
	}
}

func processExecution(order OrderFromQueue) (*Trade, error) {
	start := time.Now()

	// Get current market price
	mu.Lock()
	currentPrice, exists := marketPrices[order.Symbol]
	mu.Unlock()

	if !exists {
		// Unknown symbol, set a default price
		currentPrice = 100.00 + rand.Float64()*100
		mu.Lock()
		marketPrices[order.Symbol] = currentPrice
		mu.Unlock()
		redisClient.Set(ctx, "market:price:"+order.Symbol, currentPrice, 0)
	}

	// Determine execution price based on order type
	executionPrice := currentPrice
	switch order.OrderType {
	case "market":
		// Add slippage simulation (±0.1%)
		slippage := currentPrice * (rand.Float64()*0.002 - 0.001)
		executionPrice = math.Round((currentPrice+slippage)*100) / 100
	case "limit":
		if order.Side == "buy" && order.Price < currentPrice {
			return nil, fmt.Errorf("limit buy price $%.2f below market $%.2f", order.Price, currentPrice)
		}
		if order.Side == "sell" && order.Price > currentPrice {
			return nil, fmt.Errorf("limit sell price $%.2f above market $%.2f", order.Price, currentPrice)
		}
		executionPrice = order.Price
	case "stop", "stop_limit":
		executionPrice = currentPrice
	}

	// Calculate commission (0.1% of trade value, min $1)
	grossAmount := executionPrice * order.Quantity
	commission := math.Max(grossAmount*0.001, 1.00)
	commission = math.Round(commission*100) / 100

	var netAmount float64
	if order.Side == "buy" {
		netAmount = -(grossAmount + commission) // Debit
	} else {
		netAmount = grossAmount - commission // Credit
	}
	netAmount = math.Round(netAmount*100) / 100

	tradeID := "TRD-" + uuid.New().String()[:8]

	var trade Trade
	err := db.QueryRow(`
		INSERT INTO trades (trade_id, order_id, account_id, symbol, side, quantity, price, commission, net_amount, executed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
		RETURNING id, trade_id, order_id, account_id, symbol, side, quantity, price, commission, net_amount, executed_at`,
		tradeID, order.OrderID, order.AccountID, order.Symbol, order.Side,
		order.Quantity, executionPrice, commission, netAmount,
	).Scan(&trade.ID, &trade.TradeID, &trade.OrderID, &trade.AccountID,
		&trade.Symbol, &trade.Side, &trade.Quantity, &trade.Price,
		&trade.Commission, &trade.NetAmount, &trade.ExecutedAt)

	if err != nil {
		return nil, fmt.Errorf("failed to insert trade: %v", err)
	}

	// Publish trade.executed event
	publishTradeEvent("trade.executed", trade)

	// Send to clearing queue
	sendToClearingQueue(trade)

	latency := float64(time.Since(start).Milliseconds())
	executionLatency.Observe(latency)
	tradesExecuted.WithLabelValues(order.Side, order.Symbol).Inc()
	totalTradeVolume.WithLabelValues(order.Symbol).Add(grossAmount)

	return &trade, nil
}

// ─── Handlers ────────────────────────────────────────────────────────────────

func executeOrder(c *gin.Context) {
	var req ExecuteOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	order := OrderFromQueue{
		OrderID:   req.OrderID,
		AccountID: req.AccountID,
		Symbol:    req.Symbol,
		Side:      req.Side,
		OrderType: req.OrderType,
		Quantity:  req.Quantity,
		Price:     req.Price,
	}

	trade, err := processExecution(order)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Execution failed", "details": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"status": "success", "data": trade})
}

func listTrades(c *gin.Context) {
	limit := c.DefaultQuery("limit", "50")
	offset := c.DefaultQuery("offset", "0")

	rows, err := db.Query(`SELECT id, trade_id, order_id, account_id, symbol, side, quantity, price, commission, net_amount, executed_at
		FROM trades ORDER BY executed_at DESC LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	trades := []Trade{}
	for rows.Next() {
		var t Trade
		rows.Scan(&t.ID, &t.TradeID, &t.OrderID, &t.AccountID, &t.Symbol,
			&t.Side, &t.Quantity, &t.Price, &t.Commission, &t.NetAmount, &t.ExecutedAt)
		trades = append(trades, t)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": trades, "count": len(trades)})
}

func getTrade(c *gin.Context) {
	tradeID := c.Param("id")

	var t Trade
	err := db.QueryRow(`SELECT id, trade_id, order_id, account_id, symbol, side, quantity, price, commission, net_amount, executed_at
		FROM trades WHERE trade_id = $1`, tradeID).Scan(
		&t.ID, &t.TradeID, &t.OrderID, &t.AccountID, &t.Symbol,
		&t.Side, &t.Quantity, &t.Price, &t.Commission, &t.NetAmount, &t.ExecutedAt)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Trade not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": t})
}

func getTradesByOrder(c *gin.Context) {
	orderID := c.Param("order_id")

	rows, err := db.Query(`SELECT id, trade_id, order_id, account_id, symbol, side, quantity, price, commission, net_amount, executed_at
		FROM trades WHERE order_id = $1 ORDER BY executed_at DESC`, orderID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	trades := []Trade{}
	for rows.Next() {
		var t Trade
		rows.Scan(&t.ID, &t.TradeID, &t.OrderID, &t.AccountID, &t.Symbol,
			&t.Side, &t.Quantity, &t.Price, &t.Commission, &t.NetAmount, &t.ExecutedAt)
		trades = append(trades, t)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": trades, "count": len(trades)})
}

func getTradesByAccount(c *gin.Context) {
	accountID := c.Param("account_id")
	limit := c.DefaultQuery("limit", "50")

	rows, err := db.Query(`SELECT id, trade_id, order_id, account_id, symbol, side, quantity, price, commission, net_amount, executed_at
		FROM trades WHERE account_id = $1 ORDER BY executed_at DESC LIMIT $2`, accountID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	trades := []Trade{}
	for rows.Next() {
		var t Trade
		rows.Scan(&t.ID, &t.TradeID, &t.OrderID, &t.AccountID, &t.Symbol,
			&t.Side, &t.Quantity, &t.Price, &t.Commission, &t.NetAmount, &t.ExecutedAt)
		trades = append(trades, t)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": trades, "count": len(trades)})
}

func getTradesBySymbol(c *gin.Context) {
	symbol := c.Param("symbol")
	limit := c.DefaultQuery("limit", "50")

	rows, err := db.Query(`SELECT id, trade_id, order_id, account_id, symbol, side, quantity, price, commission, net_amount, executed_at
		FROM trades WHERE symbol = $1 ORDER BY executed_at DESC LIMIT $2`, symbol, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	trades := []Trade{}
	for rows.Next() {
		var t Trade
		rows.Scan(&t.ID, &t.TradeID, &t.OrderID, &t.AccountID, &t.Symbol,
			&t.Side, &t.Quantity, &t.Price, &t.Commission, &t.NetAmount, &t.ExecutedAt)
		trades = append(trades, t)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": trades, "count": len(trades)})
}

func getMarketPrice(c *gin.Context) {
	symbol := c.Param("symbol")

	mu.Lock()
	price, exists := marketPrices[symbol]
	mu.Unlock()

	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Symbol not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"symbol":    symbol,
			"price":     price,
			"timestamp": time.Now().Format(time.RFC3339),
		},
	})
}

func getExecutionStats(c *gin.Context) {
	var totalTrades int
	var totalVolume, totalCommission float64

	db.QueryRow("SELECT COUNT(*), COALESCE(SUM(ABS(net_amount)),0), COALESCE(SUM(commission),0) FROM trades").
		Scan(&totalTrades, &totalVolume, &totalCommission)

	var todayTrades int
	var todayVolume float64
	db.QueryRow("SELECT COUNT(*), COALESCE(SUM(ABS(net_amount)),0) FROM trades WHERE executed_at >= CURRENT_DATE").
		Scan(&todayTrades, &todayVolume)

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"total_trades":     totalTrades,
			"total_volume":     math.Round(totalVolume*100) / 100,
			"total_commission": math.Round(totalCommission*100) / 100,
			"today_trades":     todayTrades,
			"today_volume":     math.Round(todayVolume*100) / 100,
			"active_symbols":   len(marketPrices),
		},
	})
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func publishTradeEvent(eventType string, data interface{}) {
	if rabbitCh == nil {
		return
	}
	body, _ := json.Marshal(gin.H{
		"event_type": eventType,
		"data":       data,
		"timestamp":  time.Now(),
		"service":    "execution-engine",
	})
	rabbitCh.Publish("", "trade_events", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
}

func sendToClearingQueue(trade Trade) {
	if rabbitCh == nil {
		return
	}
	body, _ := json.Marshal(trade)
	rabbitCh.Publish("", "clearing_queue", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
}

func healthCheck(c *gin.Context) {
	health := gin.H{
		"status":  "UP",
		"service": "tradehub-execution-engine",
		"port":    "8204",
		"time":    time.Now().Format(time.RFC3339),
	}
	if db != nil {
		if err := db.Ping(); err != nil {
			health["database"] = "DOWN"
		} else {
			health["database"] = "UP"
		}
	}
	if err := redisClient.Ping(ctx).Err(); err != nil {
		health["redis"] = "DOWN"
	} else {
		health["redis"] = "UP"
	}
	if rabbitCh != nil {
		health["rabbitmq"] = "UP"
	} else {
		health["rabbitmq"] = "DOWN"
	}
	health["active_symbols"] = len(marketPrices)
	c.JSON(http.StatusOK, health)
}

func prometheusMiddleware() gin.HandlerFunc {
	return
func(c *gin.Context) {
		start := time.Now()
		c.Next()
		duration := time.Since(start).Seconds()
		status := fmt.Sprintf("%d", c.Writer.Status())
		endpoint := c.FullPath()
		if endpoint == "" {
			endpoint = c.Request.URL.Path
		}
		requestsTotal.WithLabelValues(c.Request.Method, endpoint, status).Inc()
		requestDuration.WithLabelValues(c.Request.Method, endpoint).Observe(duration)
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
