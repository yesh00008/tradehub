package main


import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
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

	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tradehub_order_mgmt_requests_total",
			Help: "Total requests to Order Management Service",
		},
		[]string{"method", "endpoint", "status"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "tradehub_order_mgmt_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)
	ordersPlaced = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tradehub_orders_placed_total",
			Help: "Total orders placed",
		},
		[]string{"order_type", "side"},
	)
	ordersCancelled = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "tradehub_orders_cancelled_total",
			Help: "Total orders cancelled",
		},
	)
	activeOrders = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "tradehub_active_orders",
			Help: "Number of active orders",
		},
	)
)

func init() {
	prometheus.MustRegister(requestsTotal)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(ordersPlaced)
	prometheus.MustRegister(ordersCancelled)
	prometheus.MustRegister(activeOrders)
}

// ─── Models ──────────────────────────────────────────────────────────────────

type Order struct {
	ID             int       `json:"id"`
	OrderID        string    `json:"order_id"`
	AccountID      string    `json:"account_id"`
	Symbol         string    `json:"symbol"`
	Side           string    `json:"side"`
	OrderType      string    `json:"order_type"`
	Quantity       float64   `json:"quantity"`
	Price          float64   `json:"price"`
	FilledQuantity float64   `json:"filled_quantity"`
	FilledPrice    float64   `json:"filled_price"`
	Status         string    `json:"status"`
	TimeInForce    string    `json:"time_in_force"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type PlaceOrderRequest struct {
	AccountID   string  `json:"account_id" binding:"required"`
	Symbol      string  `json:"symbol" binding:"required"`
	Side        string  `json:"side" binding:"required"`       // buy, sell
	OrderType   string  `json:"order_type" binding:"required"` // market, limit, stop, stop_limit
	Quantity    float64 `json:"quantity" binding:"required"`
	Price       float64 `json:"price"`         // required for limit, stop_limit
	StopPrice   float64 `json:"stop_price"`    // required for stop, stop_limit
	TimeInForce string  `json:"time_in_force"` // day, gtc, ioc, fok
}

type ModifyOrderRequest struct {
	Quantity    float64 `json:"quantity"`
	Price       float64 `json:"price"`
	TimeInForce string  `json:"time_in_force"`
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	log.Println("🚀 TradeHub Order Management Service starting...")

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
			rabbitCh.QueueDeclare("order_events", true, false, false, false, nil)
			rabbitCh.QueueDeclare("execution_queue", true, false, false, false, nil)
		}
	}

	// Initialize tables
	initializeTables()

	// Gin - with request logging
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.Use(prometheusMiddleware())

	// Health & metrics
	router.GET("/health", healthCheck)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	router.GET("/api/v1/ping",
func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong", "service": "order-management-service"})
	})

	// API routes
	v1 := router.Group("/api/v1")
	{
		v1.POST("/orders", placeOrder)
		v1.GET("/orders", listOrders)
		v1.GET("/orders/:id", getOrder)
		v1.PUT("/orders/:id", modifyOrder)
		v1.DELETE("/orders/:id", cancelOrder)
		v1.GET("/orders/account/:account_id", getOrdersByAccount)
		v1.GET("/orders/status/:status", getOrdersByStatus)
		// Internal endpoints for execution engine
		v1.PUT("/orders/:id/fill", fillOrder)
		v1.PUT("/orders/:id/partial-fill", partialFillOrder)
	}

	// Start server
	port := getEnv("PORT", "8203")
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go
func() {
		log.Printf("🚀 Order Management Service running on port %s\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down Order Management Service...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
	log.Println("Order Management Service exited")
}

// ─── Database Initialization ─────────────────────────────────────────────────

func initializeTables() {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS orders (
			id SERIAL PRIMARY KEY,
			order_id
varCHAR(50) UNIQUE NOT NULL,
			account_id
varCHAR(50) NOT NULL,
			symbol
varCHAR(20) NOT NULL,
			side
varCHAR(10) NOT NULL,
			order_type
varCHAR(20) NOT NULL,
			quantity DECIMAL(18,6) NOT NULL,
			price DECIMAL(18,4) DEFAULT 0,
			filled_quantity DECIMAL(18,6) DEFAULT 0,
			filled_price DECIMAL(18,4) DEFAULT 0,
			status
varCHAR(20) DEFAULT 'pending',
			time_in_force
varCHAR(10) DEFAULT 'day',
			created_at TIMESTAMP DEFAULT NOW(),
			updated_at TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_orders_order_id ON orders(order_id)`,
		`CREATE INDEX IF NOT EXISTS idx_orders_account_id ON orders(account_id)`,
		`CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol)`,
		`CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)`,
		`CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at)`,
	}

	if db != nil {
		for _, q := range queries {
			if _, err := db.Exec(q); err != nil {
				log.Println("⚠ Table creation error:", err)
			}
		}
		log.Println("✓ Orders table ready")
	}
}

// ─── Handlers ────────────────────────────────────────────────────────────────

func placeOrder(c *gin.Context) {
	var req PlaceOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	// Validate side
	req.Side = strings.ToLower(req.Side)
	if req.Side != "buy" && req.Side != "sell" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Side must be 'buy' or 'sell'"})
		return
	}

	// Validate order type
	req.OrderType = strings.ToLower(req.OrderType)
	validTypes := map[string]bool{"market": true, "limit": true, "stop": true, "stop_limit": true}
	if !validTypes[req.OrderType] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Order type must be: market, limit, stop, stop_limit"})
		return
	}

	// Validate price for limit/stop orders
	if (req.OrderType == "limit" || req.OrderType == "stop_limit") && req.Price <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Price is required for limit and stop_limit orders"})
		return
	}

	// Validate quantity
	if req.Quantity <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Quantity must be positive"})
		return
	}

	// Validate symbol
	req.Symbol = strings.ToUpper(req.Symbol)
	if len(req.Symbol) == 0 || len(req.Symbol) > 10 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid symbol"})
		return
	}

	// Default time in force
	if req.TimeInForce == "" {
		req.TimeInForce = "day"
	}
	validTIF := map[string]bool{"day": true, "gtc": true, "ioc": true, "fok": true}
	if !validTIF[req.TimeInForce] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Time in force must be: day, gtc, ioc, fok"})
		return
	}

	orderID := "ORD-" + uuid.New().String()[:8]
	price := req.Price
	if req.OrderType == "market" {
		price = 0 // Market orders don't have a set price
	}

	var order Order
	err := db.QueryRow(`
		INSERT INTO orders (order_id, account_id, symbol, side, order_type, quantity, price, filled_quantity, filled_price, status, time_in_force)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 0, 0, 'pending', $8)
		RETURNING id, order_id, account_id, symbol, side, order_type, quantity, price, filled_quantity, filled_price, status, time_in_force, created_at, updated_at`,
		orderID, req.AccountID, req.Symbol, req.Side, req.OrderType, req.Quantity, price, req.TimeInForce,
	).Scan(&order.ID, &order.OrderID, &order.AccountID, &order.Symbol, &order.Side,
		&order.OrderType, &order.Quantity, &order.Price, &order.FilledQuantity,
		&order.FilledPrice, &order.Status, &order.TimeInForce,
		&order.CreatedAt, &order.UpdatedAt)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to place order", "details": err.Error()})
		return
	}

	// Publish order.placed event
	publishEvent("order.placed", order)

	// Send to execution queue
	sendToExecutionQueue(order)

	ordersPlaced.WithLabelValues(order.OrderType, order.Side).Inc()
	activeOrders.Inc()

	// Cache order
	cacheOrder(order)

	c.JSON(http.StatusCreated, gin.H{"status": "success", "data": order, "message": "Order placed successfully"})
}

func getOrder(c *gin.Context) {
	orderID := c.Param("id")

	// Try cache
	cached, err := redisClient.Get(ctx, "order:"+orderID).Result()
	if err == nil {
		var order Order
		if json.Unmarshal([]byte(cached), &order) == nil {
			c.JSON(http.StatusOK, gin.H{"status": "success", "data": order, "source": "cache"})
			return
		}
	}

	order, err := fetchOrder(orderID)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Order not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	cacheOrder(order)
	c.JSON(http.StatusOK, gin.H{"status": "success", "data": order})
}

func listOrders(c *gin.Context) {
	limit := c.DefaultQuery("limit", "50")
	offset := c.DefaultQuery("offset", "0")
	symbol := c.DefaultQuery("symbol", "")
	status := c.DefaultQuery("status", "")
	side := c.DefaultQuery("side", "")

	query := `SELECT id, order_id, account_id, symbol, side, order_type, quantity, price, filled_quantity, filled_price, status, time_in_force, created_at, updated_at FROM orders WHERE 1=1`
	args := []interface{}{}
	argIdx := 1

	if symbol != "" {
		query += fmt.Sprintf(" AND symbol = $%d", argIdx)
		args = append(args, strings.ToUpper(symbol))
		argIdx++
	}
	if status != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}
	if side != "" {
		query += fmt.Sprintf(" AND side = $%d", argIdx)
		args = append(args, strings.ToLower(side))
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
	args = append(args, limit, offset)

	rows, err := db.Query(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error", "details": err.Error()})
		return
	}
	defer rows.Close()

	orders := []Order{}
	for rows.Next() {
		var o Order
		rows.Scan(&o.ID, &o.OrderID, &o.AccountID, &o.Symbol, &o.Side,
			&o.OrderType, &o.Quantity, &o.Price, &o.FilledQuantity,
			&o.FilledPrice, &o.Status, &o.TimeInForce,
			&o.CreatedAt, &o.UpdatedAt)
		orders = append(orders, o)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": orders, "count": len(orders)})
}

func modifyOrder(c *gin.Context) {
	orderID := c.Param("id")
	var req ModifyOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	// Check if order can be modified
	var currentStatus, orderType string
	err := db.QueryRow("SELECT status, order_type FROM orders WHERE order_id = $1", orderID).Scan(&currentStatus, &orderType)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Order not found"})
		return
	}

	if currentStatus != "pending" && currentStatus != "open" {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Cannot modify order in '%s' status", currentStatus)})
		return
	}

	sets := []string{}
	args := []interface{}{}
	argIdx := 1

	if req.Quantity > 0 {
		sets = append(sets, fmt.Sprintf("quantity = $%d", argIdx))
		args = append(args, req.Quantity)
		argIdx++
	}
	if req.Price > 0 && (orderType == "limit" || orderType == "stop_limit") {
		sets = append(sets, fmt.Sprintf("price = $%d", argIdx))
		args = append(args, req.Price)
		argIdx++
	}
	if req.TimeInForce != "" {
		sets = append(sets, fmt.Sprintf("time_in_force = $%d", argIdx))
		args = append(args, req.TimeInForce)
		argIdx++
	}

	if len(sets) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No fields to modify"})
		return
	}

	sets = append(sets, fmt.Sprintf("updated_at = $%d", argIdx))
	args = append(args, time.Now())
	argIdx++

	args = append(args, orderID)
	query := fmt.Sprintf("UPDATE orders SET %s WHERE order_id = $%d RETURNING id, order_id, account_id, symbol, side, order_type, quantity, price, filled_quantity, filled_price, status, time_in_force, created_at, updated_at",
		strings.Join(sets, ", "), argIdx)

	var order Order
	err = db.QueryRow(query, args...).Scan(
		&order.ID, &order.OrderID, &order.AccountID, &order.Symbol, &order.Side,
		&order.OrderType, &order.Quantity, &order.Price, &order.FilledQuantity,
		&order.FilledPrice, &order.Status, &order.TimeInForce,
		&order.CreatedAt, &order.UpdatedAt)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to modify order"})
		return
	}

	publishEvent("order.modified", order)
	cacheOrder(order)

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": order, "message": "Order modified"})
}

func cancelOrder(c *gin.Context) {
	orderID := c.Param("id")

	var currentStatus string
	err := db.QueryRow("SELECT status FROM orders WHERE order_id = $1", orderID).Scan(&currentStatus)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Order not found"})
		return
	}

	if currentStatus == "filled" || currentStatus == "cancelled" || currentStatus == "expired" {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Cannot cancel order in '%s' status", currentStatus)})
		return
	}

	var order Order
	err = db.QueryRow(`UPDATE orders SET status = 'cancelled', updated_at = $1 WHERE order_id = $2
		RETURNING id, order_id, account_id, symbol, side, order_type, quantity, price, filled_quantity, filled_price, status, time_in_force, created_at, updated_at`,
		time.Now(), orderID).Scan(
		&order.ID, &order.OrderID, &order.AccountID, &order.Symbol, &order.Side,
		&order.OrderType, &order.Quantity, &order.Price, &order.FilledQuantity,
		&order.FilledPrice, &order.Status, &order.TimeInForce,
		&order.CreatedAt, &order.UpdatedAt)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to cancel order"})
		return
	}

	publishEvent("order.cancelled", order)
	ordersCancelled.Inc()
	activeOrders.Dec()
	redisClient.Del(ctx, "order:"+orderID)

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": order, "message": "Order cancelled"})
}

func getOrdersByAccount(c *gin.Context) {
	accountID := c.Param("account_id")
	status := c.DefaultQuery("status", "")

	query := `SELECT id, order_id, account_id, symbol, side, order_type, quantity, price, filled_quantity, filled_price, status, time_in_force, created_at, updated_at
		FROM orders WHERE account_id = $1`
	args := []interface{}{accountID}

	if status != "" {
		query += " AND status = $2"
		args = append(args, status)
	}
	query += " ORDER BY created_at DESC LIMIT 100"

	rows, err := db.Query(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	orders := []Order{}
	for rows.Next() {
		var o Order
		rows.Scan(&o.ID, &o.OrderID, &o.AccountID, &o.Symbol, &o.Side,
			&o.OrderType, &o.Quantity, &o.Price, &o.FilledQuantity,
			&o.FilledPrice, &o.Status, &o.TimeInForce,
			&o.CreatedAt, &o.UpdatedAt)
		orders = append(orders, o)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": orders, "count": len(orders)})
}

func getOrdersByStatus(c *gin.Context) {
	status := c.Param("status")

	rows, err := db.Query(`SELECT id, order_id, account_id, symbol, side, order_type, quantity, price, filled_quantity, filled_price, status, time_in_force, created_at, updated_at
		FROM orders WHERE status = $1 ORDER BY created_at DESC LIMIT 100`, status)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	orders := []Order{}
	for rows.Next() {
		var o Order
		rows.Scan(&o.ID, &o.OrderID, &o.AccountID, &o.Symbol, &o.Side,
			&o.OrderType, &o.Quantity, &o.Price, &o.FilledQuantity,
			&o.FilledPrice, &o.Status, &o.TimeInForce,
			&o.CreatedAt, &o.UpdatedAt)
		orders = append(orders, o)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": orders, "count": len(orders)})
}

// Internal endpoint for execution engine to fill an order
func fillOrder(c *gin.Context) {
	orderID := c.Param("id")
	var req struct {
		FilledPrice float64 `json:"filled_price" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	var order Order
	err := db.QueryRow(`UPDATE orders SET status = 'filled', filled_quantity = quantity, filled_price = $1, updated_at = $2
		WHERE order_id = $3 AND status IN ('pending', 'open', 'partial')
		RETURNING id, order_id, account_id, symbol, side, order_type, quantity, price, filled_quantity, filled_price, status, time_in_force, created_at, updated_at`,
		req.FilledPrice, time.Now(), orderID).Scan(
		&order.ID, &order.OrderID, &order.AccountID, &order.Symbol, &order.Side,
		&order.OrderType, &order.Quantity, &order.Price, &order.FilledQuantity,
		&order.FilledPrice, &order.Status, &order.TimeInForce,
		&order.CreatedAt, &order.UpdatedAt)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Order not found or cannot be filled"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fill order"})
		return
	}

	publishEvent("order.filled", order)
	activeOrders.Dec()
	cacheOrder(order)

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": order})
}

func partialFillOrder(c *gin.Context) {
	orderID := c.Param("id")
	var req struct {
		FilledQuantity float64 `json:"filled_quantity" binding:"required"`
		FilledPrice    float64 `json:"filled_price" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	var order Order
	err := db.QueryRow(`UPDATE orders SET status = 'partial', filled_quantity = filled_quantity + $1, filled_price = $2, updated_at = $3
		WHERE order_id = $4 AND status IN ('pending', 'open', 'partial')
		RETURNING id, order_id, account_id, symbol, side, order_type, quantity, price, filled_quantity, filled_price, status, time_in_force, created_at, updated_at`,
		req.FilledQuantity, req.FilledPrice, time.Now(), orderID).Scan(
		&order.ID, &order.OrderID, &order.AccountID, &order.Symbol, &order.Side,
		&order.OrderType, &order.Quantity, &order.Price, &order.FilledQuantity,
		&order.FilledPrice, &order.Status, &order.TimeInForce,
		&order.CreatedAt, &order.UpdatedAt)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to partial fill order"})
		return
	}

	// Check if fully filled
	if order.FilledQuantity >= order.Quantity {
		db.Exec("UPDATE orders SET status = 'filled', updated_at = $1 WHERE order_id = $2", time.Now(), orderID)
		order.Status = "filled"
		publishEvent("order.filled", order)
		activeOrders.Dec()
	} else {
		publishEvent("order.partial_fill", order)
	}

	cacheOrder(order)
	c.JSON(http.StatusOK, gin.H{"status": "success", "data": order})
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func fetchOrder(orderID string) (Order, error) {
	var o Order
	err := db.QueryRow(`SELECT id, order_id, account_id, symbol, side, order_type, quantity, price, filled_quantity, filled_price, status, time_in_force, created_at, updated_at
		FROM orders WHERE order_id = $1`, orderID).Scan(
		&o.ID, &o.OrderID, &o.AccountID, &o.Symbol, &o.Side,
		&o.OrderType, &o.Quantity, &o.Price, &o.FilledQuantity,
		&o.FilledPrice, &o.Status, &o.TimeInForce,
		&o.CreatedAt, &o.UpdatedAt)
	return o, err
}

func cacheOrder(order Order) {
	data, _ := json.Marshal(order)
	redisClient.Set(ctx, "order:"+order.OrderID, data, 5*time.Minute)
}

func publishEvent(eventType string, data interface{}) {
	if rabbitCh == nil {
		return
	}
	body, _ := json.Marshal(gin.H{
		"event_type": eventType,
		"data":       data,
		"timestamp":  time.Now(),
		"service":    "order-management-service",
	})
	rabbitCh.Publish("", "order_events", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
}

func sendToExecutionQueue(order Order) {
	if rabbitCh == nil {
		return
	}
	body, _ := json.Marshal(order)
	rabbitCh.Publish("", "execution_queue", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
}

func healthCheck(c *gin.Context) {
	health := gin.H{
		"status":  "UP",
		"service": "tradehub-order-management-service",
		"port":    "8203",
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
