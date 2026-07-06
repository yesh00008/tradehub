package main


import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
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
			Name: "tradehub_clearing_requests_total",
			Help: "Total requests to Clearing Service",
		},
		[]string{"method", "endpoint", "status"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "tradehub_clearing_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)
	settlementsCreated = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "tradehub_settlements_created_total",
			Help: "Total settlements created",
		},
	)
	settlementsCompleted = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "tradehub_settlements_completed_total",
			Help: "Total settlements completed",
		},
	)
	settlementAmount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "tradehub_settlement_amount_total",
			Help: "Total settlement amount",
		},
	)
)

func init() {
	prometheus.MustRegister(requestsTotal)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(settlementsCreated)
	prometheus.MustRegister(settlementsCompleted)
	prometheus.MustRegister(settlementAmount)
}

// ─── Models ──────────────────────────────────────────────────────────────────

type Settlement struct {
	ID             int        `json:"id"`
	SettlementID   string     `json:"settlement_id"`
	TradeID        string     `json:"trade_id"`
	AccountID      string     `json:"account_id"`
	SettlementDate time.Time  `json:"settlement_date"`
	Amount         float64    `json:"amount"`
	Status         string     `json:"status"`
	SettledAt      *time.Time `json:"settled_at"`
}

type TradeFromQueue struct {
	TradeID    string  `json:"trade_id"`
	OrderID    string  `json:"order_id"`
	AccountID  string  `json:"account_id"`
	Symbol     string  `json:"symbol"`
	Side       string  `json:"side"`
	Quantity   float64 `json:"quantity"`
	Price      float64 `json:"price"`
	Commission float64 `json:"commission"`
	NetAmount  float64 `json:"net_amount"`
}

type ReconciliationReport struct {
	Date                 string  `json:"date"`
	TotalTrades          int     `json:"total_trades"`
	TotalSettlements     int     `json:"total_settlements"`
	PendingSettlements   int     `json:"pending_settlements"`
	CompletedSettlements int     `json:"completed_settlements"`
	FailedSettlements    int     `json:"failed_settlements"`
	TotalAmount          float64 `json:"total_amount"`
	SettledAmount        float64 `json:"settled_amount"`
	PendingAmount        float64 `json:"pending_amount"`
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	log.Println("🚀 TradeHub Clearing Service starting...")

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
			rabbitCh.QueueDeclare("clearing_queue", true, false, false, false, nil)
			rabbitCh.QueueDeclare("settlement_events", true, false, false, false, nil)

			// Start consuming from clearing queue
			go consumeClearingQueue()
		}
	}

	// Initialize tables
	initializeTables()

	// Start settlement processor (runs every minute)
	go settlementProcessor()

	// Gin - with request logging
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.Use(prometheusMiddleware())

	// Health & metrics
	router.GET("/health", healthCheck)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	router.GET("/api/v1/ping",
func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong", "service": "clearing-service"})
	})

	// API routes
	v1 := router.Group("/api/v1")
	{
		v1.GET("/settlements", listSettlements)
		v1.GET("/settlements/:id", getSettlement)
		v1.GET("/settlements/account/:account_id", getSettlementsByAccount)
		v1.GET("/settlements/trade/:trade_id", getSettlementByTrade)
		v1.POST("/settlements/:id/settle", settleManually)
		v1.POST("/settlements/:id/fail", failSettlement)
		v1.GET("/reconciliation", getReconciliation)
		v1.GET("/reconciliation/daily", getDailyReconciliation)
		v1.POST("/settlements/process-pending", processPendingSettlements)
	}

	// Start server
	port := getEnv("PORT", "8205")
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go
func() {
		log.Printf("🚀 Clearing Service running on port %s\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down Clearing Service...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
	log.Println("Clearing Service exited")
}

// ─── Database Initialization ─────────────────────────────────────────────────

func initializeTables() {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS settlements (
			id SERIAL PRIMARY KEY,
			settlement_id
varCHAR(50) UNIQUE NOT NULL,
			trade_id
varCHAR(50) NOT NULL,
			account_id
varCHAR(50) NOT NULL,
			settlement_date TIMESTAMP NOT NULL,
			amount DECIMAL(18,4) NOT NULL,
			status
varCHAR(20) DEFAULT 'pending',
			settled_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_settlements_settlement_id ON settlements(settlement_id)`,
		`CREATE INDEX IF NOT EXISTS idx_settlements_trade_id ON settlements(trade_id)`,
		`CREATE INDEX IF NOT EXISTS idx_settlements_account_id ON settlements(account_id)`,
		`CREATE INDEX IF NOT EXISTS idx_settlements_status ON settlements(status)`,
		`CREATE INDEX IF NOT EXISTS idx_settlements_settlement_date ON settlements(settlement_date)`,
	}

	if db != nil {
		for _, q := range queries {
			if _, err := db.Exec(q); err != nil {
				log.Println("⚠ Table creation error:", err)
			}
		}
		log.Println("✓ Settlements table ready")
	}
}

// ─── Queue Consumer ──────────────────────────────────────────────────────────

func consumeClearingQueue() {
	msgs, err := rabbitCh.Consume("clearing_queue", "", false, false, false, false, nil)
	if err != nil {
		log.Println("⚠ Failed to consume clearing queue:", err)
		return
	}

	log.Println("✓ Listening on clearing_queue...")

	for msg := range msgs {
		var trade TradeFromQueue
		if err := json.Unmarshal(msg.Body, &trade); err != nil {
			log.Println("⚠ Invalid message in clearing queue:", err)
			msg.Ack(false)
			continue
		}

		log.Printf("📥 Received trade for clearing: %s", trade.TradeID)
		err := createSettlement(trade)
		if err != nil {
			log.Printf("⚠ Settlement creation failed for trade %s: %v", trade.TradeID, err)
			msg.Nack(false, true)
			continue
		}

		msg.Ack(false)
	}
}

func createSettlement(trade TradeFromQueue) error {
	settlementID := "STL-" + uuid.New().String()[:8]
	// T+2 settlement date (skip weekends)
	settlementDate := calculateSettlementDate(time.Now(), 2)

	amount := math.Abs(trade.NetAmount)

	_, err := db.Exec(`INSERT INTO settlements (settlement_id, trade_id, account_id, settlement_date, amount, status)
		VALUES ($1, $2, $3, $4, $5, 'pending')`,
		settlementID, trade.TradeID, trade.AccountID, settlementDate, amount)
	if err != nil {
		return fmt.Errorf("failed to insert settlement: %v", err)
	}

	settlementsCreated.Inc()
	settlementAmount.Add(amount)

	publishEvent("settlement.created", gin.H{
		"settlement_id":   settlementID,
		"trade_id":        trade.TradeID,
		"account_id":      trade.AccountID,
		"settlement_date": settlementDate,
		"amount":          amount,
	})

	log.Printf("✅ Settlement created: %s for trade %s, settling on %s", settlementID, trade.TradeID, settlementDate.Format("2006-01-02"))
	return nil
}

func calculateSettlementDate(from time.Time, businessDays int) time.Time {
	date := from
	for businessDays > 0 {
		date = date.Add(24 * time.Hour)
		if date.Weekday() != time.Saturday && date.Weekday() != time.Sunday {
			businessDays--
		}
	}
	return date
}

// ─── Settlement Processor (background) ───────────────────────────────────────

func settlementProcessor() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		processSettlements()
	}
}

func processSettlements() {
	// Find settlements that are due (settlement_date <= NOW)
	rows, err := db.Query(`SELECT id, settlement_id, trade_id, account_id, settlement_date, amount
		FROM settlements WHERE status = 'pending' AND settlement_date <= NOW()
		ORDER BY settlement_date ASC LIMIT 100`)
	if err != nil {
		log.Println("⚠ Failed to query pending settlements:", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var s Settlement
		if err := rows.Scan(&s.ID, &s.SettlementID, &s.TradeID, &s.AccountID, &s.SettlementDate, &s.Amount); err != nil {
			continue
		}

		// Execute settlement
		now := time.Now()
		_, err := db.Exec("UPDATE settlements SET status = 'settled', settled_at = $1 WHERE settlement_id = $2", now, s.SettlementID)
		if err != nil {
			log.Printf("⚠ Failed to settle %s: %v", s.SettlementID, err)
			db.Exec("UPDATE settlements SET status = 'failed' WHERE settlement_id = $1", s.SettlementID)
			continue
		}

		settlementsCompleted.Inc()
		publishEvent("settlement.completed", gin.H{
			"settlement_id": s.SettlementID,
			"trade_id":      s.TradeID,
			"account_id":    s.AccountID,
			"amount":        s.Amount,
			"settled_at":    now,
		})

		count++
	}

	if count > 0 {
		log.Printf("✅ Processed %d settlements", count)
	}
}

// ─── Handlers ────────────────────────────────────────────────────────────────

func listSettlements(c *gin.Context) {
	status := c.DefaultQuery("status", "")
	limit := c.DefaultQuery("limit", "50")
	offset := c.DefaultQuery("offset", "0")

	query := `SELECT id, settlement_id, trade_id, account_id, settlement_date, amount, status, settled_at
		FROM settlements WHERE 1=1`
	args := []interface{}{}
	argIdx := 1

	if status != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY settlement_date DESC LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
	args = append(args, limit, offset)

	rows, err := db.Query(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	settlements := []Settlement{}
	for rows.Next() {
		var s Settlement
		rows.Scan(&s.ID, &s.SettlementID, &s.TradeID, &s.AccountID, &s.SettlementDate, &s.Amount, &s.Status, &s.SettledAt)
		settlements = append(settlements, s)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": settlements, "count": len(settlements)})
}

func getSettlement(c *gin.Context) {
	settlementID := c.Param("id")

	var s Settlement
	err := db.QueryRow(`SELECT id, settlement_id, trade_id, account_id, settlement_date, amount, status, settled_at
		FROM settlements WHERE settlement_id = $1`, settlementID).
		Scan(&s.ID, &s.SettlementID, &s.TradeID, &s.AccountID, &s.SettlementDate, &s.Amount, &s.Status, &s.SettledAt)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Settlement not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": s})
}

func getSettlementsByAccount(c *gin.Context) {
	accountID := c.Param("account_id")

	rows, err := db.Query(`SELECT id, settlement_id, trade_id, account_id, settlement_date, amount, status, settled_at
		FROM settlements WHERE account_id = $1 ORDER BY settlement_date DESC LIMIT 100`, accountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	settlements := []Settlement{}
	for rows.Next() {
		var s Settlement
		rows.Scan(&s.ID, &s.SettlementID, &s.TradeID, &s.AccountID, &s.SettlementDate, &s.Amount, &s.Status, &s.SettledAt)
		settlements = append(settlements, s)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": settlements, "count": len(settlements)})
}

func getSettlementByTrade(c *gin.Context) {
	tradeID := c.Param("trade_id")

	var s Settlement
	err := db.QueryRow(`SELECT id, settlement_id, trade_id, account_id, settlement_date, amount, status, settled_at
		FROM settlements WHERE trade_id = $1`, tradeID).
		Scan(&s.ID, &s.SettlementID, &s.TradeID, &s.AccountID, &s.SettlementDate, &s.Amount, &s.Status, &s.SettledAt)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Settlement not found for this trade"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": s})
}

func settleManually(c *gin.Context) {
	settlementID := c.Param("id")

	now := time.Now()
	result, err := db.Exec("UPDATE settlements SET status = 'settled', settled_at = $1 WHERE settlement_id = $2 AND status = 'pending'",
		now, settlementID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to settle"})
		return
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Settlement not found or not in pending status"})
		return
	}

	settlementsCompleted.Inc()
	publishEvent("settlement.manual", gin.H{"settlement_id": settlementID, "settled_at": now})

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Settlement completed manually"})
}

func failSettlement(c *gin.Context) {
	settlementID := c.Param("id")
	var req struct {
		Reason string `json:"reason"`
	}
	c.ShouldBindJSON(&req)

	result, err := db.Exec("UPDATE settlements SET status = 'failed' WHERE settlement_id = $1 AND status = 'pending'", settlementID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update settlement"})
		return
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Settlement not found or not in pending status"})
		return
	}

	publishEvent("settlement.failed", gin.H{"settlement_id": settlementID, "reason": req.Reason})

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Settlement marked as failed"})
}

func getReconciliation(c *gin.Context) {
	var total, pending, completed, failed int
	var totalAmt, settledAmt, pendingAmt float64

	db.QueryRow("SELECT COUNT(*) FROM settlements").Scan(&total)
	db.QueryRow("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM settlements WHERE status = 'pending'").Scan(&pending, &pendingAmt)
	db.QueryRow("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM settlements WHERE status = 'settled'").Scan(&completed, &settledAmt)
	db.QueryRow("SELECT COUNT(*) FROM settlements WHERE status = 'failed'").Scan(&failed)
	db.QueryRow("SELECT COALESCE(SUM(amount),0) FROM settlements").Scan(&totalAmt)

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": ReconciliationReport{
			Date:                 time.Now().Format("2006-01-02"),
			TotalSettlements:     total,
			PendingSettlements:   pending,
			CompletedSettlements: completed,
			FailedSettlements:    failed,
			TotalAmount:          math.Round(totalAmt*100) / 100,
			SettledAmount:        math.Round(settledAmt*100) / 100,
			PendingAmount:        math.Round(pendingAmt*100) / 100,
		},
	})
}

func getDailyReconciliation(c *gin.Context) {
	date := c.DefaultQuery("date", time.Now().Format("2006-01-02"))

	rows, err := db.Query(`
		SELECT status, COUNT(*), COALESCE(SUM(amount),0)
		FROM settlements WHERE DATE(settlement_date) = $1
		GROUP BY status`, date)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	breakdown := map[string]gin.H{}
	var grandTotal float64
	var grandCount int
	for rows.Next() {
		var status string
		var count int
		var amount float64
		rows.Scan(&status, &count, &amount)
		breakdown[status] = gin.H{"count": count, "amount": math.Round(amount*100) / 100}
		grandTotal += amount
		grandCount += count
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"date":         date,
			"total_count":  grandCount,
			"total_amount": math.Round(grandTotal*100) / 100,
			"breakdown":    breakdown,
		},
	})
}

func processPendingSettlements(c *gin.Context) {
	processSettlements()
	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Pending settlements processed"})
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func publishEvent(eventType string, data interface{}) {
	if rabbitCh == nil {
		return
	}
	body, _ := json.Marshal(gin.H{
		"event_type": eventType,
		"data":       data,
		"timestamp":  time.Now(),
		"service":    "clearing-service",
	})
	rabbitCh.Publish("", "settlement_events", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
}

func healthCheck(c *gin.Context) {
	health := gin.H{
		"status":  "UP",
		"service": "tradehub-clearing-service",
		"port":    "8205",
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

	// Settlement stats
	var pending int
	db.QueryRow("SELECT COUNT(*) FROM settlements WHERE status = 'pending'").Scan(&pending)
	health["pending_settlements"] = pending

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
