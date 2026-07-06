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
			Name: "tradehub_trader_requests_total",
			Help: "Total requests to Trader Service",
		},
		[]string{"method", "endpoint", "status"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "tradehub_trader_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)
	tradersCreated = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "tradehub_traders_created_total",
			Help: "Total traders created",
		},
	)
	tradersUpdated = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "tradehub_traders_updated_total",
			Help: "Total traders updated",
		},
	)
)

func init() {
	prometheus.MustRegister(requestsTotal)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(tradersCreated)
	prometheus.MustRegister(tradersUpdated)
}

// ─── Models ──────────────────────────────────────────────────────────────────

type Trader struct {
	ID           int       `json:"id"`
	TraderID     string    `json:"trader_id"`
	Username     string    `json:"username"`
	Email        string    `json:"email"`
	FullName     string    `json:"full_name"`
	Phone        string    `json:"phone"`
	TradingLevel string    `json:"trading_level"`
	KYCStatus    string    `json:"kyc_status"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type CreateTraderRequest struct {
	Username string `json:"username" binding:"required"`
	Email    string `json:"email" binding:"required"`
	FullName string `json:"full_name" binding:"required"`
	Phone    string `json:"phone"`
}

type UpdateTraderRequest struct {
	FullName     string `json:"full_name"`
	Phone        string `json:"phone"`
	TradingLevel string `json:"trading_level"`
}

type KYCRequest struct {
	DocumentType   string `json:"document_type" binding:"required"`
	DocumentNumber string `json:"document_number" binding:"required"`
	Action         string `json:"action" binding:"required"` // submit, approve, reject
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	log.Println("🚀 TradeHub Trader Service starting...")

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
			rabbitCh.QueueDeclare("trader_events", true, false, false, false, nil)
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
		c.JSON(http.StatusOK, gin.H{"message": "pong", "service": "trader-service"})
	})

	// API routes
	v1 := router.Group("/api/v1")
	{
		v1.POST("/traders", createTrader)
		v1.GET("/traders", listTraders)
		v1.GET("/traders/:id", getTrader)
		v1.PUT("/traders/:id", updateTrader)
		v1.DELETE("/traders/:id", deleteTrader)
		v1.POST("/traders/:id/kyc", manageKYC)
		v1.GET("/traders/:id/kyc", getKYCStatus)
		v1.GET("/traders/search", searchTraders)
	}

	// Start server
	port := getEnv("PORT", "8201")
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go
func() {
		log.Printf("🚀 Trader Service running on port %s\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down Trader Service...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
	log.Println("Trader Service exited")
}

// ─── Database Initialization ─────────────────────────────────────────────────

func initializeTables() {
	query := `
	CREATE TABLE IF NOT EXISTS traders (
		id SERIAL PRIMARY KEY,
		trader_id
varCHAR(50) UNIQUE NOT NULL,
		username
varCHAR(100) UNIQUE NOT NULL,
		email
varCHAR(255) UNIQUE NOT NULL,
		full_name
varCHAR(255) NOT NULL,
		phone
varCHAR(50) DEFAULT '',
		trading_level
varCHAR(20) DEFAULT 'basic',
		kyc_status
varCHAR(20) DEFAULT 'pending',
		status
varCHAR(20) DEFAULT 'active',
		created_at TIMESTAMP DEFAULT NOW(),
		updated_at TIMESTAMP DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_traders_trader_id ON traders(trader_id);
	CREATE INDEX IF NOT EXISTS idx_traders_email ON traders(email);
	CREATE INDEX IF NOT EXISTS idx_traders_username ON traders(username);
	CREATE INDEX IF NOT EXISTS idx_traders_status ON traders(status);
	`

	if db != nil {
		if _, err := db.Exec(query); err != nil {
			log.Println("⚠ Table creation error:", err)
		} else {
			log.Println("✓ Traders table ready")
			seedTraders()
		}
	}
}

func seedTraders() {
	var count int
	db.QueryRow("SELECT COUNT(*) FROM traders").Scan(&count)
	if count > 0 {
		return
	}

	traders := []struct {
		username, email, fullName, phone, level, kyc string
	}{
		{"john_trader", "john@tradehub.com", "John Smith", "+1-555-0101", "advanced", "verified"},
		{"jane_invest", "jane@tradehub.com", "Jane Doe", "+1-555-0102", "professional", "verified"},
		{"bob_market", "bob@tradehub.com", "Bob Johnson", "+1-555-0103", "basic", "pending"},
		{"alice_stocks", "alice@tradehub.com", "Alice Williams", "+1-555-0104", "intermediate", "verified"},
		{"charlie_day", "charlie@tradehub.com", "Charlie Brown", "+1-555-0105", "basic", "submitted"},
	}

	for _, t := range traders {
		traderID := "TRD-" + uuid.New().String()[:8]
		db.Exec(`INSERT INTO traders (trader_id, username, email, full_name, phone, trading_level, kyc_status, status)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, 'active')`,
			traderID, t.username, t.email, t.fullName, t.phone, t.level, t.kyc)
	}
	log.Println("✓ Seed traders inserted")
}

// ─── Handlers ────────────────────────────────────────────────────────────────

func createTrader(c *gin.Context) {
	var req CreateTraderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	// Validate email format
	if !strings.Contains(req.Email, "@") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid email format"})
		return
	}

	// Check duplicate username
	var exists bool
	db.QueryRow("SELECT EXISTS(SELECT 1 FROM traders WHERE username = $1)", req.Username).Scan(&exists)
	if exists {
		c.JSON(http.StatusConflict, gin.H{"error": "Username already exists"})
		return
	}

	// Check duplicate email
	db.QueryRow("SELECT EXISTS(SELECT 1 FROM traders WHERE email = $1)", req.Email).Scan(&exists)
	if exists {
		c.JSON(http.StatusConflict, gin.H{"error": "Email already exists"})
		return
	}

	traderID := "TRD-" + uuid.New().String()[:8]
	phone := req.Phone
	if phone == "" {
		phone = ""
	}

	var trader Trader
	err := db.QueryRow(`
		INSERT INTO traders (trader_id, username, email, full_name, phone, trading_level, kyc_status, status)
		VALUES ($1, $2, $3, $4, $5, 'basic', 'pending', 'active')
		RETURNING id, trader_id, username, email, full_name, phone, trading_level, kyc_status, status, created_at, updated_at`,
		traderID, req.Username, req.Email, req.FullName, phone,
	).Scan(&trader.ID, &trader.TraderID, &trader.Username, &trader.Email, &trader.FullName,
		&trader.Phone, &trader.TradingLevel, &trader.KYCStatus, &trader.Status,
		&trader.CreatedAt, &trader.UpdatedAt)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create trader", "details": err.Error()})
		return
	}

	// Cache trader
	cacheTrader(trader)

	// Publish event
	publishEvent("trader.created", trader)
	tradersCreated.Inc()

	c.JSON(http.StatusCreated, gin.H{"status": "success", "data": trader})
}

func getTrader(c *gin.Context) {
	traderID := c.Param("id")

	// Try cache first
	cached, err := redisClient.Get(ctx, "trader:"+traderID).Result()
	if err == nil {
		var trader Trader
		if json.Unmarshal([]byte(cached), &trader) == nil {
			c.JSON(http.StatusOK, gin.H{"status": "success", "data": trader, "source": "cache"})
			return
		}
	}

	var trader Trader
	err = db.QueryRow(`
		SELECT id, trader_id, username, email, full_name, phone, trading_level, kyc_status, status, created_at, updated_at
		FROM traders WHERE trader_id = $1`, traderID,
	).Scan(&trader.ID, &trader.TraderID, &trader.Username, &trader.Email, &trader.FullName,
		&trader.Phone, &trader.TradingLevel, &trader.KYCStatus, &trader.Status,
		&trader.CreatedAt, &trader.UpdatedAt)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Trader not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error", "details": err.Error()})
		return
	}

	cacheTrader(trader)
	c.JSON(http.StatusOK, gin.H{"status": "success", "data": trader})
}

func listTraders(c *gin.Context) {
	status := c.DefaultQuery("status", "")
	kycStatus := c.DefaultQuery("kyc_status", "")
	limit := c.DefaultQuery("limit", "50")
	offset := c.DefaultQuery("offset", "0")

	query := "SELECT id, trader_id, username, email, full_name, phone, trading_level, kyc_status, status, created_at, updated_at FROM traders WHERE 1=1"
	args := []interface{}{}
	argIdx := 1

	if status != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}
	if kycStatus != "" {
		query += fmt.Sprintf(" AND kyc_status = $%d", argIdx)
		args = append(args, kycStatus)
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

	traders := []Trader{}
	for rows.Next() {
		var t Trader
		if err := rows.Scan(&t.ID, &t.TraderID, &t.Username, &t.Email, &t.FullName,
			&t.Phone, &t.TradingLevel, &t.KYCStatus, &t.Status,
			&t.CreatedAt, &t.UpdatedAt); err != nil {
			continue
		}
		traders = append(traders, t)
	}

	var total int
	db.QueryRow("SELECT COUNT(*) FROM traders").Scan(&total)

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": traders, "total": total})
}

func updateTrader(c *gin.Context) {
	traderID := c.Param("id")
	var req UpdateTraderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	// Build dynamic update
	sets := []string{}
	args := []interface{}{}
	argIdx := 1

	if req.FullName != "" {
		sets = append(sets, fmt.Sprintf("full_name = $%d", argIdx))
		args = append(args, req.FullName)
		argIdx++
	}
	if req.Phone != "" {
		sets = append(sets, fmt.Sprintf("phone = $%d", argIdx))
		args = append(args, req.Phone)
		argIdx++
	}
	if req.TradingLevel != "" {
		validLevels := map[string]bool{"basic": true, "intermediate": true, "advanced": true, "professional": true}
		if !validLevels[req.TradingLevel] {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid trading level. Must be: basic, intermediate, advanced, professional"})
			return
		}
		sets = append(sets, fmt.Sprintf("trading_level = $%d", argIdx))
		args = append(args, req.TradingLevel)
		argIdx++
	}

	if len(sets) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No fields to update"})
		return
	}

	sets = append(sets, fmt.Sprintf("updated_at = $%d", argIdx))
	args = append(args, time.Now())
	argIdx++

	args = append(args, traderID)
	query := fmt.Sprintf("UPDATE traders SET %s WHERE trader_id = $%d RETURNING id, trader_id, username, email, full_name, phone, trading_level, kyc_status, status, created_at, updated_at",
		strings.Join(sets, ", "), argIdx)

	var trader Trader
	err := db.QueryRow(query, args...).Scan(
		&trader.ID, &trader.TraderID, &trader.Username, &trader.Email, &trader.FullName,
		&trader.Phone, &trader.TradingLevel, &trader.KYCStatus, &trader.Status,
		&trader.CreatedAt, &trader.UpdatedAt)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Trader not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update trader", "details": err.Error()})
		return
	}

	cacheTrader(trader)
	publishEvent("trader.updated", trader)
	tradersUpdated.Inc()

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": trader})
}

func deleteTrader(c *gin.Context) {
	traderID := c.Param("id")

	result, err := db.Exec("UPDATE traders SET status = 'inactive', updated_at = $1 WHERE trader_id = $2 AND status = 'active'", time.Now(), traderID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to deactivate trader"})
		return
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Trader not found or already inactive"})
		return
	}

	redisClient.Del(ctx, "trader:"+traderID)
	publishEvent("trader.deactivated", gin.H{"trader_id": traderID})

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Trader deactivated"})
}

func manageKYC(c *gin.Context) {
	traderID := c.Param("id")
	var req KYCRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	var currentKYC string
	err := db.QueryRow("SELECT kyc_status FROM traders WHERE trader_id = $1", traderID).Scan(&currentKYC)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Trader not found"})
		return
	}

	var newStatus string
	switch req.Action {
	case "submit":
		if currentKYC != "pending" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "KYC already submitted or verified"})
			return
		}
		newStatus = "submitted"
	case "approve":
		if currentKYC != "submitted" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "KYC must be submitted before approval"})
			return
		}
		newStatus = "verified"
	case "reject":
		if currentKYC != "submitted" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "KYC must be submitted before rejection"})
			return
		}
		newStatus = "rejected"
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid action. Must be: submit, approve, reject"})
		return
	}

	_, err = db.Exec("UPDATE traders SET kyc_status = $1, updated_at = $2 WHERE trader_id = $3", newStatus, time.Now(), traderID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update KYC status"})
		return
	}

	// If approved, upgrade trading level
	if newStatus == "verified" {
		db.Exec("UPDATE traders SET trading_level = 'intermediate' WHERE trader_id = $1 AND trading_level = 'basic'", traderID)
	}

	redisClient.Del(ctx, "trader:"+traderID)
	publishEvent("trader.kyc."+req.Action, gin.H{"trader_id": traderID, "kyc_status": newStatus})

	c.JSON(http.StatusOK, gin.H{"status": "success", "kyc_status": newStatus, "message": fmt.Sprintf("KYC %s successful", req.Action)})
}

func getKYCStatus(c *gin.Context) {
	traderID := c.Param("id")

	var kycStatus, tradingLevel string
	err := db.QueryRow("SELECT kyc_status, trading_level FROM traders WHERE trader_id = $1", traderID).Scan(&kycStatus, &tradingLevel)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Trader not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":        "success",
		"trader_id":     traderID,
		"kyc_status":    kycStatus,
		"trading_level": tradingLevel,
	})
}

func searchTraders(c *gin.Context) {
	query := c.Query("q")
	if query == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Search query required"})
		return
	}

	searchPattern := "%" + strings.ToLower(query) + "%"
	rows, err := db.Query(`
		SELECT id, trader_id, username, email, full_name, phone, trading_level, kyc_status, status, created_at, updated_at
		FROM traders WHERE LOWER(username) LIKE $1 OR LOWER(email) LIKE $1 OR LOWER(full_name) LIKE $1
		ORDER BY created_at DESC LIMIT 20`, searchPattern)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Search failed"})
		return
	}
	defer rows.Close()

	traders := []Trader{}
	for rows.Next() {
		var t Trader
		if err := rows.Scan(&t.ID, &t.TraderID, &t.Username, &t.Email, &t.FullName,
			&t.Phone, &t.TradingLevel, &t.KYCStatus, &t.Status,
			&t.CreatedAt, &t.UpdatedAt); err != nil {
			continue
		}
		traders = append(traders, t)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": traders, "count": len(traders)})
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func cacheTrader(trader Trader) {
	data, _ := json.Marshal(trader)
	redisClient.Set(ctx, "trader:"+trader.TraderID, data, 5*time.Minute)
}

func publishEvent(eventType string, data interface{}) {
	if rabbitCh == nil {
		return
	}
	body, _ := json.Marshal(gin.H{
		"event_type": eventType,
		"data":       data,
		"timestamp":  time.Now(),
		"service":    "trader-service",
	})
	rabbitCh.Publish("", "trader_events", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
}

func healthCheck(c *gin.Context) {
	health := gin.H{
		"status":  "UP",
		"service": "tradehub-trader-service",
		"port":    "8201",
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
