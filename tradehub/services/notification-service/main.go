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
	"strconv"
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
	amqpConn    *amqp.Connection
	amqpCh      *amqp.Channel
	ctx         = context.Background()

	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tradehub_notifications_requests_total",
			Help: "Total requests to Notification Service",
		},
		[]string{"method", "endpoint", "status"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "tradehub_notifications_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)
	notificationsSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tradehub_notifications_sent_total",
			Help: "Total notifications sent",
		},
		[]string{"type", "channel"},
	)
	alertsTriggered = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "tradehub_price_alerts_triggered_total",
			Help: "Total price alerts triggered",
		},
	)
)

func init() {
	prometheus.MustRegister(requestsTotal)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(notificationsSent)
	prometheus.MustRegister(alertsTriggered)
}

// ─── Models ──────────────────────────────────────────────────────────────────

type Notification struct {
	ID        string `json:"id"`
	TraderID  string `json:"trader_id"`
	Type      string `json:"type"`    // order_filled, order_cancelled, price_alert, settlement, system
	Channel   string `json:"channel"` // email, sms, push, in_app
	Title     string `json:"title"`
	Message   string `json:"message"`
	Priority  string `json:"priority"` // low, normal, high, critical
	Status    string `json:"status"`   // pending, sent, delivered, failed, read
	Metadata  string `json:"metadata"`
	CreatedAt string `json:"created_at"`
	SentAt    string `json:"sent_at,omitempty"`
	ReadAt    string `json:"read_at,omitempty"`
}

type PriceAlert struct {
	ID          string  `json:"id"`
	TraderID    string  `json:"trader_id"`
	Symbol      string  `json:"symbol"`
	Condition   string  `json:"condition"` // above, below, crosses
	Price       float64 `json:"price"`
	Status      string  `json:"status"` // active, triggered, cancelled
	Triggered   bool    `json:"triggered"`
	CreatedAt   string  `json:"created_at"`
	TriggeredAt string  `json:"triggered_at,omitempty"`
}

type NotificationPreference struct {
	TraderID           string `json:"trader_id"`
	EmailEnabled       bool   `json:"email_enabled"`
	SMSEnabled         bool   `json:"sms_enabled"`
	PushEnabled        bool   `json:"push_enabled"`
	InAppEnabled       bool   `json:"in_app_enabled"`
	OrderFills         bool   `json:"order_fills"`
	OrderCancellations bool   `json:"order_cancellations"`
	PriceAlerts        bool   `json:"price_alerts"`
	Settlements        bool   `json:"settlements"`
	DailyDigest        bool   `json:"daily_digest"`
}

type CreateNotificationRequest struct {
	TraderID string `json:"trader_id" binding:"required"`
	Type     string `json:"type" binding:"required"`
	Channel  string `json:"channel" binding:"required"`
	Title    string `json:"title" binding:"required"`
	Message  string `json:"message" binding:"required"`
	Priority string `json:"priority"`
	Metadata string `json:"metadata"`
}

type CreatePriceAlertRequest struct {
	TraderID  string  `json:"trader_id" binding:"required"`
	Symbol    string  `json:"symbol" binding:"required"`
	Condition string  `json:"condition" binding:"required"`
	Price     float64 `json:"price" binding:"required"`
}

type UpdatePreferencesRequest struct {
	EmailEnabled       *bool `json:"email_enabled"`
	SMSEnabled         *bool `json:"sms_enabled"`
	PushEnabled        *bool `json:"push_enabled"`
	InAppEnabled       *bool `json:"in_app_enabled"`
	OrderFills         *bool `json:"order_fills"`
	OrderCancellations *bool `json:"order_cancellations"`
	PriceAlerts        *bool `json:"price_alerts"`
	Settlements        *bool `json:"settlements"`
	DailyDigest        *bool `json:"daily_digest"`
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	log.Println("🚀 TradeHub Notification Service starting...")

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
	amqpConn, err = amqp.Dial(rabbitURL)
	if err != nil {
		log.Println("⚠ RabbitMQ not available:", err)
	} else {
		log.Println("✓ RabbitMQ connected")
		amqpCh, err = amqpConn.Channel()
		if err != nil {
			log.Println("⚠ Failed to open channel:", err)
		} else {
			// Declare queues we'll consume from
			for _, q := range []string{"order_events", "trade_events", "settlement_events"} {
				amqpCh.QueueDeclare(q, true, false, false, false, nil)
			}
			// Declare our notification queue
			amqpCh.QueueDeclare("notification_events", true, false, false, false, nil)
		}
		defer amqpConn.Close()
		defer amqpCh.Close()
	}

	// Initialize tables
	initializeTables()
	seedNotificationData()

	// Start event consumers
	if amqpCh != nil {
		go consumeOrderEvents()
		go consumeTradeEvents()
		go consumeSettlementEvents()
	}

	// Start price alert monitor
	go monitorPriceAlerts()

	// Gin - with request logging
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.Use(prometheusMiddleware())

	// Health & metrics
	router.GET("/health", healthCheck)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	router.GET("/api/v1/ping",
func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong", "service": "notification-service"})
	})

	// API routes
	v1 := router.Group("/api/v1")
	{
		// Notifications
		v1.POST("/notifications", createNotification)
		v1.GET("/notifications", listNotifications)
		v1.GET("/notifications/:id", getNotification)
		v1.PUT("/notifications/:id/read", markAsRead)
		v1.PUT("/notifications/read-all", markAllAsRead)
		v1.DELETE("/notifications/:id", deleteNotification)
		v1.GET("/notifications/unread-count", getUnreadCount)

		// Price Alerts
		v1.POST("/alerts", createPriceAlert)
		v1.GET("/alerts", listPriceAlerts)
		v1.GET("/alerts/:id", getPriceAlert)
		v1.DELETE("/alerts/:id", cancelPriceAlert)

		// Notification Preferences
		v1.GET("/preferences/:trader_id", getPreferences)
		v1.PUT("/preferences/:trader_id", updatePreferences)

		// Internal endpoints
		v1.POST("/internal/notify", internalNotify)
	}

	// Start server
	port := getEnv("PORT", "8208")
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go
func() {
		log.Printf("🚀 Notification Service running on port %s\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down Notification Service...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
	log.Println("Notification Service exited")
}

// ─── Database Initialization ─────────────────────────────────────────────────

func initializeTables() {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS notifications (
			id
varCHAR(36) PRIMARY KEY,
			trader_id
varCHAR(36) NOT NULL,
			type
varCHAR(50) NOT NULL DEFAULT 'system',
			channel
varCHAR(20) NOT NULL DEFAULT 'in_app',
			title
varCHAR(255) NOT NULL,
			message TEXT NOT NULL,
			priority
varCHAR(20) DEFAULT 'normal',
			status
varCHAR(20) DEFAULT 'pending',
			metadata JSONB DEFAULT '{}',
			created_at TIMESTAMP DEFAULT NOW(),
			sent_at TIMESTAMP,
			read_at TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS price_alerts (
			id
varCHAR(36) PRIMARY KEY,
			trader_id
varCHAR(36) NOT NULL,
			symbol
varCHAR(20) NOT NULL,
			condition
varCHAR(20) NOT NULL,
			price DECIMAL(18,4) NOT NULL,
			status
varCHAR(20) DEFAULT 'active',
			triggered BOOLEAN DEFAULT FALSE,
			created_at TIMESTAMP DEFAULT NOW(),
			triggered_at TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS notification_preferences (
			trader_id
varCHAR(36) PRIMARY KEY,
			email_enabled BOOLEAN DEFAULT TRUE,
			sms_enabled BOOLEAN DEFAULT FALSE,
			push_enabled BOOLEAN DEFAULT TRUE,
			in_app_enabled BOOLEAN DEFAULT TRUE,
			order_fills BOOLEAN DEFAULT TRUE,
			order_cancellations BOOLEAN DEFAULT TRUE,
			price_alerts BOOLEAN DEFAULT TRUE,
			settlements BOOLEAN DEFAULT TRUE,
			daily_digest BOOLEAN DEFAULT FALSE,
			updated_at TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_notifications_trader ON notifications(trader_id)`,
		`CREATE INDEX IF NOT EXISTS idx_notifications_status ON notifications(status)`,
		`CREATE INDEX IF NOT EXISTS idx_notifications_type ON notifications(type)`,
		`CREATE INDEX IF NOT EXISTS idx_price_alerts_trader ON price_alerts(trader_id)`,
		`CREATE INDEX IF NOT EXISTS idx_price_alerts_status ON price_alerts(status)`,
	}

	if db != nil {
		for _, q := range queries {
			if _, err := db.Exec(q); err != nil {
				log.Println("⚠ Table creation error:", err)
			}
		}
		log.Println("✓ Notification tables ready")
	}
}

func seedNotificationData() {
	if db == nil {
		return
	}
	var count int
	db.QueryRow("SELECT COUNT(*) FROM notifications").Scan(&count)
	if count > 0 {
		return
	}

	notifications := []struct {
		traderID string
		ntype    string
		channel  string
		title    string
		message  string
		priority string
		status   string
	}{
		{"TRD-001", "order_filled", "in_app", "Order Filled: AAPL", "Your market order for 100 shares of AAPL has been filled at $178.50", "normal", "delivered"},
		{"TRD-001", "settlement", "email", "Settlement Complete", "Trade settlement for AAPL completed successfully. T+2 settlement cycle finished.", "normal", "sent"},
		{"TRD-002", "price_alert", "push", "Price Alert: TSLA above $250", "TSLA has crossed above your target price of $250.00. Current price: $251.35", "high", "delivered"},
		{"TRD-002", "order_cancelled", "in_app", "Order Cancelled", "Your limit order #ORD-2024-0015 for GOOGL has been cancelled.", "normal", "read"},
		{"TRD-003", "system", "in_app", "Welcome to TradeHub", "Your trading account has been activated. Start trading today!", "low", "delivered"},
		{"TRD-003", "order_filled", "push", "Order Filled: MSFT", "Your limit order for 50 shares of MSFT has been filled at $378.90", "normal", "sent"},
		{"TRD-004", "system", "email", "Account Verification Complete", "Your KYC verification is now complete. You have full trading access.", "normal", "delivered"},
	}

	for _, n := range notifications {
		db.Exec(`INSERT INTO notifications (id, trader_id, type, channel, title, message, priority, status, created_at, sent_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW() - interval '1 hour' * floor(random()*48), NOW())`,
			uuid.New().String(), n.traderID, n.ntype, n.channel, n.title, n.message, n.priority, n.status)
	}

	// Seed price alerts
	alerts := []struct {
		traderID  string
		symbol    string
		condition string
		price     float64
	}{
		{"TRD-001", "AAPL", "above", 185.00},
		{"TRD-001", "TSLA", "below", 230.00},
		{"TRD-002", "GOOGL", "above", 150.00},
		{"TRD-002", "NVDA", "above", 900.00},
		{"TRD-003", "MSFT", "below", 370.00},
	}
	for _, a := range alerts {
		db.Exec(`INSERT INTO price_alerts (id, trader_id, symbol, condition, price, status, created_at)
			VALUES ($1, $2, $3, $4, $5, 'active', NOW())`,
			uuid.New().String(), a.traderID, a.symbol, a.condition, a.price)
	}

	// Seed preferences
	traders := []string{"TRD-001", "TRD-002", "TRD-003", "TRD-004", "TRD-005"}
	for _, tid := range traders {
		db.Exec(`INSERT INTO notification_preferences (trader_id) VALUES ($1) ON CONFLICT DO NOTHING`, tid)
	}

	log.Println("✓ Notification seed data inserted")
}

// ─── RabbitMQ Consumers ──────────────────────────────────────────────────────

func consumeOrderEvents() {
	msgs, err := amqpCh.Consume("order_events", "notification-service-orders", true, false, false, false, nil)
	if err != nil {
		log.Println("⚠ Failed to consume order_events:", err)
		return
	}
	log.Println("📨 Consuming order_events queue...")

	for msg := range msgs {
		var event map[string]interface{}
		if json.Unmarshal(msg.Body, &event) != nil {
			continue
		}

		eventType, _ := event["event"].(string)
		traderID, _ := event["trader_id"].(string)
		symbol, _ := event["symbol"].(string)
		orderID, _ := event["order_id"].(string)

		if traderID == "" {
			continue
		}

		var title, message, priority string
		switch eventType {
		case "order.placed":
			title = fmt.Sprintf("Order Placed: %s", symbol)
			message = fmt.Sprintf("Your order %s for %s has been placed successfully.", orderID, symbol)
			priority = "normal"
		case "order.filled":
			title = fmt.Sprintf("Order Filled: %s", symbol)
			quantity, _ := event["quantity"].(float64)
			price, _ := event["price"].(float64)
			message = fmt.Sprintf("Your order for %.0f shares of %s has been filled at $%.2f", quantity, symbol, price)
			priority = "high"
		case "order.partial_fill":
			title = fmt.Sprintf("Partial Fill: %s", symbol)
			message = fmt.Sprintf("Your order %s for %s has been partially filled.", orderID, symbol)
			priority = "normal"
		case "order.cancelled":
			title = fmt.Sprintf("Order Cancelled: %s", symbol)
			message = fmt.Sprintf("Your order %s for %s has been cancelled.", orderID, symbol)
			priority = "normal"
		default:
			continue
		}

		createNotificationFromEvent(traderID, eventType, title, message, priority, string(msg.Body))
	}
}

func consumeTradeEvents() {
	msgs, err := amqpCh.Consume("trade_events", "notification-service-trades", true, false, false, false, nil)
	if err != nil {
		log.Println("⚠ Failed to consume trade_events:", err)
		return
	}
	log.Println("📨 Consuming trade_events queue...")

	for msg := range msgs {
		var event map[string]interface{}
		if json.Unmarshal(msg.Body, &event) != nil {
			continue
		}

		traderID, _ := event["trader_id"].(string)
		symbol, _ := event["symbol"].(string)
		side, _ := event["side"].(string)
		quantity, _ := event["quantity"].(float64)
		price, _ := event["price"].(float64)
		commission, _ := event["commission"].(float64)

		if traderID == "" {
			continue
		}

		total := quantity*price + commission
		title := fmt.Sprintf("Trade Executed: %s %s", side, symbol)
		message := fmt.Sprintf("Trade executed: %s %.0f shares of %s at $%.2f. Total: $%.2f (incl. $%.2f commission)",
			side, quantity, symbol, price, total, commission)

		createNotificationFromEvent(traderID, "trade.executed", title, message, "high", string(msg.Body))
	}
}

func consumeSettlementEvents() {
	msgs, err := amqpCh.Consume("settlement_events", "notification-service-settlements", true, false, false, false, nil)
	if err != nil {
		log.Println("⚠ Failed to consume settlement_events:", err)
		return
	}
	log.Println("📨 Consuming settlement_events queue...")

	for msg := range msgs {
		var event map[string]interface{}
		if json.Unmarshal(msg.Body, &event) != nil {
			continue
		}

		traderID, _ := event["trader_id"].(string)
		symbol, _ := event["symbol"].(string)
		status, _ := event["status"].(string)
		settlementID, _ := event["settlement_id"].(string)

		if traderID == "" {
			continue
		}

		var title, message, priority string
		switch status {
		case "settled":
			title = fmt.Sprintf("Settlement Complete: %s", symbol)
			message = fmt.Sprintf("Settlement %s for %s has been completed successfully.", settlementID, symbol)
			priority = "normal"
		case "failed":
			title = fmt.Sprintf("Settlement Failed: %s", symbol)
			message = fmt.Sprintf("Settlement %s for %s has failed. Please contact support.", settlementID, symbol)
			priority = "critical"
		default:
			continue
		}

		createNotificationFromEvent(traderID, "settlement."+status, title, message, priority, string(msg.Body))
	}
}

func createNotificationFromEvent(traderID, ntype, title, message, priority, metadata string) {
	if db == nil {
		return
	}

	id := uuid.New().String()
	_, err := db.Exec(`INSERT INTO notifications (id, trader_id, type, channel, title, message, priority, status, metadata, created_at, sent_at)
		VALUES ($1, $2, $3, 'in_app', $4, $5, $6, 'sent', $7, NOW(), NOW())`,
		id, traderID, ntype, title, message, priority, metadata)
	if err != nil {
		log.Println("⚠ Failed to create notification:", err)
		return
	}

	notificationsSent.WithLabelValues(ntype, "in_app").Inc()

	// Cache unread count update
	redisClient.Del(ctx, "notifications:unread:"+traderID)

	log.Printf("📬 Notification sent: [%s] %s -> %s", ntype, title, traderID)
}

// ─── Price Alert Monitor ─────────────────────────────────────────────────────

func monitorPriceAlerts() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if db == nil {
			continue
		}

		rows, err := db.Query("SELECT id, trader_id, symbol, condition, price FROM price_alerts WHERE status = 'active'")
		if err != nil {
			continue
		}

		type alert struct {
			id        string
			traderID  string
			symbol    string
			condition string
			price     float64
		}
		var alerts []alert

		for rows.Next() {
			var a alert
			rows.Scan(&a.id, &a.traderID, &a.symbol, &a.condition, &a.price)
			alerts = append(alerts, a)
		}
		rows.Close()

		for _, a := range alerts {
			// Get current price from Redis
			priceStr, err := redisClient.Get(ctx, "market:price:"+a.symbol).Result()
			if err != nil {
				continue
			}
			currentPrice, err := strconv.ParseFloat(priceStr, 64)
			if err != nil {
				continue
			}

			triggered := false
			switch a.condition {
			case "above":
				triggered = currentPrice >= a.price
			case "below":
				triggered = currentPrice <= a.price
			case "crosses":
				triggered = currentPrice >= a.price || currentPrice <= a.price
			}

			if triggered {
				// Mark alert as triggered
				db.Exec("UPDATE price_alerts SET status = 'triggered', triggered = TRUE, triggered_at = NOW() WHERE id = $1", a.id)
				alertsTriggered.Inc()

				// Create notification
				title := fmt.Sprintf("Price Alert: %s %s $%.2f", a.symbol, a.condition, a.price)
				message := fmt.Sprintf("%s has reached your target. Condition: %s $%.2f. Current price: $%.2f",
					a.symbol, a.condition, a.price, currentPrice)

				createNotificationFromEvent(a.traderID, "price_alert", title, message, "high", fmt.Sprintf(`{"symbol":"%s","condition":"%s","target_price":%.2f,"current_price":%.2f}`, a.symbol, a.condition, a.price, currentPrice))

				log.Printf("🔔 Price alert triggered: %s %s $%.2f (current: $%.2f)", a.symbol, a.condition, a.price, currentPrice)
			}
		}
	}
}

// ─── Notification Handlers ───────────────────────────────────────────────────

func createNotification(c *gin.Context) {
	var req CreateNotificationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	// Validate type
	validTypes := map[string]bool{"order_filled": true, "order_cancelled": true, "price_alert": true, "settlement": true, "system": true, "trade_executed": true}
	if !validTypes[req.Type] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid notification type"})
		return
	}

	// Validate channel
	validChannels := map[string]bool{"email": true, "sms": true, "push": true, "in_app": true}
	if !validChannels[req.Channel] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid channel"})
		return
	}

	if req.Priority == "" {
		req.Priority = "normal"
	}

	id := uuid.New().String()
	_, err := db.Exec(`INSERT INTO notifications (id, trader_id, type, channel, title, message, priority, status, metadata, created_at, sent_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'sent', $8, NOW(), NOW())`,
		id, req.TraderID, req.Type, req.Channel, req.Title, req.Message, req.Priority, req.Metadata)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create notification", "details": err.Error()})
		return
	}

	notificationsSent.WithLabelValues(req.Type, req.Channel).Inc()

	// Invalidate unread cache
	redisClient.Del(ctx, "notifications:unread:"+req.TraderID)

	c.JSON(http.StatusCreated, gin.H{
		"status":  "success",
		"message": "Notification created",
		"data":    gin.H{"id": id},
	})
}

func listNotifications(c *gin.Context) {
	traderID := c.Query("trader_id")
	status := c.DefaultQuery("status", "")
	ntype := c.DefaultQuery("type", "")
	limit := c.DefaultQuery("limit", "50")
	offset := c.DefaultQuery("offset", "0")

	query := "SELECT id, trader_id, type, channel, title, message, priority, status, COALESCE(metadata::text, '{}'), created_at, sent_at, read_at FROM notifications WHERE 1=1"
	args := []interface{}{}
	argIdx := 1

	if traderID != "" {
		query += fmt.Sprintf(" AND trader_id = $%d", argIdx)
		args = append(args, traderID)
		argIdx++
	}
	if status != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}
	if ntype != "" {
		query += fmt.Sprintf(" AND type = $%d", argIdx)
		args = append(args, ntype)
		argIdx++
	}

	query += " ORDER BY created_at DESC"
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
	args = append(args, limit, offset)

	rows, err := db.Query(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error", "details": err.Error()})
		return
	}
	defer rows.Close()

	notifications := []Notification{}
	for rows.Next() {
		var n Notification
		var sentAt, readAt sql.NullTime
		var createdAt time.Time
		rows.Scan(&n.ID, &n.TraderID, &n.Type, &n.Channel, &n.Title, &n.Message, &n.Priority, &n.Status, &n.Metadata, &createdAt, &sentAt, &readAt)
		n.CreatedAt = createdAt.Format(time.RFC3339)
		if sentAt.Valid {
			n.SentAt = sentAt.Time.Format(time.RFC3339)
		}
		if readAt.Valid {
			n.ReadAt = readAt.Time.Format(time.RFC3339)
		}
		notifications = append(notifications, n)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": notifications, "count": len(notifications)})
}

func getNotification(c *gin.Context) {
	id := c.Param("id")

	var n Notification
	var sentAt, readAt sql.NullTime
	var createdAt time.Time
	err := db.QueryRow(`SELECT id, trader_id, type, channel, title, message, priority, status, COALESCE(metadata::text, '{}'), created_at, sent_at, read_at
		FROM notifications WHERE id = $1`, id).Scan(
		&n.ID, &n.TraderID, &n.Type, &n.Channel, &n.Title, &n.Message, &n.Priority, &n.Status, &n.Metadata, &createdAt, &sentAt, &readAt)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Notification not found"})
		return
	}
	n.CreatedAt = createdAt.Format(time.RFC3339)
	if sentAt.Valid {
		n.SentAt = sentAt.Time.Format(time.RFC3339)
	}
	if readAt.Valid {
		n.ReadAt = readAt.Time.Format(time.RFC3339)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": n})
}

func markAsRead(c *gin.Context) {
	id := c.Param("id")

	var traderID string
	err := db.QueryRow("UPDATE notifications SET status = 'read', read_at = NOW() WHERE id = $1 RETURNING trader_id", id).Scan(&traderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Notification not found"})
		return
	}

	redisClient.Del(ctx, "notifications:unread:"+traderID)

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Notification marked as read"})
}

func markAllAsRead(c *gin.Context) {
	traderID := c.Query("trader_id")
	if traderID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "trader_id required"})
		return
	}

	result, err := db.Exec("UPDATE notifications SET status = 'read', read_at = NOW() WHERE trader_id = $1 AND status != 'read'", traderID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	affected, _ := result.RowsAffected()
	redisClient.Del(ctx, "notifications:unread:"+traderID)

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": fmt.Sprintf("%d notifications marked as read", affected)})
}

func deleteNotification(c *gin.Context) {
	id := c.Param("id")

	result, err := db.Exec("DELETE FROM notifications WHERE id = $1", id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Notification not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Notification deleted"})
}

func getUnreadCount(c *gin.Context) {
	traderID := c.Query("trader_id")
	if traderID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "trader_id required"})
		return
	}

	// Check Redis cache first
	cacheKey := "notifications:unread:" + traderID
	if cached, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
		count, _ := strconv.Atoi(cached)
		c.JSON(http.StatusOK, gin.H{"status": "success", "data": gin.H{"trader_id": traderID, "unread_count": count}})
		return
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM notifications WHERE trader_id = $1 AND status != 'read'", traderID).Scan(&count)

	// Cache for 30 seconds
	redisClient.Set(ctx, cacheKey, count, 30*time.Second)

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": gin.H{"trader_id": traderID, "unread_count": count}})
}

// ─── Price Alert Handlers ────────────────────────────────────────────────────

func createPriceAlert(c *gin.Context) {
	var req CreatePriceAlertRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	validConditions := map[string]bool{"above": true, "below": true, "crosses": true}
	if !validConditions[req.Condition] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid condition. Must be: above, below, or crosses"})
		return
	}

	if req.Price <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Price must be positive"})
		return
	}

	id := uuid.New().String()
	_, err := db.Exec(`INSERT INTO price_alerts (id, trader_id, symbol, condition, price, status, created_at)
		VALUES ($1, $2, $3, $4, $5, 'active', NOW())`,
		id, req.TraderID, req.Symbol, req.Condition, req.Price)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create price alert"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"status":  "success",
		"message": "Price alert created",
		"data":    gin.H{"id": id, "symbol": req.Symbol, "condition": req.Condition, "price": req.Price},
	})
}

func listPriceAlerts(c *gin.Context) {
	traderID := c.Query("trader_id")
	status := c.DefaultQuery("status", "")

	query := "SELECT id, trader_id, symbol, condition, price, status, triggered, created_at, triggered_at FROM price_alerts WHERE 1=1"
	args := []interface{}{}
	argIdx := 1

	if traderID != "" {
		query += fmt.Sprintf(" AND trader_id = $%d", argIdx)
		args = append(args, traderID)
		argIdx++
	}
	if status != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}

	query += " ORDER BY created_at DESC"

	rows, err := db.Query(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	alerts := []PriceAlert{}
	for rows.Next() {
		var a PriceAlert
		var createdAt time.Time
		var triggeredAt sql.NullTime
		rows.Scan(&a.ID, &a.TraderID, &a.Symbol, &a.Condition, &a.Price, &a.Status, &a.Triggered, &createdAt, &triggeredAt)
		a.CreatedAt = createdAt.Format(time.RFC3339)
		if triggeredAt.Valid {
			a.TriggeredAt = triggeredAt.Time.Format(time.RFC3339)
		}
		alerts = append(alerts, a)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": alerts, "count": len(alerts)})
}

func getPriceAlert(c *gin.Context) {
	id := c.Param("id")

	var a PriceAlert
	var createdAt time.Time
	var triggeredAt sql.NullTime
	err := db.QueryRow(`SELECT id, trader_id, symbol, condition, price, status, triggered, created_at, triggered_at
		FROM price_alerts WHERE id = $1`, id).Scan(
		&a.ID, &a.TraderID, &a.Symbol, &a.Condition, &a.Price, &a.Status, &a.Triggered, &createdAt, &triggeredAt)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Price alert not found"})
		return
	}
	a.CreatedAt = createdAt.Format(time.RFC3339)
	if triggeredAt.Valid {
		a.TriggeredAt = triggeredAt.Time.Format(time.RFC3339)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": a})
}

func cancelPriceAlert(c *gin.Context) {
	id := c.Param("id")

	result, err := db.Exec("UPDATE price_alerts SET status = 'cancelled' WHERE id = $1 AND status = 'active'", id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Active price alert not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Price alert cancelled"})
}

// ─── Preference Handlers ────────────────────────────────────────────────────

func getPreferences(c *gin.Context) {
	traderID := c.Param("trader_id")

	var p NotificationPreference
	err := db.QueryRow(`SELECT trader_id, email_enabled, sms_enabled, push_enabled, in_app_enabled,
		order_fills, order_cancellations, price_alerts, settlements, daily_digest
		FROM notification_preferences WHERE trader_id = $1`, traderID).Scan(
		&p.TraderID, &p.EmailEnabled, &p.SMSEnabled, &p.PushEnabled, &p.InAppEnabled,
		&p.OrderFills, &p.OrderCancellations, &p.PriceAlerts, &p.Settlements, &p.DailyDigest)
	if err != nil {
		// Return defaults
		p = NotificationPreference{
			TraderID:           traderID,
			EmailEnabled:       true,
			SMSEnabled:         false,
			PushEnabled:        true,
			InAppEnabled:       true,
			OrderFills:         true,
			OrderCancellations: true,
			PriceAlerts:        true,
			Settlements:        true,
			DailyDigest:        false,
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": p})
}

func updatePreferences(c *gin.Context) {
	traderID := c.Param("trader_id")

	var req UpdatePreferencesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	// Upsert preferences
	_, err := db.Exec(`INSERT INTO notification_preferences (trader_id) VALUES ($1) ON CONFLICT DO NOTHING`, traderID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	updates := map[string]interface{}{}
	if req.EmailEnabled != nil {
		updates["email_enabled"] = *req.EmailEnabled
	}
	if req.SMSEnabled != nil {
		updates["sms_enabled"] = *req.SMSEnabled
	}
	if req.PushEnabled != nil {
		updates["push_enabled"] = *req.PushEnabled
	}
	if req.InAppEnabled != nil {
		updates["in_app_enabled"] = *req.InAppEnabled
	}
	if req.OrderFills != nil {
		updates["order_fills"] = *req.OrderFills
	}
	if req.OrderCancellations != nil {
		updates["order_cancellations"] = *req.OrderCancellations
	}
	if req.PriceAlerts != nil {
		updates["price_alerts"] = *req.PriceAlerts
	}
	if req.Settlements != nil {
		updates["settlements"] = *req.Settlements
	}
	if req.DailyDigest != nil {
		updates["daily_digest"] = *req.DailyDigest
	}

	if len(updates) > 0 {
		setClauses := []string{}
		args := []interface{}{}
		idx := 1
		for col, val := range updates {
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, idx))
			args = append(args, val)
			idx++
		}
		args = append(args, traderID)

		query := fmt.Sprintf("UPDATE notification_preferences SET %s, updated_at = NOW() WHERE trader_id = $%d",
			joinStrings(setClauses, ", "), idx)
		db.Exec(query, args...)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Preferences updated"})
}

// ─── Internal Endpoints ──────────────────────────────────────────────────────

func internalNotify(c *gin.Context) {
	var req struct {
		TraderID string `json:"trader_id" binding:"required"`
		Type     string `json:"type" binding:"required"`
		Title    string `json:"title" binding:"required"`
		Message  string `json:"message" binding:"required"`
		Priority string `json:"priority"`
		Metadata string `json:"metadata"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	if req.Priority == "" {
		req.Priority = "normal"
	}

	createNotificationFromEvent(req.TraderID, req.Type, req.Title, req.Message, req.Priority, req.Metadata)

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Internal notification sent"})
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func joinStrings(strs []string, sep string) string {
	result := ""
	for i, s := range strs {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}

func healthCheck(c *gin.Context) {
	health := gin.H{
		"status":  "UP",
		"service": "tradehub-notification-service",
		"port":    "8208",
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
	if amqpConn != nil && !amqpConn.IsClosed() {
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
