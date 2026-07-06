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
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ─── Global
variables ────────────────────────────────────────────────────────

var (
	db          *sql.DB
	redisClient *redis.Client
	ctx         = context.Background()

	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tradehub_portfolio_requests_total",
			Help: "Total requests to Portfolio Service",
		},
		[]string{"method", "endpoint", "status"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "tradehub_portfolio_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)
	portfolioUpdates = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "tradehub_portfolio_updates_total",
			Help: "Total portfolio updates",
		},
	)
	totalPortfolioValue = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "tradehub_total_portfolio_value",
			Help: "Total portfolio value across all accounts",
		},
	)
)

func init() {
	prometheus.MustRegister(requestsTotal)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(portfolioUpdates)
	prometheus.MustRegister(totalPortfolioValue)
}

// ─── Models ──────────────────────────────────────────────────────────────────

type Holding struct {
	ID            int       `json:"id"`
	HoldingID     string    `json:"holding_id"`
	AccountID     string    `json:"account_id"`
	Symbol        string    `json:"symbol"`
	Quantity      float64   `json:"quantity"`
	AveragePrice  float64   `json:"average_price"`
	CurrentPrice  float64   `json:"current_price"`
	MarketValue   float64   `json:"market_value"`
	UnrealizedPnl float64   `json:"unrealized_pnl"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type PortfolioSummary struct {
	AccountID       string    `json:"account_id"`
	TotalValue      float64   `json:"total_value"`
	TotalCost       float64   `json:"total_cost"`
	UnrealizedPnl   float64   `json:"unrealized_pnl"`
	RealizedPnl     float64   `json:"realized_pnl"`
	TotalPnl        float64   `json:"total_pnl"`
	PnlPercent      float64   `json:"pnl_percent"`
	HoldingsCount   int       `json:"holdings_count"`
	TopHolding      string    `json:"top_holding"`
	TopHoldingValue float64   `json:"top_holding_value"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type PnLRecord struct {
	ID         int       `json:"id"`
	AccountID  string    `json:"account_id"`
	Symbol     string    `json:"symbol"`
	TradeID    string    `json:"trade_id"`
	Side       string    `json:"side"`
	Quantity   float64   `json:"quantity"`
	EntryPrice float64   `json:"entry_price"`
	ExitPrice  float64   `json:"exit_price"`
	PnL        float64   `json:"pnl"`
	RecordedAt time.Time `json:"recorded_at"`
}

type UpdateHoldingRequest struct {
	AccountID string  `json:"account_id" binding:"required"`
	Symbol    string  `json:"symbol" binding:"required"`
	Quantity  float64 `json:"quantity" binding:"required"` // positive = buy, negative = sell
	Price     float64 `json:"price" binding:"required"`
	TradeID   string  `json:"trade_id"`
}

type PerformanceMetrics struct {
	DayReturn     float64 `json:"day_return"`
	WeekReturn    float64 `json:"week_return"`
	MonthReturn   float64 `json:"month_return"`
	TotalReturn   float64 `json:"total_return"`
	WinRate       float64 `json:"win_rate"`
	TotalTrades   int     `json:"total_trades"`
	WinningTrades int     `json:"winning_trades"`
	LosingTrades  int     `json:"losing_trades"`
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	log.Println("🚀 TradeHub Portfolio Service starting...")

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

	// Initialize tables
	initializeTables()

	// Start price updater (refreshes current prices from Redis)
	go priceUpdater()

	// Gin - with request logging
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.Use(prometheusMiddleware())

	// Health & metrics
	router.GET("/health", healthCheck)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	router.GET("/api/v1/ping",
func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong", "service": "portfolio-service"})
	})

	// API routes
	v1 := router.Group("/api/v1")
	{
		v1.GET("/portfolio/:account_id", getPortfolioSummary)
		v1.GET("/portfolio/:account_id/holdings", getHoldings)
		v1.GET("/portfolio/:account_id/pnl", getPnL)
		v1.GET("/portfolio/:account_id/performance", getPerformance)
		v1.GET("/portfolio/:account_id/holdings/:symbol", getHoldingBySymbol)
		// Internal endpoints for execution/clearing services
		v1.POST("/holdings/update", updateHolding)
		v1.POST("/portfolio/:account_id/refresh", refreshPortfolio)
	}

	// Start server
	port := getEnv("PORT", "8206")
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go
func() {
		log.Printf("🚀 Portfolio Service running on port %s\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down Portfolio Service...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
	log.Println("Portfolio Service exited")
}

// ─── Database Initialization ─────────────────────────────────────────────────

func initializeTables() {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS holdings (
			id SERIAL PRIMARY KEY,
			holding_id
varCHAR(50) UNIQUE NOT NULL,
			account_id
varCHAR(50) NOT NULL,
			symbol
varCHAR(20) NOT NULL,
			quantity DECIMAL(18,6) DEFAULT 0,
			average_price DECIMAL(18,4) DEFAULT 0,
			current_price DECIMAL(18,4) DEFAULT 0,
			market_value DECIMAL(18,4) DEFAULT 0,
			unrealized_pnl DECIMAL(18,4) DEFAULT 0,
			updated_at TIMESTAMP DEFAULT NOW(),
			UNIQUE(account_id, symbol)
		)`,
		`CREATE TABLE IF NOT EXISTS realized_pnl (
			id SERIAL PRIMARY KEY,
			account_id
varCHAR(50) NOT NULL,
			symbol
varCHAR(20) NOT NULL,
			trade_id
varCHAR(50),
			side
varCHAR(10) NOT NULL,
			quantity DECIMAL(18,6) NOT NULL,
			entry_price DECIMAL(18,4) NOT NULL,
			exit_price DECIMAL(18,4) NOT NULL,
			pnl DECIMAL(18,4) NOT NULL,
			recorded_at TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_holdings_account_id ON holdings(account_id)`,
		`CREATE INDEX IF NOT EXISTS idx_holdings_symbol ON holdings(symbol)`,
		`CREATE INDEX IF NOT EXISTS idx_realized_pnl_account_id ON realized_pnl(account_id)`,
	}

	if db != nil {
		for _, q := range queries {
			if _, err := db.Exec(q); err != nil {
				log.Println("⚠ Table creation error:", err)
			}
		}
		log.Println("✓ Portfolio tables ready")
		seedHoldings()
	}
}

func seedHoldings() {
	var count int
	db.QueryRow("SELECT COUNT(*) FROM holdings").Scan(&count)
	if count > 0 {
		return
	}

	holdings := []struct {
		accountID string
		symbol    string
		quantity  float64
		avgPrice  float64
		curPrice  float64
	}{
		{"ACC-SEED0001", "AAPL", 100, 165.50, 178.50},
		{"ACC-SEED0001", "GOOGL", 50, 135.20, 141.80},
		{"ACC-SEED0001", "MSFT", 75, 350.00, 378.90},
		{"ACC-SEED0002", "TSLA", 30, 220.00, 248.50},
		{"ACC-SEED0002", "NVDA", 20, 750.00, 875.30},
		{"ACC-SEED0003", "META", 40, 480.00, 505.75},
		{"ACC-SEED0003", "AMZN", 60, 170.00, 178.25},
	}

	for i, h := range holdings {
		holdingID := fmt.Sprintf("HLD-%05d", i+1)
		marketValue := h.quantity * h.curPrice
		unrealizedPnl := (h.curPrice - h.avgPrice) * h.quantity
		db.Exec(`INSERT INTO holdings (holding_id, account_id, symbol, quantity, average_price, current_price, market_value, unrealized_pnl)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			holdingID, h.accountID, h.symbol, h.quantity, h.avgPrice, h.curPrice, marketValue, unrealizedPnl)
	}
	log.Println("✓ Seed holdings inserted")
}

// ─── Price Updater (background) ──────────────────────────────────────────────

func priceUpdater() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		rows, err := db.Query("SELECT DISTINCT symbol FROM holdings WHERE quantity > 0")
		if err != nil {
			continue
		}

		for rows.Next() {
			var symbol string
			rows.Scan(&symbol)

			// Get current price from Redis (set by execution engine)
			priceStr, err := redisClient.Get(ctx, "market:price:"+symbol).Result()
			if err != nil {
				continue
			}

			var price float64
			fmt.Sscanf(priceStr, "%f", &price)
			if price <= 0 {
				continue
			}

			// Update all holdings for this symbol
			db.Exec(`UPDATE holdings SET
				current_price = $1,
				market_value = quantity * $1,
				unrealized_pnl = (($1 - average_price) * quantity),
				updated_at = $2
				WHERE symbol = $3 AND quantity > 0`, price, time.Now(), symbol)
		}
		rows.Close()

		// Update total portfolio value metric
		var total float64
		db.QueryRow("SELECT COALESCE(SUM(market_value), 0) FROM holdings WHERE quantity > 0").Scan(&total)
		totalPortfolioValue.Set(total)
	}
}

// ─── Handlers ────────────────────────────────────────────────────────────────

func getPortfolioSummary(c *gin.Context) {
	accountID := c.Param("account_id")

	// Try cache
	cached, err := redisClient.Get(ctx, "portfolio:summary:"+accountID).Result()
	if err == nil {
		var summary PortfolioSummary
		if json.Unmarshal([]byte(cached), &summary) == nil {
			c.JSON(http.StatusOK, gin.H{"status": "success", "data": summary, "source": "cache"})
			return
		}
	}

	var totalValue, totalCost, unrealizedPnl float64
	var holdingsCount int
	err = db.QueryRow(`
		SELECT COALESCE(SUM(market_value),0), COALESCE(SUM(average_price * quantity),0),
			COALESCE(SUM(unrealized_pnl),0), COUNT(*)
		FROM holdings WHERE account_id = $1 AND quantity > 0`, accountID).
		Scan(&totalValue, &totalCost, &unrealizedPnl, &holdingsCount)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	var realizedPnl float64
	db.QueryRow("SELECT COALESCE(SUM(pnl),0) FROM realized_pnl WHERE account_id = $1", accountID).Scan(&realizedPnl)

	// Top holding
	var topSymbol string
	var topValue float64
	db.QueryRow("SELECT symbol, market_value FROM holdings WHERE account_id = $1 AND quantity > 0 ORDER BY market_value DESC LIMIT 1", accountID).
		Scan(&topSymbol, &topValue)

	pnlPercent := 0.0
	if totalCost > 0 {
		pnlPercent = (unrealizedPnl / totalCost) * 100
	}

	summary := PortfolioSummary{
		AccountID:       accountID,
		TotalValue:      math.Round(totalValue*100) / 100,
		TotalCost:       math.Round(totalCost*100) / 100,
		UnrealizedPnl:   math.Round(unrealizedPnl*100) / 100,
		RealizedPnl:     math.Round(realizedPnl*100) / 100,
		TotalPnl:        math.Round((unrealizedPnl+realizedPnl)*100) / 100,
		PnlPercent:      math.Round(pnlPercent*100) / 100,
		HoldingsCount:   holdingsCount,
		TopHolding:      topSymbol,
		TopHoldingValue: math.Round(topValue*100) / 100,
		UpdatedAt:       time.Now(),
	}

	// Cache for 30 seconds
	data, _ := json.Marshal(summary)
	redisClient.Set(ctx, "portfolio:summary:"+accountID, data, 30*time.Second)

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": summary})
}

func getHoldings(c *gin.Context) {
	accountID := c.Param("account_id")
	includeZero := c.DefaultQuery("include_zero", "false")

	query := `SELECT id, holding_id, account_id, symbol, quantity, average_price, current_price, market_value, unrealized_pnl, updated_at
		FROM holdings WHERE account_id = $1`
	if includeZero != "true" {
		query += " AND quantity > 0"
	}
	query += " ORDER BY market_value DESC"

	rows, err := db.Query(query, accountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	holdings := []Holding{}
	for rows.Next() {
		var h Holding
		rows.Scan(&h.ID, &h.HoldingID, &h.AccountID, &h.Symbol, &h.Quantity,
			&h.AveragePrice, &h.CurrentPrice, &h.MarketValue, &h.UnrealizedPnl, &h.UpdatedAt)
		holdings = append(holdings, h)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": holdings, "count": len(holdings)})
}

func getHoldingBySymbol(c *gin.Context) {
	accountID := c.Param("account_id")
	symbol := c.Param("symbol")

	var h Holding
	err := db.QueryRow(`SELECT id, holding_id, account_id, symbol, quantity, average_price, current_price, market_value, unrealized_pnl, updated_at
		FROM holdings WHERE account_id = $1 AND symbol = $2`, accountID, symbol).
		Scan(&h.ID, &h.HoldingID, &h.AccountID, &h.Symbol, &h.Quantity,
			&h.AveragePrice, &h.CurrentPrice, &h.MarketValue, &h.UnrealizedPnl, &h.UpdatedAt)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Holding not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": h})
}

func getPnL(c *gin.Context) {
	accountID := c.Param("account_id")
	limit := c.DefaultQuery("limit", "50")

	// Unrealized P&L
	rows, err := db.Query(`SELECT symbol, quantity, average_price, current_price, unrealized_pnl
		FROM holdings WHERE account_id = $1 AND quantity > 0 ORDER BY unrealized_pnl DESC`, accountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	unrealizedItems := []gin.H{}
	var totalUnrealized float64
	for rows.Next() {
		var symbol string
		var quantity, avgPrice, curPrice, pnl float64
		rows.Scan(&symbol, &quantity, &avgPrice, &curPrice, &pnl)
		pnlPct := 0.0
		if avgPrice > 0 {
			pnlPct = ((curPrice - avgPrice) / avgPrice) * 100
		}
		unrealizedItems = append(unrealizedItems, gin.H{
			"symbol":        symbol,
			"quantity":      quantity,
			"avg_price":     avgPrice,
			"current_price": curPrice,
			"pnl":           math.Round(pnl*100) / 100,
			"pnl_percent":   math.Round(pnlPct*100) / 100,
		})
		totalUnrealized += pnl
	}

	// Realized P&L
	realizedRows, err := db.Query(`SELECT id, account_id, symbol, trade_id, side, quantity, entry_price, exit_price, pnl, recorded_at
		FROM realized_pnl WHERE account_id = $1 ORDER BY recorded_at DESC LIMIT $2`, accountID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer realizedRows.Close()

	realizedItems := []PnLRecord{}
	var totalRealized float64
	for realizedRows.Next() {
		var r PnLRecord
		realizedRows.Scan(&r.ID, &r.AccountID, &r.Symbol, &r.TradeID, &r.Side,
			&r.Quantity, &r.EntryPrice, &r.ExitPrice, &r.PnL, &r.RecordedAt)
		realizedItems = append(realizedItems, r)
		totalRealized += r.PnL
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"unrealized": gin.H{
				"items": unrealizedItems,
				"total": math.Round(totalUnrealized*100) / 100,
			},
			"realized": gin.H{
				"items": realizedItems,
				"total": math.Round(totalRealized*100) / 100,
			},
			"total_pnl": math.Round((totalUnrealized+totalRealized)*100) / 100,
		},
	})
}

func getPerformance(c *gin.Context) {
	accountID := c.Param("account_id")

	// Calculate performance metrics
	var totalTrades, winningTrades, losingTrades int
	db.QueryRow("SELECT COUNT(*) FROM realized_pnl WHERE account_id = $1", accountID).Scan(&totalTrades)
	db.QueryRow("SELECT COUNT(*) FROM realized_pnl WHERE account_id = $1 AND pnl > 0", accountID).Scan(&winningTrades)
	db.QueryRow("SELECT COUNT(*) FROM realized_pnl WHERE account_id = $1 AND pnl < 0", accountID).Scan(&losingTrades)

	winRate := 0.0
	if totalTrades > 0 {
		winRate = float64(winningTrades) / float64(totalTrades) * 100
	}

	// Total return
	var totalCost, totalValue float64
	db.QueryRow("SELECT COALESCE(SUM(average_price * quantity),0), COALESCE(SUM(market_value),0) FROM holdings WHERE account_id = $1 AND quantity > 0", accountID).
		Scan(&totalCost, &totalValue)

	totalReturn := 0.0
	if totalCost > 0 {
		totalReturn = ((totalValue - totalCost) / totalCost) * 100
	}

	metrics := PerformanceMetrics{
		DayReturn:     math.Round(totalReturn/30*100) / 100, // Simplified
		WeekReturn:    math.Round(totalReturn/4*100) / 100,  // Simplified
		MonthReturn:   math.Round(totalReturn*100) / 100,
		TotalReturn:   math.Round(totalReturn*100) / 100,
		WinRate:       math.Round(winRate*100) / 100,
		TotalTrades:   totalTrades,
		WinningTrades: winningTrades,
		LosingTrades:  losingTrades,
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": metrics})
}

// Internal endpoint: update holdings after a trade
func updateHolding(c *gin.Context) {
	var req UpdateHoldingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	tx, err := db.Begin()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Transaction error"})
		return
	}

	// Check existing holding
	var existingQty, existingAvgPrice float64
	var holdingID string
	err = tx.QueryRow("SELECT holding_id, quantity, average_price FROM holdings WHERE account_id = $1 AND symbol = $2 FOR UPDATE",
		req.AccountID, req.Symbol).Scan(&holdingID, &existingQty, &existingAvgPrice)

	if err == sql.ErrNoRows {
		// New holding (buy)
		if req.Quantity < 0 {
			tx.Rollback()
			c.JSON(http.StatusBadRequest, gin.H{"error": "Cannot sell a position you don't hold"})
			return
		}
		holdingID = fmt.Sprintf("HLD-%s", time.Now().Format("20060102150405"))
		marketValue := req.Quantity * req.Price
		_, err = tx.Exec(`INSERT INTO holdings (holding_id, account_id, symbol, quantity, average_price, current_price, market_value, unrealized_pnl)
			VALUES ($1, $2, $3, $4, $5, $5, $6, 0)`,
			holdingID, req.AccountID, req.Symbol, req.Quantity, req.Price, marketValue)
		if err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create holding"})
			return
		}
	} else if err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	} else {
		// Update existing holding
		if req.Quantity > 0 {
			// Buying more: update average price
			totalCost := (existingAvgPrice * existingQty) + (req.Price * req.Quantity)
			newQty := existingQty + req.Quantity
			newAvgPrice := totalCost / newQty
			marketValue := newQty * req.Price
			unrealizedPnl := (req.Price - newAvgPrice) * newQty

			_, err = tx.Exec(`UPDATE holdings SET quantity = $1, average_price = $2, current_price = $3,
				market_value = $4, unrealized_pnl = $5, updated_at = $6 WHERE holding_id = $7`,
				newQty, math.Round(newAvgPrice*100)/100, req.Price, math.Round(marketValue*100)/100,
				math.Round(unrealizedPnl*100)/100, time.Now(), holdingID)
		} else {
			// Selling: reduce quantity and record realized P&L
			sellQty := math.Abs(req.Quantity)
			if sellQty > existingQty {
				tx.Rollback()
				c.JSON(http.StatusBadRequest, gin.H{"error": "Insufficient shares", "available": existingQty})
				return
			}

			// Record realized P&L
			realizedPnl := (req.Price - existingAvgPrice) * sellQty
			tx.Exec(`INSERT INTO realized_pnl (account_id, symbol, trade_id, side, quantity, entry_price, exit_price, pnl)
				VALUES ($1, $2, $3, 'sell', $4, $5, $6, $7)`,
				req.AccountID, req.Symbol, req.TradeID, sellQty, existingAvgPrice, req.Price, math.Round(realizedPnl*100)/100)

			newQty := existingQty - sellQty
			marketValue := newQty * req.Price
			unrealizedPnl := (req.Price - existingAvgPrice) * newQty

			_, err = tx.Exec(`UPDATE holdings SET quantity = $1, current_price = $2,
				market_value = $3, unrealized_pnl = $4, updated_at = $5 WHERE holding_id = $6`,
				newQty, req.Price, math.Round(marketValue*100)/100,
				math.Round(unrealizedPnl*100)/100, time.Now(), holdingID)
		}
		if err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update holding"})
			return
		}
	}

	tx.Commit()
	portfolioUpdates.Inc()

	// Invalidate cache
	redisClient.Del(ctx, "portfolio:summary:"+req.AccountID)

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Holding updated", "holding_id": holdingID})
}

func refreshPortfolio(c *gin.Context) {
	accountID := c.Param("account_id")

	rows, err := db.Query("SELECT symbol FROM holdings WHERE account_id = $1 AND quantity > 0", accountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	updated := 0
	for rows.Next() {
		var symbol string
		rows.Scan(&symbol)

		priceStr, err := redisClient.Get(ctx, "market:price:"+symbol).Result()
		if err != nil {
			continue
		}
		var price float64
		fmt.Sscanf(priceStr, "%f", &price)
		if price > 0 {
			db.Exec(`UPDATE holdings SET current_price = $1, market_value = quantity * $1,
				unrealized_pnl = (($1 - average_price) * quantity), updated_at = $2
				WHERE account_id = $3 AND symbol = $4`, price, time.Now(), accountID, symbol)
			updated++
		}
	}

	redisClient.Del(ctx, "portfolio:summary:"+accountID)
	c.JSON(http.StatusOK, gin.H{"status": "success", "message": fmt.Sprintf("Refreshed %d holdings", updated)})
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func healthCheck(c *gin.Context) {
	health := gin.H{
		"status":  "UP",
		"service": "tradehub-portfolio-service",
		"port":    "8206",
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
