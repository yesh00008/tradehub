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
)

// ─── Global
variables ────────────────────────────────────────────────────────

var (
	db          *sql.DB
	redisClient *redis.Client
	ctx         = context.Background()

	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tradehub_trading_account_requests_total",
			Help: "Total requests to Trading Account Service",
		},
		[]string{"method", "endpoint", "status"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "tradehub_trading_account_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)
	depositsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tradehub_deposits_total",
			Help: "Total deposits processed",
		},
		[]string{"status"},
	)
	withdrawalsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tradehub_withdrawals_total",
			Help: "Total withdrawals processed",
		},
		[]string{"status"},
	)
	totalBalance = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "tradehub_total_cash_balance",
			Help: "Total cash balance across all accounts",
		},
	)
)

func init() {
	prometheus.MustRegister(requestsTotal)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(depositsTotal)
	prometheus.MustRegister(withdrawalsTotal)
	prometheus.MustRegister(totalBalance)
}

// ─── Models ──────────────────────────────────────────────────────────────────

type TradingAccount struct {
	ID             int       `json:"id"`
	AccountID      string    `json:"account_id"`
	TraderID       string    `json:"trader_id"`
	AccountNumber  string    `json:"account_number"`
	AccountType    string    `json:"account_type"`
	CashBalance    float64   `json:"cash_balance"`
	BuyingPower    float64   `json:"buying_power"`
	PortfolioValue float64   `json:"portfolio_value"`
	MarginUsed     float64   `json:"margin_used"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type CreateAccountRequest struct {
	TraderID    string `json:"trader_id" binding:"required"`
	AccountType string `json:"account_type" binding:"required"` // cash or margin
}

type DepositRequest struct {
	Amount      float64 `json:"amount" binding:"required"`
	Description string  `json:"description"`
}

type WithdrawRequest struct {
	Amount      float64 `json:"amount" binding:"required"`
	Description string  `json:"description"`
}

type Transaction struct {
	ID            int       `json:"id"`
	TransactionID string    `json:"transaction_id"`
	AccountID     string    `json:"account_id"`
	Type          string    `json:"type"`
	Amount        float64   `json:"amount"`
	BalanceBefore float64   `json:"balance_before"`
	BalanceAfter  float64   `json:"balance_after"`
	Description   string    `json:"description"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	log.Println("🚀 TradeHub Trading Account Service starting...")

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

	// Gin - with request logging
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.Use(prometheusMiddleware())

	// Health & metrics
	router.GET("/health", healthCheck)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	router.GET("/api/v1/ping",
func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong", "service": "trading-account-service"})
	})

	// API routes
	v1 := router.Group("/api/v1")
	{
		v1.POST("/accounts", createAccount)
		v1.GET("/accounts", listAccounts)
		v1.GET("/accounts/:id", getAccount)
		v1.GET("/accounts/:id/balance", getBalance)
		v1.POST("/accounts/:id/deposit", deposit)
		v1.POST("/accounts/:id/withdraw", withdraw)
		v1.GET("/accounts/:id/transactions", getTransactions)
		v1.PUT("/accounts/:id/status", updateAccountStatus)
		v1.GET("/accounts/trader/:trader_id", getAccountsByTrader)
		// Internal endpoints for other services
		v1.POST("/accounts/:id/adjust-balance", adjustBalance)
	}

	// Start server
	port := getEnv("PORT", "8202")
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go
func() {
		log.Printf("🚀 Trading Account Service running on port %s\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down Trading Account Service...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
	log.Println("Trading Account Service exited")
}

// ─── Database Initialization ─────────────────────────────────────────────────

func initializeTables() {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS trading_accounts (
			id SERIAL PRIMARY KEY,
			account_id
varCHAR(50) UNIQUE NOT NULL,
			trader_id
varCHAR(50) NOT NULL,
			account_number
varCHAR(20) UNIQUE NOT NULL,
			account_type
varCHAR(20) NOT NULL DEFAULT 'cash',
			cash_balance DECIMAL(18,2) DEFAULT 0.00,
			buying_power DECIMAL(18,2) DEFAULT 0.00,
			portfolio_value DECIMAL(18,2) DEFAULT 0.00,
			margin_used DECIMAL(18,2) DEFAULT 0.00,
			status
varCHAR(20) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT NOW(),
			updated_at TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS account_transactions (
			id SERIAL PRIMARY KEY,
			transaction_id
varCHAR(50) UNIQUE NOT NULL,
			account_id
varCHAR(50) NOT NULL,
			type
varCHAR(20) NOT NULL,
			amount DECIMAL(18,2) NOT NULL,
			balance_before DECIMAL(18,2) NOT NULL,
			balance_after DECIMAL(18,2) NOT NULL,
			description TEXT DEFAULT '',
			status
varCHAR(20) DEFAULT 'completed',
			created_at TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_trading_accounts_account_id ON trading_accounts(account_id)`,
		`CREATE INDEX IF NOT EXISTS idx_trading_accounts_trader_id ON trading_accounts(trader_id)`,
		`CREATE INDEX IF NOT EXISTS idx_account_transactions_account_id ON account_transactions(account_id)`,
	}

	if db != nil {
		for _, q := range queries {
			if _, err := db.Exec(q); err != nil {
				log.Println("⚠ Table creation error:", err)
			}
		}
		log.Println("✓ Trading account tables ready")
		seedAccounts()
	}
}

func seedAccounts() {
	var count int
	db.QueryRow("SELECT COUNT(*) FROM trading_accounts").Scan(&count)
	if count > 0 {
		return
	}

	accounts := []struct {
		traderID    string
		accountType string
		balance     float64
	}{
		{"TRD-SEED0001", "cash", 100000.00},
		{"TRD-SEED0001", "margin", 250000.00},
		{"TRD-SEED0002", "cash", 50000.00},
		{"TRD-SEED0003", "cash", 75000.00},
		{"TRD-SEED0004", "margin", 500000.00},
	}

	for i, a := range accounts {
		accountID := fmt.Sprintf("ACC-%s", uuid.New().String()[:8])
		accountNumber := fmt.Sprintf("TH%08d", 10000001+i)
		buyingPower := a.balance
		if a.accountType == "margin" {
			buyingPower = a.balance * 2 // 2x margin
		}
		db.Exec(`INSERT INTO trading_accounts (account_id, trader_id, account_number, account_type, cash_balance, buying_power, status)
				 VALUES ($1, $2, $3, $4, $5, $6, 'active')`,
			accountID, a.traderID, accountNumber, a.accountType, a.balance, buyingPower)
	}
	log.Println("✓ Seed trading accounts inserted")
}

// ─── Handlers ────────────────────────────────────────────────────────────────

func createAccount(c *gin.Context) {
	var req CreateAccountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	if req.AccountType != "cash" && req.AccountType != "margin" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Account type must be 'cash' or 'margin'"})
		return
	}

	// Check existing accounts for this trader with same type
	var exists bool
	db.QueryRow("SELECT EXISTS(SELECT 1 FROM trading_accounts WHERE trader_id = $1 AND account_type = $2 AND status = 'active')",
		req.TraderID, req.AccountType).Scan(&exists)
	if exists {
		c.JSON(http.StatusConflict, gin.H{"error": fmt.Sprintf("Trader already has an active %s account", req.AccountType)})
		return
	}

	accountID := "ACC-" + uuid.New().String()[:8]
	// Generate unique account number
	var maxNum int
	db.QueryRow("SELECT COALESCE(MAX(CAST(SUBSTRING(account_number FROM 3) AS INTEGER)), 10000000) FROM trading_accounts").Scan(&maxNum)
	accountNumber := fmt.Sprintf("TH%08d", maxNum+1)

	var account TradingAccount
	err := db.QueryRow(`
		INSERT INTO trading_accounts (account_id, trader_id, account_number, account_type, cash_balance, buying_power, portfolio_value, margin_used, status)
		VALUES ($1, $2, $3, $4, 0.00, 0.00, 0.00, 0.00, 'active')
		RETURNING id, account_id, trader_id, account_number, account_type, cash_balance, buying_power, portfolio_value, margin_used, status, created_at, updated_at`,
		accountID, req.TraderID, accountNumber, req.AccountType,
	).Scan(&account.ID, &account.AccountID, &account.TraderID, &account.AccountNumber,
		&account.AccountType, &account.CashBalance, &account.BuyingPower,
		&account.PortfolioValue, &account.MarginUsed, &account.Status,
		&account.CreatedAt, &account.UpdatedAt)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create account", "details": err.Error()})
		return
	}

	cacheAccount(account)
	c.JSON(http.StatusCreated, gin.H{"status": "success", "data": account})
}

func getAccount(c *gin.Context) {
	accountID := c.Param("id")

	// Try cache
	cached, err := redisClient.Get(ctx, "account:"+accountID).Result()
	if err == nil {
		var account TradingAccount
		if json.Unmarshal([]byte(cached), &account) == nil {
			c.JSON(http.StatusOK, gin.H{"status": "success", "data": account, "source": "cache"})
			return
		}
	}

	account, err := fetchAccount(accountID)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Account not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	cacheAccount(account)
	c.JSON(http.StatusOK, gin.H{"status": "success", "data": account})
}

func listAccounts(c *gin.Context) {
	status := c.DefaultQuery("status", "")
	accountType := c.DefaultQuery("type", "")
	limit := c.DefaultQuery("limit", "50")
	offset := c.DefaultQuery("offset", "0")

	query := `SELECT id, account_id, trader_id, account_number, account_type, cash_balance, buying_power, portfolio_value, margin_used, status, created_at, updated_at
			  FROM trading_accounts WHERE 1=1`
	args := []interface{}{}
	argIdx := 1

	if status != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}
	if accountType != "" {
		query += fmt.Sprintf(" AND account_type = $%d", argIdx)
		args = append(args, accountType)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
	args = append(args, limit, offset)

	rows, err := db.Query(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	accounts := []TradingAccount{}
	for rows.Next() {
		var a TradingAccount
		rows.Scan(&a.ID, &a.AccountID, &a.TraderID, &a.AccountNumber,
			&a.AccountType, &a.CashBalance, &a.BuyingPower,
			&a.PortfolioValue, &a.MarginUsed, &a.Status,
			&a.CreatedAt, &a.UpdatedAt)
		accounts = append(accounts, a)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": accounts})
}

func getBalance(c *gin.Context) {
	accountID := c.Param("id")
	account, err := fetchAccount(accountID)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Account not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	totalValue := account.CashBalance + account.PortfolioValue

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"account_id":      account.AccountID,
			"cash_balance":    account.CashBalance,
			"buying_power":    account.BuyingPower,
			"portfolio_value": account.PortfolioValue,
			"margin_used":     account.MarginUsed,
			"total_value":     math.Round(totalValue*100) / 100,
			"account_type":    account.AccountType,
		},
	})
}

func deposit(c *gin.Context) {
	accountID := c.Param("id")
	var req DepositRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	if req.Amount <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Deposit amount must be positive"})
		return
	}
	if req.Amount > 1000000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Deposit amount exceeds maximum of $1,000,000"})
		return
	}

	// Use transaction for atomicity
	tx, err := db.Begin()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Transaction error"})
		return
	}

	var currentBalance float64
	var acctStatus, acctType string
	err = tx.QueryRow("SELECT cash_balance, status, account_type FROM trading_accounts WHERE account_id = $1 FOR UPDATE", accountID).
		Scan(&currentBalance, &acctStatus, &acctType)
	if err == sql.ErrNoRows {
		tx.Rollback()
		c.JSON(http.StatusNotFound, gin.H{"error": "Account not found"})
		return
	}
	if acctStatus != "active" {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{"error": "Account is not active"})
		return
	}

	newBalance := math.Round((currentBalance+req.Amount)*100) / 100
	buyingPower := newBalance
	if acctType == "margin" {
		buyingPower = newBalance * 2
	}

	_, err = tx.Exec(`UPDATE trading_accounts SET cash_balance = $1, buying_power = $2, updated_at = $3 WHERE account_id = $4`,
		newBalance, buyingPower, time.Now(), accountID)
	if err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update balance"})
		return
	}

	// Record transaction
	txnID := "TXN-" + uuid.New().String()[:8]
	description := req.Description
	if description == "" {
		description = "Cash deposit"
	}
	_, err = tx.Exec(`INSERT INTO account_transactions (transaction_id, account_id, type, amount, balance_before, balance_after, description, status)
		VALUES ($1, $2, 'deposit', $3, $4, $5, $6, 'completed')`,
		txnID, accountID, req.Amount, currentBalance, newBalance, description)
	if err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to record transaction"})
		return
	}

	tx.Commit()
	depositsTotal.WithLabelValues("success").Inc()

	// Invalidate cache
	redisClient.Del(ctx, "account:"+accountID)

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"transaction_id": txnID,
			"account_id":     accountID,
			"type":           "deposit",
			"amount":         req.Amount,
			"balance_before": currentBalance,
			"balance_after":  newBalance,
			"buying_power":   buyingPower,
		},
	})
}

func withdraw(c *gin.Context) {
	accountID := c.Param("id")
	var req WithdrawRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	if req.Amount <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Withdrawal amount must be positive"})
		return
	}

	tx, err := db.Begin()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Transaction error"})
		return
	}

	var currentBalance float64
	var acctStatus, acctType string
	err = tx.QueryRow("SELECT cash_balance, status, account_type FROM trading_accounts WHERE account_id = $1 FOR UPDATE", accountID).
		Scan(&currentBalance, &acctStatus, &acctType)
	if err == sql.ErrNoRows {
		tx.Rollback()
		c.JSON(http.StatusNotFound, gin.H{"error": "Account not found"})
		return
	}
	if acctStatus != "active" {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{"error": "Account is not active"})
		return
	}
	if req.Amount > currentBalance {
		tx.Rollback()
		withdrawalsTotal.WithLabelValues("insufficient_funds").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": "Insufficient funds", "available_balance": currentBalance})
		return
	}

	newBalance := math.Round((currentBalance-req.Amount)*100) / 100
	buyingPower := newBalance
	if acctType == "margin" {
		buyingPower = newBalance * 2
	}

	_, err = tx.Exec(`UPDATE trading_accounts SET cash_balance = $1, buying_power = $2, updated_at = $3 WHERE account_id = $4`,
		newBalance, buyingPower, time.Now(), accountID)
	if err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update balance"})
		return
	}

	txnID := "TXN-" + uuid.New().String()[:8]
	description := req.Description
	if description == "" {
		description = "Cash withdrawal"
	}
	_, err = tx.Exec(`INSERT INTO account_transactions (transaction_id, account_id, type, amount, balance_before, balance_after, description, status)
		VALUES ($1, $2, 'withdrawal', $3, $4, $5, $6, 'completed')`,
		txnID, accountID, req.Amount, currentBalance, newBalance, description)
	if err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to record transaction"})
		return
	}

	tx.Commit()
	withdrawalsTotal.WithLabelValues("success").Inc()
	redisClient.Del(ctx, "account:"+accountID)

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"transaction_id": txnID,
			"account_id":     accountID,
			"type":           "withdrawal",
			"amount":         req.Amount,
			"balance_before": currentBalance,
			"balance_after":  newBalance,
			"buying_power":   buyingPower,
		},
	})
}

func getTransactions(c *gin.Context) {
	accountID := c.Param("id")
	limit := c.DefaultQuery("limit", "50")
	offset := c.DefaultQuery("offset", "0")
	txnType := c.DefaultQuery("type", "")

	query := `SELECT id, transaction_id, account_id, type, amount, balance_before, balance_after, description, status, created_at
			  FROM account_transactions WHERE account_id = $1`
	args := []interface{}{accountID}
	argIdx := 2

	if txnType != "" {
		query += fmt.Sprintf(" AND type = $%d", argIdx)
		args = append(args, txnType)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
	args = append(args, limit, offset)

	rows, err := db.Query(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	txns := []Transaction{}
	for rows.Next() {
		var t Transaction
		rows.Scan(&t.ID, &t.TransactionID, &t.AccountID, &t.Type, &t.Amount,
			&t.BalanceBefore, &t.BalanceAfter, &t.Description, &t.Status, &t.CreatedAt)
		txns = append(txns, t)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": txns})
}

func getAccountsByTrader(c *gin.Context) {
	traderID := c.Param("trader_id")

	rows, err := db.Query(`SELECT id, account_id, trader_id, account_number, account_type, cash_balance, buying_power, portfolio_value, margin_used, status, created_at, updated_at
		FROM trading_accounts WHERE trader_id = $1 ORDER BY created_at DESC`, traderID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	accounts := []TradingAccount{}
	for rows.Next() {
		var a TradingAccount
		rows.Scan(&a.ID, &a.AccountID, &a.TraderID, &a.AccountNumber,
			&a.AccountType, &a.CashBalance, &a.BuyingPower,
			&a.PortfolioValue, &a.MarginUsed, &a.Status,
			&a.CreatedAt, &a.UpdatedAt)
		accounts = append(accounts, a)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": accounts})
}

func updateAccountStatus(c *gin.Context) {
	accountID := c.Param("id")
	var req struct {
		Status string `json:"status" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	validStatuses := map[string]bool{"active": true, "suspended": true, "closed": true}
	if !validStatuses[req.Status] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid status. Must be: active, suspended, closed"})
		return
	}

	result, err := db.Exec("UPDATE trading_accounts SET status = $1, updated_at = $2 WHERE account_id = $3", req.Status, time.Now(), accountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update status"})
		return
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Account not found"})
		return
	}

	redisClient.Del(ctx, "account:"+accountID)
	c.JSON(http.StatusOK, gin.H{"status": "success", "message": fmt.Sprintf("Account status updated to %s", req.Status)})
}

// adjustBalance is an internal endpoint used by execution engine / clearing service
func adjustBalance(c *gin.Context) {
	accountID := c.Param("id")
	var req struct {
		Amount      float64 `json:"amount" binding:"required"` // positive = credit, negative = debit
		Type        string  `json:"type" binding:"required"`   // trade_buy, trade_sell, settlement, commission
		Description string  `json:"description"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	tx, err := db.Begin()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Transaction error"})
		return
	}

	var currentBalance float64
	var acctType string
	err = tx.QueryRow("SELECT cash_balance, account_type FROM trading_accounts WHERE account_id = $1 FOR UPDATE", accountID).
		Scan(&currentBalance, &acctType)
	if err != nil {
		tx.Rollback()
		c.JSON(http.StatusNotFound, gin.H{"error": "Account not found"})
		return
	}

	newBalance := math.Round((currentBalance+req.Amount)*100) / 100
	if newBalance < 0 {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{"error": "Insufficient funds for adjustment"})
		return
	}

	buyingPower := newBalance
	if acctType == "margin" {
		buyingPower = newBalance * 2
	}

	tx.Exec(`UPDATE trading_accounts SET cash_balance = $1, buying_power = $2, updated_at = $3 WHERE account_id = $4`,
		newBalance, buyingPower, time.Now(), accountID)

	txnID := "TXN-" + uuid.New().String()[:8]
	tx.Exec(`INSERT INTO account_transactions (transaction_id, account_id, type, amount, balance_before, balance_after, description, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'completed')`,
		txnID, accountID, req.Type, req.Amount, currentBalance, newBalance, req.Description)

	tx.Commit()
	redisClient.Del(ctx, "account:"+accountID)

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"transaction_id": txnID,
			"balance_before": currentBalance,
			"balance_after":  newBalance,
			"buying_power":   buyingPower,
		},
	})
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func fetchAccount(accountID string) (TradingAccount, error) {
	var a TradingAccount
	err := db.QueryRow(`SELECT id, account_id, trader_id, account_number, account_type, cash_balance, buying_power, portfolio_value, margin_used, status, created_at, updated_at
		FROM trading_accounts WHERE account_id = $1`, accountID).
		Scan(&a.ID, &a.AccountID, &a.TraderID, &a.AccountNumber,
			&a.AccountType, &a.CashBalance, &a.BuyingPower,
			&a.PortfolioValue, &a.MarginUsed, &a.Status,
			&a.CreatedAt, &a.UpdatedAt)
	return a, err
}

func cacheAccount(account TradingAccount) {
	data, _ := json.Marshal(account)
	redisClient.Set(ctx, "account:"+account.AccountID, data, 5*time.Minute)
}

func healthCheck(c *gin.Context) {
	health := gin.H{
		"status":  "UP",
		"service": "tradehub-trading-account-service",
		"port":    "8202",
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
