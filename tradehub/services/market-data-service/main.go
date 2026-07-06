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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
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

	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:    
func(r *http.Request) bool {
return true
}
,
	}

	wsClients   = make(map[*websocket.Conn]map[string]bool) // conn -> subscribed symbols
	wsClientsMu sync.RWMutex

	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tradehub_marketdata_requests_total",
			Help: "Total requests to Market Data Service",
		},
		[]string{"method", "endpoint", "status"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "tradehub_marketdata_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)
	wsConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "tradehub_websocket_connections",
			Help: "Active WebSocket connections",
		},
	)
	quotesGenerated = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "tradehub_quotes_generated_total",
			Help: "Total quotes generated",
		},
	)
)

func init() {
	prometheus.MustRegister(requestsTotal)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(wsConnections)
	prometheus.MustRegister(quotesGenerated)
}

// ─── Models ──────────────────────────────────────────────────────────────────

type Security struct {
	Symbol       string  `json:"symbol"`
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	Exchange     string  `json:"exchange"`
	CurrentPrice float64 `json:"current_price"`
}

type MarketDataPoint struct {
	ID        int       `json:"id"`
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    int64     `json:"volume"`
	Timestamp time.Time `json:"timestamp"`
}

type Quote struct {
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price"`
	Bid       float64 `json:"bid"`
	Ask       float64 `json:"ask"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Volume    int64   `json:"volume"`
	Change    float64 `json:"change"`
	ChangePct float64 `json:"change_pct"`
	Timestamp string  `json:"timestamp"`
}

// Securities universe
var securities = map[string]Security{
	"AAPL":  {Symbol: "AAPL", Name: "Apple Inc.", Type: "stock", Exchange: "NASDAQ", CurrentPrice: 178.50},
	"GOOGL": {Symbol: "GOOGL", Name: "Alphabet Inc.", Type: "stock", Exchange: "NASDAQ", CurrentPrice: 141.80},
	"MSFT":  {Symbol: "MSFT", Name: "Microsoft Corp.", Type: "stock", Exchange: "NASDAQ", CurrentPrice: 378.90},
	"AMZN":  {Symbol: "AMZN", Name: "Amazon.com Inc.", Type: "stock", Exchange: "NASDAQ", CurrentPrice: 178.25},
	"TSLA":  {Symbol: "TSLA", Name: "Tesla Inc.", Type: "stock", Exchange: "NASDAQ", CurrentPrice: 248.50},
	"META":  {Symbol: "META", Name: "Meta Platforms Inc.", Type: "stock", Exchange: "NASDAQ", CurrentPrice: 505.75},
	"NVDA":  {Symbol: "NVDA", Name: "NVIDIA Corp.", Type: "stock", Exchange: "NASDAQ", CurrentPrice: 875.30},
	"JPM":   {Symbol: "JPM", Name: "JPMorgan Chase & Co.", Type: "stock", Exchange: "NYSE", CurrentPrice: 196.40},
	"V":     {Symbol: "V", Name: "Visa Inc.", Type: "stock", Exchange: "NYSE", CurrentPrice: 279.15},
	"JNJ":   {Symbol: "JNJ", Name: "Johnson & Johnson", Type: "stock", Exchange: "NYSE", CurrentPrice: 156.80},
	"WMT":   {Symbol: "WMT", Name: "Walmart Inc.", Type: "stock", Exchange: "NYSE", CurrentPrice: 165.30},
	"PG":    {Symbol: "PG", Name: "Procter & Gamble Co.", Type: "stock", Exchange: "NYSE", CurrentPrice: 160.25},
	"UNH":   {Symbol: "UNH", Name: "UnitedHealth Group", Type: "stock", Exchange: "NYSE", CurrentPrice: 527.40},
	"HD":    {Symbol: "HD", Name: "Home Depot Inc.", Type: "stock", Exchange: "NYSE", CurrentPrice: 370.80},
	"MA":    {Symbol: "MA", Name: "Mastercard Inc.", Type: "stock", Exchange: "NYSE", CurrentPrice: 458.60},
	"DIS":   {Symbol: "DIS", Name: "Walt Disney Co.", Type: "stock", Exchange: "NYSE", CurrentPrice: 112.45},
	"BAC":   {Symbol: "BAC", Name: "Bank of America Corp.", Type: "stock", Exchange: "NYSE", CurrentPrice: 34.60},
	"NFLX":  {Symbol: "NFLX", Name: "Netflix Inc.", Type: "stock", Exchange: "NASDAQ", CurrentPrice: 605.20},
	"ADBE":  {Symbol: "ADBE", Name: "Adobe Inc.", Type: "stock", Exchange: "NASDAQ", CurrentPrice: 570.15},
	"CRM":   {Symbol: "CRM", Name: "Salesforce Inc.", Type: "stock", Exchange: "NYSE", CurrentPrice: 275.80},
	"AMD":   {Symbol: "AMD", Name: "AMD Inc.", Type: "stock", Exchange: "NASDAQ", CurrentPrice: 175.40},
	"INTC":  {Symbol: "INTC", Name: "Intel Corp.", Type: "stock", Exchange: "NASDAQ", CurrentPrice: 43.25},
	"PYPL":  {Symbol: "PYPL", Name: "PayPal Holdings", Type: "stock", Exchange: "NASDAQ", CurrentPrice: 63.80},
	"COIN":  {Symbol: "COIN", Name: "Coinbase Global", Type: "stock", Exchange: "NASDAQ", CurrentPrice: 235.60},
	"SQ":    {Symbol: "SQ", Name: "Block Inc.", Type: "stock", Exchange: "NYSE", CurrentPrice: 78.90},
}

var (
	dayOpens   = map[string]float64{}
	dayHighs   = map[string]float64{}
	dayLows    = map[string]float64{}
	dayVolumes = map[string]int64{}
	priceMu    sync.RWMutex
)

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	log.Println("🚀 TradeHub Market Data Service starting...")

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

	// Initialize tables and data
	initializeTables()
	initializeDayData()

	// Start price simulation and broadcasting
	go simulatePrices()
	go broadcastQuotes()

	// Gin - with request logging
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.Use(prometheusMiddleware())

	// Health & metrics
	router.GET("/health", healthCheck)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	router.GET("/api/v1/ping",
func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong", "service": "market-data-service"})
	})

	// API routes
	v1 := router.Group("/api/v1")
	{
		v1.GET("/quotes/:symbol", getQuote)
		v1.GET("/quotes", getAllQuotes)
		v1.GET("/securities", listSecurities)
		v1.GET("/securities/:symbol", getSecurityInfo)
		v1.GET("/history/:symbol", getHistory)
		v1.GET("/history/:symbol/ohlcv", getOHLCV)
		v1.GET("/movers", getMovers)
		v1.GET("/search", searchSecurities)
		// WebSocket
		v1.GET("/ws/quotes", wsQuotes)
	}

	// Start server
	port := getEnv("PORT", "8207")
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go
func() {
		log.Printf("🚀 Market Data Service running on port %s\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down Market Data Service...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
	log.Println("Market Data Service exited")
}

// ─── Database Initialization ─────────────────────────────────────────────────

func initializeTables() {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS market_data (
			id SERIAL PRIMARY KEY,
			symbol
varCHAR(20) NOT NULL,
			price DECIMAL(18,4) NOT NULL,
			open DECIMAL(18,4) NOT NULL,
			high DECIMAL(18,4) NOT NULL,
			low DECIMAL(18,4) NOT NULL,
			close DECIMAL(18,4) NOT NULL,
			volume BIGINT DEFAULT 0,
			timestamp TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS securities (
			symbol
varCHAR(20) PRIMARY KEY,
			name
varCHAR(255) NOT NULL,
			type
varCHAR(20) DEFAULT 'stock',
			exchange
varCHAR(20) DEFAULT 'NASDAQ',
			current_price DECIMAL(18,4) DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_market_data_symbol ON market_data(symbol)`,
		`CREATE INDEX IF NOT EXISTS idx_market_data_timestamp ON market_data(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_market_data_symbol_timestamp ON market_data(symbol, timestamp)`,
	}

	if db != nil {
		for _, q := range queries {
			if _, err := db.Exec(q); err != nil {
				log.Println("⚠ Table creation error:", err)
			}
		}
		log.Println("✓ Market data tables ready")
		seedSecurities()
		seedHistoricalData()
	}
}

func seedSecurities() {
	for _, s := range securities {
		db.Exec(`INSERT INTO securities (symbol, name, type, exchange, current_price)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT (symbol) DO UPDATE SET current_price = $5`,
			s.Symbol, s.Name, s.Type, s.Exchange, s.CurrentPrice)
	}
	log.Println("✓ Securities seeded")
}

func seedHistoricalData() {
	var count int
	db.QueryRow("SELECT COUNT(*) FROM market_data").Scan(&count)
	if count > 100 {
		return
	}

	// Generate 30 days of historical OHLCV data for each security
	for symbol, sec := range securities {
		basePrice := sec.CurrentPrice
		for day := 30; day >= 1; day-- {
			t := time.Now().AddDate(0, 0, -day)
			open := basePrice * (1 + (rand.Float64()*0.04 - 0.02))
			close := open * (1 + (rand.Float64()*0.04 - 0.02))
			high := math.Max(open, close) * (1 + rand.Float64()*0.02)
			low := math.Min(open, close) * (1 - rand.Float64()*0.02)
			volume := int64(500000 + rand.Intn(5000000))

			db.Exec(`INSERT INTO market_data (symbol, price, open, high, low, close, volume, timestamp)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				symbol, close, round2(open), round2(high), round2(low), round2(close), volume, t)

			basePrice = close
		}
	}
	log.Println("✓ Historical market data seeded")
}

func initializeDayData() {
	priceMu.Lock()
	defer priceMu.Unlock()
	for symbol, sec := range securities {
		dayOpens[symbol] = sec.CurrentPrice
		dayHighs[symbol] = sec.CurrentPrice
		dayLows[symbol] = sec.CurrentPrice
		dayVolumes[symbol] = 0
	}
}

// ─── Price Simulation ────────────────────────────────────────────────────────

func simulatePrices() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		priceMu.Lock()
		for symbol, sec := range securities {
			// Random walk: ±0.3%
			change := sec.CurrentPrice * (rand.Float64()*0.006 - 0.003)
			newPrice := round2(sec.CurrentPrice + change)
			if newPrice <= 0 {
				newPrice = sec.CurrentPrice
			}

			sec.CurrentPrice = newPrice
			securities[symbol] = sec

			// Update day stats
			if newPrice > dayHighs[symbol] {
				dayHighs[symbol] = newPrice
			}
			if newPrice < dayLows[symbol] {
				dayLows[symbol] = newPrice
			}
			dayVolumes[symbol] += int64(rand.Intn(10000))

			// Update Redis
			redisClient.Set(ctx, "market:price:"+symbol, newPrice, 0)
		}
		priceMu.Unlock()

		quotesGenerated.Inc()

		// Store snapshot every 60 seconds
		if time.Now().Second() == 0 {
			storeSnapshot()
		}
	}
}

func storeSnapshot() {
	priceMu.RLock()
	defer priceMu.RUnlock()

	for symbol, sec := range securities {
		db.Exec(`INSERT INTO market_data (symbol, price, open, high, low, close, volume, timestamp)
			VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())`,
			symbol, sec.CurrentPrice, dayOpens[symbol], dayHighs[symbol], dayLows[symbol], sec.CurrentPrice, dayVolumes[symbol])

		db.Exec("UPDATE securities SET current_price = $1 WHERE symbol = $2", sec.CurrentPrice, symbol)
	}
}

// ─── WebSocket Broadcasting ──────────────────────────────────────────────────

func broadcastQuotes() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		wsClientsMu.RLock()
		if len(wsClients) == 0 {
			wsClientsMu.RUnlock()
			continue
		}

		priceMu.RLock()
		for conn, symbols := range wsClients {
			for symbol := range symbols {
				sec, exists := securities[symbol]
				if !exists {
					continue
				}

				quote := buildQuote(symbol, sec)
				data, _ := json.Marshal(quote)
				if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
					// Will be cleaned up by the read goroutine
					continue
				}
			}
		}
		priceMu.RUnlock()
		wsClientsMu.RUnlock()
	}
}

func buildQuote(symbol string, sec Security) Quote {
	open := dayOpens[symbol]
	change := round2(sec.CurrentPrice - open)
	changePct := 0.0
	if open > 0 {
		changePct = round2((change / open) * 100)
	}
	spread := sec.CurrentPrice * 0.001 // 0.1% spread

	return Quote{
		Symbol:    symbol,
		Price:     sec.CurrentPrice,
		Bid:       round2(sec.CurrentPrice - spread/2),
		Ask:       round2(sec.CurrentPrice + spread/2),
		Open:      open,
		High:      dayHighs[symbol],
		Low:       dayLows[symbol],
		Volume:    dayVolumes[symbol],
		Change:    change,
		ChangePct: changePct,
		Timestamp: time.Now().Format(time.RFC3339),
	}
}

// ─── Handlers ────────────────────────────────────────────────────────────────

func getQuote(c *gin.Context) {
	symbol := strings.ToUpper(c.Param("symbol"))

	priceMu.RLock()
	sec, exists := securities[symbol]
	if !exists {
		priceMu.RUnlock()
		c.JSON(http.StatusNotFound, gin.H{"error": "Symbol not found"})
		return
	}
	quote := buildQuote(symbol, sec)
	priceMu.RUnlock()

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": quote})
}

func getAllQuotes(c *gin.Context) {
	priceMu.RLock()
	quotes := []Quote{}
	for symbol, sec := range securities {
		quotes = append(quotes, buildQuote(symbol, sec))
	}
	priceMu.RUnlock()

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": quotes, "count": len(quotes)})
}

func listSecurities(c *gin.Context) {
	exchange := c.DefaultQuery("exchange", "")
	secType := c.DefaultQuery("type", "")

	priceMu.RLock()
	result := []Security{}
	for _, sec := range securities {
		if exchange != "" && sec.Exchange != strings.ToUpper(exchange) {
			continue
		}
		if secType != "" && sec.Type != secType {
			continue
		}
		result = append(result, sec)
	}
	priceMu.RUnlock()

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": result, "count": len(result)})
}

func getSecurityInfo(c *gin.Context) {
	symbol := strings.ToUpper(c.Param("symbol"))

	priceMu.RLock()
	sec, exists := securities[symbol]
	priceMu.RUnlock()

	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Security not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": sec})
}

func getHistory(c *gin.Context) {
	symbol := strings.ToUpper(c.Param("symbol"))
	days := c.DefaultQuery("days", "30")
	limit := c.DefaultQuery("limit", "100")

	rows, err := db.Query(`SELECT id, symbol, price, open, high, low, close, volume, timestamp
		FROM market_data WHERE symbol = $1 AND timestamp >= NOW() - ($2 || ' days')::INTERVAL
		ORDER BY timestamp DESC LIMIT $3`, symbol, days, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error", "details": err.Error()})
		return
	}
	defer rows.Close()

	data := []MarketDataPoint{}
	for rows.Next() {
		var d MarketDataPoint
		rows.Scan(&d.ID, &d.Symbol, &d.Price, &d.Open, &d.High, &d.Low, &d.Close, &d.Volume, &d.Timestamp)
		data = append(data, d)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": data, "count": len(data)})
}

func getOHLCV(c *gin.Context) {
	symbol := strings.ToUpper(c.Param("symbol"))
	interval := c.DefaultQuery("interval", "1d") // 1d, 1h
	limit := c.DefaultQuery("limit", "30")

	var groupBy string
	switch interval {
	case "1h":
		groupBy = "date_trunc('hour', timestamp)"
	default:
		groupBy = "date_trunc('day', timestamp)"
	}

	query := fmt.Sprintf(`SELECT %s AS period,
		(ARRAY_AGG(open ORDER BY timestamp ASC))[1] AS open,
		MAX(high) AS high, MIN(low) AS low,
		(ARRAY_AGG(close ORDER BY timestamp DESC))[1] AS close,
		SUM(volume) AS volume
		FROM market_data WHERE symbol = $1
		GROUP BY period ORDER BY period DESC LIMIT $2`, groupBy)

	rows, err := db.Query(query, symbol, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error", "details": err.Error()})
		return
	}
	defer rows.Close()

	type OHLCV struct {
		Period string  `json:"period"`
		Open   float64 `json:"open"`
		High   float64 `json:"high"`
		Low    float64 `json:"low"`
		Close  float64 `json:"close"`
		Volume int64   `json:"volume"`
	}

	data := []OHLCV{}
	for rows.Next() {
		var d OHLCV
		var t time.Time
		rows.Scan(&t, &d.Open, &d.High, &d.Low, &d.Close, &d.Volume)
		d.Period = t.Format("2006-01-02T15:04:05Z")
		data = append(data, d)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": data, "symbol": symbol, "interval": interval})
}

func getMovers(c *gin.Context) {
	priceMu.RLock()
	type Mover struct {
		Symbol    string  `json:"symbol"`
		Price     float64 `json:"price"`
		Change    float64 `json:"change"`
		ChangePct float64 `json:"change_pct"`
	}

	gainers := []Mover{}
	losers := []Mover{}

	for symbol, sec := range securities {
		open := dayOpens[symbol]
		if open == 0 {
			continue
		}
		change := sec.CurrentPrice - open
		changePct := (change / open) * 100

		m := Mover{
			Symbol:    symbol,
			Price:     sec.CurrentPrice,
			Change:    round2(change),
			ChangePct: round2(changePct),
		}

		if changePct > 0 {
			gainers = append(gainers, m)
		} else {
			losers = append(losers, m)
		}
	}
	priceMu.RUnlock()

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"gainers": gainers,
			"losers":  losers,
		},
	})
}

func searchSecurities(c *gin.Context) {
	q := strings.ToUpper(c.Query("q"))
	if q == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Search query required"})
		return
	}

	priceMu.RLock()
	results := []Security{}
	for _, sec := range securities {
		if strings.Contains(sec.Symbol, q) || strings.Contains(strings.ToUpper(sec.Name), q) {
			results = append(results, sec)
		}
	}
	priceMu.RUnlock()

	c.JSON(http.StatusOK, gin.H{"status": "success", "data": results, "count": len(results)})
}

func wsQuotes(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("WebSocket upgrade error:", err)
		return
	}
	defer conn.Close()

	// Register client
	wsClientsMu.Lock()
	wsClients[conn] = make(map[string]bool)
	wsClientsMu.Unlock()
	wsConnections.Inc()

	log.Println("📡 WebSocket client connected")

	// Read subscription messages
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var sub struct {
			Action string `json:"action"` // subscribe, unsubscribe
			Symbol string `json:"symbol"`
		}
		if json.Unmarshal(msg, &sub) != nil {
			continue
		}

		symbol := strings.ToUpper(sub.Symbol)
		wsClientsMu.Lock()
		switch sub.Action {
		case "subscribe":
			wsClients[conn][symbol] = true
			log.Printf("📡 Client subscribed to %s", symbol)
		case "unsubscribe":
			delete(wsClients[conn], symbol)
		}
		wsClientsMu.Unlock()
	}

	// Cleanup
	wsClientsMu.Lock()
	delete(wsClients, conn)
	wsClientsMu.Unlock()
	wsConnections.Dec()
	log.Println("📡 WebSocket client disconnected")
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

func healthCheck(c *gin.Context) {
	health := gin.H{
		"status":           "UP",
		"service":          "tradehub-market-data-service",
		"port":             "8207",
		"time":             time.Now().Format(time.RFC3339),
		"securities_count": len(securities),
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
	wsClientsMu.RLock()
	health["websocket_clients"] = len(wsClients)
	wsClientsMu.RUnlock()
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
