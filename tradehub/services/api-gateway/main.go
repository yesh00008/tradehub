package main


import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	db          *sql.DB
	redisClient *redis.Client
	ctx         = context.Background()

	// Service URLs
	traderServiceURL         string
	tradingAccountServiceURL string
	orderManagementURL       string
	executionEngineURL       string
	clearingServiceURL       string
	portfolioServiceURL      string
	marketDataServiceURL     string
	notificationServiceURL   string

	// Prometheus metrics
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tradehub_gateway_requests_total",
			Help: "Total number of requests to TradeHub API Gateway",
		},
		[]string{"method", "endpoint", "status"},
	)

	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "tradehub_gateway_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)

	activeConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "tradehub_gateway_active_connections",
			Help: "Number of active connections",
		},
	)
)

func init() {
	prometheus.MustRegister(requestsTotal)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(activeConnections)
}

func main() {
	log.Println("🚀 TradeHub API Gateway starting...")

	// Initialize service URLs
	traderServiceURL = getEnv("TRADER_SERVICE_URL", "http://localhost:8201")
	tradingAccountServiceURL = getEnv("TRADING_ACCOUNT_SERVICE_URL", "http://localhost:8202")
	orderManagementURL = getEnv("ORDER_MANAGEMENT_URL", "http://localhost:8203")
	executionEngineURL = getEnv("EXECUTION_ENGINE_URL", "http://localhost:8204")
	clearingServiceURL = getEnv("CLEARING_SERVICE_URL", "http://localhost:8205")
	portfolioServiceURL = getEnv("PORTFOLIO_SERVICE_URL", "http://localhost:8206")
	marketDataServiceURL = getEnv("MARKET_DATA_SERVICE_URL", "http://localhost:8207")
	notificationServiceURL = getEnv("NOTIFICATION_SERVICE_URL", "http://localhost:8208")

	// Database connection
	var err error
	dbURL := getEnv("DATABASE_URL", "postgres://fintech:fintech123@localhost:5432/tradehub?sslmode=disable")
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

	// Redis connection
	redisClient = redis.NewClient(&redis.Options{
		Addr:     getEnv("REDIS_URL", "localhost:6379"),
		Password: "",
		DB:       2,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Println("⚠ Redis not available:", err)
	} else {
		log.Println("✓ Redis connected")
	}

	// Setup Gin router - with request logging
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	// Global middleware
	router.Use(corsMiddleware())
	router.Use(requestIDMiddleware())
	router.Use(securityHeadersMiddleware())
	router.Use(metricsMiddleware())
	router.Use(rateLimitMiddleware(200))

	// Health & metrics endpoints
	router.GET("/health", healthCheck)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Public routes
	router.POST("/v1/auth/login", proxyTo(traderServiceURL))
	router.POST("/v1/auth/register", proxyTo(traderServiceURL))

	// Authenticated routes
	auth := router.Group("/v1")
	auth.Use(authMiddleware())
	{
		// Trader routes
		auth.GET("/traders/:id", proxyTo(traderServiceURL))
		auth.PUT("/traders/:id", proxyTo(traderServiceURL))
		auth.POST("/traders/:id/kyc", proxyTo(traderServiceURL))

		// Trading account routes
		auth.POST("/accounts", proxyTo(tradingAccountServiceURL))
		auth.GET("/accounts/:id", proxyTo(tradingAccountServiceURL))
		auth.GET("/accounts/:id/balance", proxyTo(tradingAccountServiceURL))
		auth.POST("/accounts/:id/deposit", proxyTo(tradingAccountServiceURL))
		auth.POST("/accounts/:id/withdraw", proxyTo(tradingAccountServiceURL))

		// Order routes (with stricter rate limit)
		auth.POST("/orders", rateLimitMiddleware(50), proxyTo(orderManagementURL))
		auth.GET("/orders/:id", proxyTo(orderManagementURL))
		auth.PUT("/orders/:id", proxyTo(orderManagementURL))
		auth.DELETE("/orders/:id", proxyTo(orderManagementURL))
		auth.GET("/orders", proxyTo(orderManagementURL))

		// Portfolio routes
		auth.GET("/portfolio/:account_id", proxyTo(portfolioServiceURL))
		auth.GET("/portfolio/:account_id/holdings", proxyTo(portfolioServiceURL))
		auth.GET("/portfolio/:account_id/pnl", proxyTo(portfolioServiceURL))
		auth.GET("/portfolio/:account_id/performance", proxyTo(portfolioServiceURL))

		// Market data routes
		auth.GET("/market-data/:symbol", proxyTo(marketDataServiceURL))
		auth.GET("/market-data/:symbol/history", proxyTo(marketDataServiceURL))

		// Clearing routes
		auth.GET("/settlements/:id", proxyTo(clearingServiceURL))
		auth.GET("/settlements", proxyTo(clearingServiceURL))
	}

	// Start server
	port := getEnv("PORT", "8200")
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go
func() {
		log.Printf("🚀 TradeHub API Gateway running on port %s\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down TradeHub API Gateway...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
	log.Println("TradeHub API Gateway exited")
}

// proxyTo creates a reverse proxy handler to the target service
func proxyTo(targetURL string) gin.HandlerFunc {
	return
func(c *gin.Context) {
		target, err := url.Parse(targetURL)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": "service unavailable"})
			return
		}

		proxy := httputil.NewSingleHostReverseProxy(target)
		proxy.ErrorHandler =
func(w http.ResponseWriter, r *http.Request, err error) {
			log.Printf("Proxy error: %v", err)
			w.WriteHeader(http.StatusBadGateway)
			json.NewEncoder(w).Encode(gin.H{"error": "service unavailable", "details": err.Error()})
		}

		c.Request.Host = target.Host
		proxy.ServeHTTP(c.Writer, c.Request)
	}
}

func healthCheck(c *gin.Context) {
	services := map[string]string{
		"trader-service":          traderServiceURL,
		"trading-account-service": tradingAccountServiceURL,
		"order-management":        orderManagementURL,
		"execution-engine":        executionEngineURL,
		"clearing-service":        clearingServiceURL,
		"portfolio-service":       portfolioServiceURL,
		"market-data-service":     marketDataServiceURL,
		"notification-service":    notificationServiceURL,
	}

	health := gin.H{
		"status":  "UP",
		"service": "tradehub-api-gateway",
		"port":    "8200",
		"time":    time.Now().Format(time.RFC3339),
	}

	// Check database
	if db != nil {
		if err := db.Ping(); err != nil {
			health["database"] = "DOWN"
		} else {
			health["database"] = "UP"
		}
	}

	// Check Redis
	if err := redisClient.Ping(ctx).Err(); err != nil {
		health["redis"] = "DOWN"
	} else {
		health["redis"] = "UP"
	}

	// Check downstream services
	serviceHealth := make(map[string]string)
	client := &http.Client{Timeout: 2 * time.Second}
	for name, svcURL := range services {
		resp, err := client.Get(svcURL + "/health")
		if err != nil || resp.StatusCode != 200 {
			serviceHealth[name] = "DOWN"
		} else {
			serviceHealth[name] = "UP"
			resp.Body.Close()
		}
	}
	health["services"] = serviceHealth

	c.JSON(http.StatusOK, health)
}

// Middleware
functions
func corsMiddleware() gin.HandlerFunc {
	return
func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Origin, Content-Type, Authorization, X-Request-ID, Idempotency-Key")
		c.Header("Access-Control-Max-Age", "86400")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

func requestIDMiddleware() gin.HandlerFunc {
	return
func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			requestID = fmt.Sprintf("th-%d", time.Now().UnixNano())
		}
		c.Header("X-Request-ID", requestID)
		c.Set("request_id", requestID)
		c.Next()
	}
}

func securityHeadersMiddleware() gin.HandlerFunc {
	return
func(c *gin.Context) {
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("X-Frame-Options", "DENY")
		c.Header("X-XSS-Protection", "1; mode=block")
		c.Header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		c.Header("Referrer-Policy", "strict-origin-when-cross-origin")
		c.Next()
	}
}

func metricsMiddleware() gin.HandlerFunc {
	return
func(c *gin.Context) {
		start := time.Now()
		activeConnections.Inc()

		c.Next()

		duration := time.Since(start).Seconds()
		status := fmt.Sprintf("%d", c.Writer.Status())
		endpoint := c.FullPath()
		if endpoint == "" {
			endpoint = c.Request.URL.Path
		}

		requestsTotal.WithLabelValues(c.Request.Method, endpoint, status).Inc()
		requestDuration.WithLabelValues(c.Request.Method, endpoint).Observe(duration)
		activeConnections.Dec()
	}
}

func rateLimitMiddleware(maxRequests int) gin.HandlerFunc {
	return
func(c *gin.Context) {
		clientIP := c.ClientIP()
		key := fmt.Sprintf("tradehub:ratelimit:%s", clientIP)

		count, err := redisClient.Incr(ctx, key).Result()
		if err != nil {
			// Fail open if Redis is down
			c.Next()
			return
		}

		if count == 1 {
			redisClient.Expire(ctx, key, time.Minute)
		}

		if count > int64(maxRequests) {
			c.Header("X-RateLimit-Limit", fmt.Sprintf("%d", maxRequests))
			c.Header("X-RateLimit-Remaining", "0")
			c.Header("Retry-After", "60")
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":   "rate limit exceeded",
				"message": fmt.Sprintf("Maximum %d requests per minute", maxRequests),
			})
			return
		}

		c.Header("X-RateLimit-Limit", fmt.Sprintf("%d", maxRequests))
		c.Header("X-RateLimit-Remaining", fmt.Sprintf("%d", int64(maxRequests)-count))
		c.Next()
	}
}

func authMiddleware() gin.HandlerFunc {
	return
func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authorization header required"})
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || parts[0] != "Bearer" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid authorization format"})
			return
		}

		// For now, accept any bearer token (validated by downstream services)
		// In production, validate JWT here
		c.Set("token", parts[1])
		c.Next()
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Suppress unused import warnings
var _ = io.ReadAll
var _ = strings.NewReader
