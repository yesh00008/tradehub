# TradeHub - Trading Platform (Application 3)

## 🏗️ Architecture Overview

TradeHub is a comprehensive trading platform with 9 microservices supporting equities, ETFs, and cryptocurrency trading.

### Core Services
| Service | Port | Technology | Purpose |
|---------|------|------------|---------|
| API Gateway | 8200 | Go (Gin) | Routing, authentication, rate limiting |
| Trader Service | 8201 | Go | Trader registration, profiles, KYC |
| Trading Account Service | 8202 | Go | Account management, cash balance |
| Order Management Service | 8203 | Go | Order placement, modification, cancellation |
| Execution Engine | 8204 | Go | Order matching, trade execution |
| Clearing Service | 8205 | Go | Trade settlement, reconciliation |
| Portfolio Service | 8206 | Go | Holdings, P&L calculation |
| Market Data Service | 8207 | Go + WebSocket | Real-time quotes, historical data |
| Notification Service | 8208 | Go | Trade fills, price alerts |

### Frontend & API
- **Backend API**: Node.js Express (Port 5000) - Unified API for frontend
- **Frontend**: React (Port 3003) - Trading dashboard

### Database
- **PostgreSQL**: tradehub database with 10+ tables
- **Redis**: Order book caching, session management
- **RabbitMQ**: Event-driven communication

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- PostgreSQL already running (payflow-postgres container)
- Redis and RabbitMQ from Application 1/2

### Start TradeHub

```powershell
# From project root
.\start-tradehub.ps1

# Or manually
cd platform/compose
docker-compose -f docker-compose.tradehub.yml up -d
```

### Test Services

```powershell
# Check all services health
Invoke-RestMethod http://localhost:8200/health  # API Gateway
Invoke-RestMethod http://localhost:8202/health  # Trading Account
Invoke-RestMethod http://localhost:8203/health  # Order Management
Invoke-RestMethod http://localhost:8206/health  # Portfolio
```

## 📊 Key Features

### Trading Capabilities
- **Order Types**: Market, Limit, Stop, Stop-Limit
- **Asset Classes**: Stocks, ETFs, Cryptocurrencies
- **Account Types**: Cash, Margin
- **Real-time Data**: WebSocket streaming quotes

### Risk Management
- Buying power calculation
- Margin requirements
- Position limits
- Day trading restrictions

### Portfolio Analytics
- Real-time P&L
- Holdings tracking
- Performance metrics
- Risk metrics (Beta, Sharpe Ratio)

## 🔄 Business Flows

### Place Market Order Flow

```
User → API Gateway (Auth) 
    → Order Management (Validate)
    → Trading Account (Check buying power)
    → Execution Engine (Match order)
    → Clearing Service (Settle trade)
    → Portfolio Service (Update holdings)
    → Notification Service (Send confirmation)
```

### Real-time Quote Streaming

```
Market Data Service → WebSocket → Frontend
(Updates every 100ms for active symbols)
```

## 📋 API Endpoints

### Trading Account Service (8202)
```http
POST   /accounts                    # Create trading account
GET    /accounts/{id}               # Get account details
GET    /accounts/{id}/balance       # Get cash balance
POST   /accounts/{id}/deposit       # Deposit funds
POST   /accounts/{id}/withdraw      # Withdraw funds
```

### Order Management Service (8203)
```http
POST   /orders                      # Place order
GET    /orders/{id}                 # Get order details
PUT    /orders/{id}                 # Modify order
DELETE /orders/{id}                 # Cancel order
GET    /orders?account_id=X         # List orders
```

### Portfolio Service (8206)
```http
GET    /portfolio/{account_id}      # Get portfolio summary
GET    /portfolio/{account_id}/holdings    # Get holdings
GET    /portfolio/{account_id}/pnl         # Get P&L
GET    /portfolio/{account_id}/performance # Performance metrics
```

### Market Data Service (8207)
```http
GET    /market-data/{symbol}        # Get current quote
GET    /market-data/{symbol}/history # Historical prices
WS     /ws/market-data              # WebSocket streaming
```

## 🗄️ Database Schema

### Core Tables
- **traders**: User accounts (email, username, KYC status)
- **trading_accounts**: Trading accounts (cash balance, buying power)
- **securities**: Tradable instruments (stocks, ETFs, crypto)
- **orders**: Order history (pending, filled, cancelled)
- **trades**: Executed trades (price, quantity, commission)
- **holdings**: Current positions (quantity, avg price, P&L)
- **market_data**: OHLCV data (open, high, low, close, volume)
- **transactions**: Cash movements (deposits, withdrawals)

## 🔧 Configuration

### Environment Variables
```bash
DATABASE_URL=postgresql://fintech:fintech123@payflow-postgres:5432/tradehub
REDIS_URL=redis://payflow-redis:6379
RABBITMQ_URL=amqp://fintech:rabbit123@payflow-rabbitmq:5672/
OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4317
```

## 📈 Performance Targets
- Order placement latency: <50ms (p95)
- Market data latency: <10ms
- Order matching throughput: 10,000 orders/second
- WebSocket concurrent connections: 50,000+

## 🔒 Security
- JWT-based authentication
- API rate limiting (100 requests/second per trader)
- TLS encryption for WebSocket
- PCI DSS compliance for payment data
- SOC 2 Type II controls

## 🧪 Testing

```powershell
# Run integration tests
cd apps/application-3-tradehub
go test ./...

# Load test with k6
k6 run tests/load/trading-load-test.js
```

## 📚 Documentation
- [API Documentation](./docs/API.md)
- [Architecture Deep Dive](./docs/ARCHITECTURE.md)
- [Trading Rules](./docs/TRADING_RULES.md)
- [Deployment Guide](./docs/DEPLOYMENT.md)

## 🎯 Roadmap
- [ ] Options trading support
- [ ] Futures and derivatives
- [ ] Fractional shares
- [ ] Social trading features
- [ ] Advanced charting
- [ ] Backtesting engine
- [ ] Algorithmic trading APIs

---

**Version**: 1.0.0  
**Last Updated**: February 24, 2026  
**Status**: Production Ready
