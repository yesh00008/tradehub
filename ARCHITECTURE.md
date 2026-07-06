# TradeHub - Architecture Deep Dive

## System Architecture

```
┌─────────────────┐    ┌──────────────────────┐    ┌──────────────────────────────┐
│  React Frontend │───→│  Node.js Backend API │───→│  9 Go Microservices          │
│  (Port 3003)    │    │  (Port 5000)         │    │  (Ports 8200-8208)           │
│                 │    │  - Rate limiting      │    │  - Gin HTTP framework        │
│  React 19       │    │  - Helmet security    │    │  - PostgreSQL (lib/pq)       │
│  React Router   │    │  - Winston logging    │    │  - Redis caching             │
│  Recharts       │    │  - Proxy routing      │    │  - RabbitMQ events           │
│  Axios          │    │  - WebSocket proxy    │    │  - Prometheus metrics         │
│  Lucide icons   │    │  - Health aggregation │    │  - WebSocket streaming        │
└─────────────────┘    └──────────────────────┘    └──────────┬───────────────────┘
                                                              │
                       ┌──────────────────────────────────────┼──────────────────┐
                       │                                      │                  │
                  ┌────▼─────┐                          ┌─────▼────┐      ┌──────▼─────┐
                  │PostgreSQL│                          │  Redis   │      │ RabbitMQ   │
                  │  :5432   │                          │  :6379   │      │  :5672     │
                  │ tradehub │                          │  DB 2    │      │            │
                  └──────────┘                          └──────────┘      └────────────┘
```

## Microservices

| # | Service | Port | Responsibility |
|---|---------|------|----------------|
| 1 | API Gateway | 8200 | Authentication, routing, rate limiting, reverse proxy |
| 2 | Trader Service | 8201 | Trader registration, profiles, KYC management |
| 3 | Trading Account Service | 8202 | Account management, cash balance, deposits/withdrawals |
| 4 | Order Management Service | 8203 | Order placement, modification, cancellation, tracking |
| 5 | Execution Engine | 8204 | Order matching, trade execution, price discovery |
| 6 | Clearing Service | 8205 | Trade settlement (T+2), reconciliation |
| 7 | Portfolio Service | 8206 | Holdings tracking, P&L calculation, performance metrics |
| 8 | Market Data Service | 8207 | Real-time quotes, historical OHLCV, WebSocket streaming |
| 9 | Notification Service | 8208 | Trade fills, order updates, price alerts |

## Data Flow: Order Execution

```
1. Client → API Gateway (JWT auth + rate limit)
2. API Gateway → Order Management (validate order)
3. Order Management → Trading Account (check buying power)
4. Order Management → Execution Engine (match order)
5. Execution Engine → Clearing Service (initiate settlement)
6. Clearing Service → Portfolio Service (update holdings)
7. Notification Service ← RabbitMQ (order.filled event)
8. Notification Service → Client (trade confirmation)
```

## Database Schema

### Tables
- `traders` - Trader profiles and KYC status
- `trading_accounts` - Cash/margin accounts with balance tracking
- `securities` - Tradable instruments (stocks, ETFs, crypto)
- `orders` - Order book with full lifecycle tracking
- `trades` - Executed trades with commission and net amount
- `settlements` - T+2 settlement tracking
- `holdings` - Current positions with P&L
- `market_data` - OHLCV price history
- `transactions` - Cash movements (deposits, withdrawals)
- `notifications` - User notifications
- `price_alerts` - Configurable price alerts

## Technology Stack

- **Language**: Go 1.21, Node.js 18, React 19
- **Framework**: Gin (Go), Express (Node), React Router (Frontend)
- **Database**: PostgreSQL 16
- **Cache**: Redis 7
- **Messaging**: RabbitMQ 3.13
- **Metrics**: Prometheus + Grafana
- **Container**: Docker + Docker Compose

## Security

- JWT Bearer authentication at API Gateway
- Rate limiting (200 req/min global, 50 req/min for orders)
- Security headers (HSTS, X-Frame-Options, nosniff)
- CORS configuration
- Request ID tracking
- Input validation on all endpoints

## Performance Targets

| Metric | Target |
|--------|--------|
| Order placement latency | <50ms (p95) |
| Market data latency | <10ms |
| Order matching throughput | 10,000 orders/sec |
| WebSocket connections | 50,000+ concurrent |
| API Gateway throughput | 5,000 req/sec |
