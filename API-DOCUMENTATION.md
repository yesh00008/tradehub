# TradeHub API Documentation

## Base URLs
- **API Gateway**: `http://localhost:8200`
- **Backend API (BFF)**: `http://localhost:5000`
- **Frontend**: `http://localhost:3003`

---

## Authentication

All authenticated endpoints require:
```
Authorization: Bearer <jwt_token>
```

---

## Trader Service (8201)

### Register Trader
```http
POST /api/v1/traders
Content-Type: application/json

{
  "username": "trader_john",
  "email": "john@tradehub.com",
  "full_name": "John Trader",
  "phone": "+1-555-0301",
  "trading_level": "advanced"
}
```

### Get Trader
```http
GET /api/v1/traders/:id
```

### Update Trader
```http
PUT /api/v1/traders/:id
Content-Type: application/json

{
  "phone": "+1-555-0999",
  "trading_level": "advanced"
}
```

### KYC Verification
```http
POST /api/v1/traders/:id/kyc
Content-Type: application/json

{
  "kyc_status": "approved",
  "document_type": "passport",
  "document_number": "AB123456"
}
```

---

## Trading Account Service (8202)

### Create Account
```http
POST /api/v1/accounts
Content-Type: application/json

{
  "trader_id": "TRD-001",
  "account_type": "cash",
  "initial_deposit": 50000.00
}
```

### Get Account
```http
GET /api/v1/accounts/:id
```

### Get Balance
```http
GET /api/v1/accounts/:id/balance
```

### Deposit
```http
POST /api/v1/accounts/:id/deposit
Content-Type: application/json

{
  "amount": 10000.00,
  "method": "wire_transfer"
}
```

### Withdraw
```http
POST /api/v1/accounts/:id/withdraw
Content-Type: application/json

{
  "amount": 5000.00,
  "method": "ach"
}
```

---

## Order Management Service (8203)

### Place Order
```http
POST /api/v1/orders
Content-Type: application/json

{
  "account_id": "ACC-001",
  "symbol": "AAPL",
  "side": "buy",
  "order_type": "market",
  "quantity": 100,
  "time_in_force": "day"
}
```

### Place Limit Order
```http
POST /api/v1/orders
Content-Type: application/json

{
  "account_id": "ACC-001",
  "symbol": "GOOGL",
  "side": "sell",
  "order_type": "limit",
  "quantity": 50,
  "price": 145.00,
  "time_in_force": "gtc"
}
```

### Get Order
```http
GET /api/v1/orders/:id
```

### List Orders
```http
GET /api/v1/orders?account_id=ACC-001
```

### Modify Order
```http
PUT /api/v1/orders/:id
Content-Type: application/json

{
  "quantity": 75,
  "price": 146.00
}
```

### Cancel Order
```http
DELETE /api/v1/orders/:id
```

---

## Portfolio Service (8206)

### Get Portfolio Summary
```http
GET /api/v1/portfolio/:account_id
```

### Get Holdings
```http
GET /api/v1/portfolio/:account_id/holdings
```

### Get P&L
```http
GET /api/v1/portfolio/:account_id/pnl
```

### Get Performance Metrics
```http
GET /api/v1/portfolio/:account_id/performance
```

---

## Market Data Service (8207)

### Get Quote
```http
GET /api/v1/market-data/:symbol
```

### Get Historical Data
```http
GET /api/v1/market-data/:symbol/history?days=30
```

### List Securities
```http
GET /api/v1/securities
```

### WebSocket Streaming
```
WS /ws/market-data

// Subscribe to symbols
{"action": "subscribe", "symbols": ["AAPL", "GOOGL", "BTC-USD"]}

// Receive real-time updates
{"symbol": "AAPL", "price": 175.52, "change": 0.02, "volume": 1500000, "timestamp": "..."}
```

---

## Clearing Service (8205)

### Get Settlement
```http
GET /api/v1/settlements/:id
```

### List Settlements
```http
GET /api/v1/settlements?account_id=ACC-001
```

---

## Notification Service (8208)

### List Notifications
```http
GET /api/v1/notifications?trader_id=TRD-001
```

### Mark Read
```http
POST /api/v1/notifications/:id/read
```

### Create Price Alert
```http
POST /api/v1/price-alerts
Content-Type: application/json

{
  "trader_id": "TRD-001",
  "symbol": "AAPL",
  "target_price": 180.00,
  "condition": "above"
}
```

---

## Health Check (All Services)
```http
GET /health
```

Response:
```json
{
  "status": "UP",
  "service": "<service-name>",
  "port": "<port>",
  "time": "2026-03-04T10:00:00Z",
  "database": "UP",
  "redis": "UP"
}
```

## Metrics (All Services)
```http
GET /metrics
```
Returns Prometheus-format metrics.
