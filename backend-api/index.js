const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const axios = require('axios');
const winston = require('winston');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

// Ensure logs directory exists
const logsDir = path.join(__dirname, 'logs');
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir);
}

// In-memory log storage (last 1000 logs)
const inMemoryLogs = [];
const MAX_LOGS = 1000;

class MemoryTransport extends winston.Transport {
  constructor(opts) {
    super(opts);
  }
  log(info, callback) {
    const logEntry = {
      id: Date.now() + Math.random(),
      timestamp: info.timestamp,
      level: info.level.toUpperCase(),
      message: info.message,
      metadata: { ...info, source: 'tradehub-backend-api', service: 'tradehub-api-gateway' }
    };
    inMemoryLogs.unshift(logEntry);
    if (inMemoryLogs.length > MAX_LOGS) inMemoryLogs.pop();
    if (callback) callback();
  }
}

const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.printf(({ timestamp, level, message, ...rest }) => {
          return `${timestamp} [${level}] ${message}${Object.keys(rest).length ? ' ' + JSON.stringify(rest) : ''}`;
        })
      )
    }),
    new winston.transports.File({ filename: path.join(logsDir, 'combined.log'), maxsize: 10485760, maxFiles: 5 }),
    new winston.transports.File({ filename: path.join(logsDir, 'error.log'), level: 'error', maxsize: 10485760, maxFiles: 5 }),
    new MemoryTransport({})
  ]
});

const app = express();
const PORT = process.env.PORT || 5000;

// Request logging middleware

// Service URLs
const SERVICES = {
  apiGateway:      process.env.API_GATEWAY_URL      || 'http://tradehub-api-gateway:8200',
  trader:          process.env.TRADER_SERVICE_URL    || 'http://tradehub-trader-service:8201',
  tradingAccount:  process.env.TRADING_ACCOUNT_URL   || 'http://tradehub-trading-account:8202',
  orderManagement: process.env.ORDER_MANAGEMENT_URL  || 'http://tradehub-order-management:8203',
  executionEngine: process.env.EXECUTION_ENGINE_URL  || 'http://tradehub-execution-engine:8204',
  clearing:        process.env.CLEARING_SERVICE_URL  || 'http://tradehub-clearing-service:8205',
  portfolio:       process.env.PORTFOLIO_SERVICE_URL  || 'http://tradehub-portfolio-service:8206',
  marketData:      process.env.MARKET_DATA_URL       || 'http://tradehub-market-data:8207',
  notification:    process.env.NOTIFICATION_URL      || 'http://tradehub-notification:8208',
};

// Middleware
app.use(helmet());
app.use(cors());
app.use(compression());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 1000,
  standardHeaders: true,
  legacyHeaders: false,
});
app.use(limiter);

// Request logging middleware
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const duration = Date.now() - start;
    logger.info(`${req.method} ${req.originalUrl} ${res.statusCode} ${duration}ms`, {
      method: req.method,
      url: req.originalUrl,
      status: res.statusCode,
      duration: `${duration}ms`,
      ip: req.ip,
    });
  });
  next();
});

// ─── Helper: Proxy request to a Go microservice ─────────────────────────────
async function proxyRequest(serviceUrl, path, method, data, headers = {}) {
  try {
    const config = {
      method,
      url: `${serviceUrl}${path}`,
      timeout: 10000,
      headers: {
        'Content-Type': 'application/json',
        ...headers
      },
    };
    if (data && ['POST', 'PUT', 'PATCH'].includes(method.toUpperCase())) {
      config.data = data;
    }
    const response = await axios(config);
    return { status: response.status, data: response.data };
  } catch (error) {
    if (error.response) {
      return { status: error.response.status, data: error.response.data };
    }
    logger.error(`Service proxy error: ${serviceUrl}${path}`, { error: error.message });
    return { status: 503, data: { error: 'Service unavailable', service: serviceUrl, path } };
  }
}

// ─── Health Check ────────────────────────────────────────────────────────────
app.get('/api/health', async (req, res) => {
  const health = {
    status: 'UP',
    service: 'tradehub-backend-api',
    port: PORT,
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    services: {}
  };

  const serviceChecks = Object.entries(SERVICES).map(async ([name, url]) => {
    try {
      const response = await axios.get(`${url}/health`, { timeout: 3000 });
      return [name, { status: 'UP', responseTime: response.headers['x-response-time'] || 'N/A' }];
    } catch {
      return [name, { status: 'DOWN' }];
    }
  });

  const results = await Promise.all(serviceChecks);
  results.forEach(([name, status]) => { health.services[name] = status; });

  const anyDown = results.some(([, s]) => s.status === 'DOWN');
  if (anyDown) health.status = 'DEGRADED';

  res.json(health);
});

// ─── Log Management ─────────────────────────────────────────────────────────
app.get('/api/logs', (req, res) => {
  const { level, limit = 100, offset = 0 } = req.query;
  let logs = [...inMemoryLogs];
  if (level) logs = logs.filter(l => l.level === level.toUpperCase());
  res.json({
    total: logs.length,
    offset: parseInt(offset),
    limit: parseInt(limit),
    logs: logs.slice(parseInt(offset), parseInt(offset) + parseInt(limit))
  });
});

app.get('/api/logs/stats', (req, res) => {
  const stats = { total: inMemoryLogs.length, byLevel: {} };
  inMemoryLogs.forEach(l => { stats.byLevel[l.level] = (stats.byLevel[l.level] || 0) + 1; });
  res.json(stats);
});

app.delete('/api/logs', (req, res) => {
  inMemoryLogs.length = 0;
  res.json({ message: 'Logs cleared' });
});

// ═══════════════════════════════════════════════════════════════════════════════
// TRADER ROUTES
// ═══════════════════════════════════════════════════════════════════════════════

app.post('/api/traders', async (req, res) => {
  const result = await proxyRequest(SERVICES.trader, '/api/v1/traders', 'POST', req.body);
  res.status(result.status).json(result.data);
});

app.get('/api/traders', async (req, res) => {
  const result = await proxyRequest(SERVICES.trader, '/api/v1/traders', 'GET');
  res.status(result.status).json(result.data);
});

app.get('/api/traders/:id', async (req, res) => {
  const result = await proxyRequest(SERVICES.trader, `/api/v1/traders/${req.params.id}`, 'GET');
  res.status(result.status).json(result.data);
});

app.put('/api/traders/:id', async (req, res) => {
  const result = await proxyRequest(SERVICES.trader, `/api/v1/traders/${req.params.id}`, 'PUT', req.body);
  res.status(result.status).json(result.data);
});

app.post('/api/traders/:id/kyc', async (req, res) => {
  const result = await proxyRequest(SERVICES.trader, `/api/v1/traders/${req.params.id}/kyc`, 'POST', req.body);
  res.status(result.status).json(result.data);
});

// ═══════════════════════════════════════════════════════════════════════════════
// TRADING ACCOUNT ROUTES
// ═══════════════════════════════════════════════════════════════════════════════

app.post('/api/accounts', async (req, res) => {
  const result = await proxyRequest(SERVICES.tradingAccount, '/api/v1/accounts', 'POST', req.body);
  res.status(result.status).json(result.data);
});

app.get('/api/accounts/:id', async (req, res) => {
  const result = await proxyRequest(SERVICES.tradingAccount, `/api/v1/accounts/${req.params.id}`, 'GET');
  res.status(result.status).json(result.data);
});

app.get('/api/accounts/trader/:traderId', async (req, res) => {
  const result = await proxyRequest(SERVICES.tradingAccount, `/api/v1/accounts/trader/${req.params.traderId}`, 'GET');
  res.status(result.status).json(result.data);
});

app.get('/api/accounts/:id/balance', async (req, res) => {
  const result = await proxyRequest(SERVICES.tradingAccount, `/api/v1/accounts/${req.params.id}/balance`, 'GET');
  res.status(result.status).json(result.data);
});

app.post('/api/accounts/:id/deposit', async (req, res) => {
  const result = await proxyRequest(SERVICES.tradingAccount, `/api/v1/accounts/${req.params.id}/deposit`, 'POST', req.body);
  res.status(result.status).json(result.data);
});

app.post('/api/accounts/:id/withdraw', async (req, res) => {
  const result = await proxyRequest(SERVICES.tradingAccount, `/api/v1/accounts/${req.params.id}/withdraw`, 'POST', req.body);
  res.status(result.status).json(result.data);
});

// ═══════════════════════════════════════════════════════════════════════════════
// ORDER ROUTES
// ═══════════════════════════════════════════════════════════════════════════════

app.post('/api/orders', async (req, res) => {
  const result = await proxyRequest(SERVICES.orderManagement, '/api/v1/orders', 'POST', req.body);
  res.status(result.status).json(result.data);
});

app.get('/api/orders/:id', async (req, res) => {
  const result = await proxyRequest(SERVICES.orderManagement, `/api/v1/orders/${req.params.id}`, 'GET');
  res.status(result.status).json(result.data);
});

app.get('/api/orders', async (req, res) => {
  const qs = req.query.account_id ? `?account_id=${req.query.account_id}` : '';
  const result = await proxyRequest(SERVICES.orderManagement, `/api/v1/orders${qs}`, 'GET');
  res.status(result.status).json(result.data);
});

app.put('/api/orders/:id', async (req, res) => {
  const result = await proxyRequest(SERVICES.orderManagement, `/api/v1/orders/${req.params.id}`, 'PUT', req.body);
  res.status(result.status).json(result.data);
});

app.delete('/api/orders/:id', async (req, res) => {
  const result = await proxyRequest(SERVICES.orderManagement, `/api/v1/orders/${req.params.id}`, 'DELETE');
  res.status(result.status).json(result.data);
});

// ═══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO ROUTES
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/portfolio/:accountId', async (req, res) => {
  const result = await proxyRequest(SERVICES.portfolio, `/api/v1/portfolio/${req.params.accountId}`, 'GET');
  res.status(result.status).json(result.data);
});

app.get('/api/portfolio/:accountId/holdings', async (req, res) => {
  const result = await proxyRequest(SERVICES.portfolio, `/api/v1/portfolio/${req.params.accountId}/holdings`, 'GET');
  res.status(result.status).json(result.data);
});

app.get('/api/portfolio/:accountId/pnl', async (req, res) => {
  const result = await proxyRequest(SERVICES.portfolio, `/api/v1/portfolio/${req.params.accountId}/pnl`, 'GET');
  res.status(result.status).json(result.data);
});

app.get('/api/portfolio/:accountId/performance', async (req, res) => {
  const result = await proxyRequest(SERVICES.portfolio, `/api/v1/portfolio/${req.params.accountId}/performance`, 'GET');
  res.status(result.status).json(result.data);
});

// ═══════════════════════════════════════════════════════════════════════════════
// MARKET DATA ROUTES
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/market-data/securities', async (req, res) => {
  const result = await proxyRequest(SERVICES.marketData, '/api/v1/securities', 'GET');
  res.status(result.status).json(result.data);
});

app.get('/api/market-data/:symbol', async (req, res) => {
  const result = await proxyRequest(SERVICES.marketData, `/api/v1/market-data/${req.params.symbol}`, 'GET');
  res.status(result.status).json(result.data);
});

app.get('/api/market-data/:symbol/history', async (req, res) => {
  const days = req.query.days || '30';
  const result = await proxyRequest(SERVICES.marketData, `/api/v1/market-data/${req.params.symbol}/history?days=${days}`, 'GET');
  res.status(result.status).json(result.data);
});

// ═══════════════════════════════════════════════════════════════════════════════
// EXECUTION & CLEARING ROUTES
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/trades', async (req, res) => {
  const qs = req.query.account_id ? `?account_id=${req.query.account_id}` : '';
  const result = await proxyRequest(SERVICES.executionEngine, `/api/v1/trades${qs}`, 'GET');
  res.status(result.status).json(result.data);
});

app.get('/api/trades/:id', async (req, res) => {
  const result = await proxyRequest(SERVICES.executionEngine, `/api/v1/trades/${req.params.id}`, 'GET');
  res.status(result.status).json(result.data);
});

app.get('/api/settlements', async (req, res) => {
  const qs = req.query.account_id ? `?account_id=${req.query.account_id}` : '';
  const result = await proxyRequest(SERVICES.clearing, `/api/v1/settlements${qs}`, 'GET');
  res.status(result.status).json(result.data);
});

app.get('/api/settlements/:id', async (req, res) => {
  const result = await proxyRequest(SERVICES.clearing, `/api/v1/settlements/${req.params.id}`, 'GET');
  res.status(result.status).json(result.data);
});

// ═══════════════════════════════════════════════════════════════════════════════
// NOTIFICATION ROUTES
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/notifications', async (req, res) => {
  const qs = req.query.trader_id ? `?trader_id=${req.query.trader_id}` : '';
  const result = await proxyRequest(SERVICES.notification, `/api/v1/notifications${qs}`, 'GET');
  res.status(result.status).json(result.data);
});

app.post('/api/notifications/:id/read', async (req, res) => {
  const result = await proxyRequest(SERVICES.notification, `/api/v1/notifications/${req.params.id}/read`, 'POST');
  res.status(result.status).json(result.data);
});

app.post('/api/price-alerts', async (req, res) => {
  const result = await proxyRequest(SERVICES.notification, '/api/v1/price-alerts', 'POST', req.body);
  res.status(result.status).json(result.data);
});

app.get('/api/price-alerts', async (req, res) => {
  const qs = req.query.trader_id ? `?trader_id=${req.query.trader_id}` : '';
  const result = await proxyRequest(SERVICES.notification, `/api/v1/price-alerts${qs}`, 'GET');
  res.status(result.status).json(result.data);
});

// ─── Dashboard Aggregate ────────────────────────────────────────────────────
app.get('/api/dashboard/:accountId', async (req, res) => {
  const accountId = req.params.accountId;
  try {
    const [accountRes, portfolioRes, ordersRes, holdingsRes] = await Promise.all([
      proxyRequest(SERVICES.tradingAccount, `/api/v1/accounts/${accountId}`, 'GET'),
      proxyRequest(SERVICES.portfolio, `/api/v1/portfolio/${accountId}`, 'GET'),
      proxyRequest(SERVICES.orderManagement, `/api/v1/orders?account_id=${accountId}`, 'GET'),
      proxyRequest(SERVICES.portfolio, `/api/v1/portfolio/${accountId}/holdings`, 'GET'),
    ]);

    res.json({
      account: accountRes.data,
      portfolio: portfolioRes.data,
      recentOrders: ordersRes.data,
      holdings: holdingsRes.data,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    logger.error('Dashboard aggregation error', { error: error.message });
    res.status(500).json({ error: 'Failed to load dashboard data' });
  }
});

// ─── 404 Handler ─────────────────────────────────────────────────────────────
app.use((req, res) => {
  res.status(404).json({
    error: 'Not Found',
    message: `Route ${req.method} ${req.originalUrl} not found`,
    service: 'tradehub-backend-api'
  });
});

// ─── Error Handler ───────────────────────────────────────────────────────────
app.use((err, req, res, next) => {
  logger.error(`Unhandled error: ${err.message}`, { stack: err.stack });
  res.status(500).json({ error: 'Internal server error', message: err.message });
});

// ─── Start Server ────────────────────────────────────────────────────────────
const server = app.listen(PORT, () => {
  logger.info(`🚀 TradeHub Backend API running on port ${PORT}`);
  logger.info(`📊 Services configured:`, { services: Object.keys(SERVICES) });
});

// WebSocket proxy for market data streaming
const wss = new WebSocket.Server({ server, path: '/ws/market-data' });

wss.on('connection', (ws, req) => {
  logger.info('WebSocket client connected for market data');

  // Connect to upstream market data service WebSocket
  const upstreamUrl = SERVICES.marketData.replace('http://', 'ws://') + '/ws/market-data';
  let upstream;
  
  try {
    upstream = new WebSocket(upstreamUrl);
    
    upstream.on('message', (data) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(data.toString());
      }
    });

    upstream.on('close', () => {
      logger.info('Upstream market data WebSocket closed');
    });

    upstream.on('error', (error) => {
      logger.error('Upstream WebSocket error', { error: error.message });
    });
  } catch (error) {
    logger.error('Failed to connect to upstream WebSocket', { error: error.message });
  }

  ws.on('message', (message) => {
    // Forward subscription messages to upstream
    if (upstream && upstream.readyState === WebSocket.OPEN) {
      upstream.send(message.toString());
    }
  });

  ws.on('close', () => {
    logger.info('WebSocket client disconnected');
    if (upstream) upstream.close();
  });
});

// Graceful shutdown
process.on('SIGTERM', () => {
  logger.info('SIGTERM received, shutting down...');
  wss.close();
  server.close(() => {
    logger.info('Server closed');
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  logger.info('SIGINT received, shutting down...');
  wss.close();
  server.close(() => {
    logger.info('Server closed');
    process.exit(0);
  });
});
