const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const redis = require('redis');
const amqp = require('amqplib');
const jwt = require('jsonwebtoken');
const prometheus = require('prom-client');
const {createLogger, format, transports} = require('winston');

// ═══════════════════════════════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════

const config = {
  port: process.env.PORT || 8091,
  redis: {
    url: process.env.REDIS_URL || 'redis://:redis123@localhost:6379/6'
  },
  rabbitmq: {
    url: process.env.RABBITMQ_URL || 'amqp://fintech:rabbit123@localhost:5672/'
  },
  jwt: {
    secret: process.env.JWT_SECRET || 'supersecretkey123'
  },
  cors: {
    origin: process.env.CORS_ORIGIN || '*'
  }
};

// ═══════════════════════════════════════════════════════════════════════════
// LOGGER
// ═══════════════════════════════════════════════════════════════════════════

const logger = createLogger({
  level:process.env.LOG_LEVEL || 'info',
  format: format.combine(
    format.timestamp(),
    format.errors({stack: true}),
    format.json()
  ),
  transports: [
    new transports.Console({
      format: format.combine(
        format.colorize(),
        format.printf(({timestamp, level, message, ...meta}) => {
          return `${timestamp} [${level}]: ${message} ${Object.keys(meta).length ? JSON.stringify(meta) : ''}`;
        })
      )
    })
  ]
});

// ═══════════════════════════════════════════════════════════════════════════
// PROMETHEUS METRICS
// ═══════════════════════════════════════════════════════════════════════════

const register = new prometheus.Registry();
prometheus.collectDefaultMetrics({register});

const connectedClients = new prometheus.Gauge({
  name: 'realtime_connected_clients',
  help: 'Number of connected WebSocket clients',
  registers: [register]
});

const messagesProcessed = new prometheus.Counter({
  name: 'realtime_messages_processed_total',
  help: 'Total number of realtime messages processed',
  labelNames: ['event_type', 'source'],
  registers: [register]
});

const messageLatency = new prometheus.Histogram({
  name: 'realtime_message_latency_seconds',
  help: 'Latency of message processing',
  labelNames: ['event_type'],
  buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1],
  registers: [register]
});

// ═══════════════════════════════════════════════════════════════════════════
// INITIALIZE EXPRESS & SOCKET.IO
// ═══════════════════════════════════════════════════════════════════════════

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: config.cors.origin,
    methods: ['GET', 'POST'],
    credentials: true
  },
  path: '/ws',
  transports: ['websocket', 'polling']
});

app.use(express.json());

// ═══════════════════════════════════════════════════════════════════════════
// REDIS CLIENT
// ═══════════════════════════════════════════════════════════════════════════

const redisClient = redis.createClient({
  url: config.redis.url
});

redisClient.on('error', (err) => logger.error('Redis error:', err));
redisClient.on('connect', () => logger.info('Redis connected'));

// ═══════════════════════════════════════════════════════════════════════════
// RABBITMQ CONNECTION
// ═══════════════════════════════════════════════════════════════════════════

let rabbitmqChannel = null;
let rabbitmqConnection = null;

async function connectRabbitMQ() {
  try {
    rabbitmqConnection = await amqp.connect(config.rabbitmq.url);
    rabbitmqChannel = await rabbitmqConnection.createChannel();
    
    // Declare exchanges
    await rabbitmqChannel.assertExchange('transaction_events', 'topic', {durable: true});
    await rabbitmqChannel.assertExchange('notification_events', 'topic', {durable: true});
    await rabbitmqChannel.assertExchange('account_events', 'topic', {durable: true});
    
    // Create queue for realtime service
    const {queue} = await rabbitmqChannel.assertQueue('realtime_events', {
      durable: true,
      arguments: {'x-message-ttl': 60000} // 60 seconds TTL
    });
    
    // Bind to relevant routing keys
    await rabbitmqChannel.bindQueue(queue, 'transaction_events', 'transaction.*');
    await rabbitmqChannel.bindQueue(queue, 'notification_events', 'notification.*');
    await rabbitmqChannel.bindQueue(queue, 'account_events', 'account.*');
    
    // Consume messages
    rabbitmqChannel.consume(queue, handleRabbitMQMessage, {noAck: false});
    
    logger.info('RabbitMQ connected and consuming messages');
  } catch (error) {
    logger.error('Failed to connect to RabbitMQ:', error);
    setTimeout(connectRabbitMQ, 5000); // Retry after 5 seconds
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// JWT AUTHENTICATION MIDDLEWARE
// ═══════════════════════════════════════════════════════════════════════════

function authenticateSocket(socket, next) {
  const token = socket.handshake.auth.token || socket.handshake.query.token;
  
  if (!token) {
    return next(new Error('Authentication error: No token provided'));
  }
  
  try {
    const decoded = jwt.verify(token, config.jwt.secret);
    socket.userId = decoded.user_id || decoded.sub;
    socket.userEmail = decoded.email;
    socket.userRoles = decoded.roles || [];
    logger.info(`Socket authenticated for user ${socket.userId}`);
    next();
  } catch (error) {
    logger.error('JWT verification failed:', error.message);
    next(new Error('Authentication error: Invalid token'));
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// SOCKET.IO CONNECTION HANDLER
// ═══════════════════════════════════════════════════════════════════════════

io.use(authenticateSocket);

io.on('connection', (socket) => {
  connectedClients.inc();
  logger.info(`Client connected: ${socket.id}, User: ${socket.userId}`);
  
  // Join user-specific room
  const userRoom = `user:${socket.userId}`;
  socket.join(userRoom);
  
  // Send welcome message
  socket.emit('connected', {
    message: 'Connected to PayFlow realtime service',
    userId: socket.userId,
    serverTime: new Date().toISOString()
  });
  
  // ═══════════════════════════════════════════════════════════════════════════
  // SUBSCRIBE TO USER ACCOUNTS
  // ═══════════════════════════════════════════════════════════════════════════
  
  socket.on('subscribe:accounts', async (data) => {
    try {
      const accountIds = data.account_ids || [];
      
      for (const accountId of accountIds) {
        const accountRoom = `account:${accountId}`;
        socket.join(accountRoom);
        logger.info(`User ${socket.userId} subscribed to account ${accountId}`);
      }
      
      socket.emit('subscribe:accounts:success', {
        account_ids: accountIds,
        message: 'Subscribed to account updates'
      });
      
      messagesProcessed.inc({event_type: 'subscribe_accounts', source: 'client'});
    } catch (error) {
      logger.error('Error subscribing to accounts:', error);
      socket.emit('error', {message: 'Failed to subscribe to accounts'});
    }
  });
  
  // ═══════════════════════════════════════════════════════════════════════════
  // SUBSCRIBE TO TRANSACTIONS
  // ═══════════════════════════════════════════════════════════════════════════
  
  socket.on('subscribe:transactions', async (data) => {
    try {
      const transactionRoom = `transactions:${socket.userId}`;
      socket.join(transactionRoom);
      logger.info(`User ${socket.userId} subscribed to transaction updates`);
      
      socket.emit('subscribe:transactions:success', {
        message: 'Subscribed to transaction updates'
      });
      
      messagesProcessed.inc({event_type: 'subscribe_transactions', source: 'client'});
    } catch (error) {
      logger.error('Error subscribing to transactions:', error);
      socket.emit('error', {message: 'Failed to subscribe to transactions'});
    }
  });
  
  // ═══════════════════════════════════════════════════════════════════════════
  // SUBSCRIBE TO NOTIFICATIONS
  // ═══════════════════════════════════════════════════════════════════════════
  
  socket.on('subscribe:notifications', () => {
    try {
      const notificationRoom = `notifications:${socket.userId}`;
      socket.join(notificationRoom);
      logger.info(`User ${socket.userId} subscribed to notifications`);
      
      socket.emit('subscribe:notifications:success', {
        message: 'Subscribed to notifications'
      });
      
      messagesProcessed.inc({event_type: 'subscribe_notifications', source: 'client'});
    } catch (error) {
      logger.error('Error subscribing to notifications:', error);
      socket.emit('error', {message: 'Failed to subscribe to notifications'});
    }
  });
  
  // ═══════════════════════════════════════════════════════════════════════════
  // PING/PONG FOR KEEPALIVE
  // ═══════════════════════════════════════════════════════════════════════════
  
  socket.on('ping', () => {
    socket.emit('pong', {timestamp: Date.now()});
  });
  
  // ═══════════════════════════════════════════════════════════════════════════
  // DISCONNECT HANDLER
  // ═══════════════════════════════════════════════════════════════════════════
  
  socket.on('disconnect', (reason) => {
    connectedClients.dec();
    logger.info(`Client disconnected: ${socket.id}, User: ${socket.userId}, Reason: ${reason}`);
  });
  
  socket.on('error', (error) => {
    logger.error(`Socket error for ${socket.id}:`, error);
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// RABBITMQ MESSAGE HANDLER
// ═══════════════════════════════════════════════════════════════════════════

async function handleRabbitMQMessage(msg) {
  if (!msg) return;
  
  const startTime = Date.now();
  
  try {
    const content = JSON.parse(msg.content.toString());
    const routingKey = msg.fields.routingKey;
    const eventType = routingKey.split('.')[1]; // Extract event type from routing key
    
    logger.debug(`Processing message: ${routingKey}`, {content});
    
    // Route message based on event type
    switch (eventType) {
      case 'created':
      case 'updated':
      case 'completed':
      case 'failed':
        await handleTransactionEvent(content, eventType);
        break;
      case 'notification':
        await handleNotificationEvent(content);
        break;
      case 'balance':
        await handleAccountEvent(content);
        break;
      default:
        logger.warn(`Unknown event type: ${eventType}`);
    }
    
    messagesProcessed.inc({event_type: eventType, source: 'rabbitmq'});
    
    const duration = (Date.now() - startTime) / 1000;
    messageLatency.observe({event_type: eventType}, duration);
    
    rabbitmqChannel.ack(msg);
  } catch (error) {
    logger.error('Error processing RabbitMQ message:', error);
    rabbitmqChannel.nack(msg, false, false); // Don't requeue failed messages
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// EVENT HANDLERS
// ═══════════════════════════════════════════════════════════════════════════

async function handleTransactionEvent(data, eventType) {
  const {user_id, transaction_id, from_account_id, to_account_id, amount, currency, status} = data;
  
  // Emit to user room
  if (user_id) {
    io.to(`user:${user_id}`).emit('transaction:update', {
      event: `transaction.${eventType}`,
      transaction_id,
      from_account_id,
      to_account_id,
      amount,
      currency,
      status,
      timestamp: new Date().toISOString()
    });
  }
  
  // Emit to account rooms
  if (from_account_id) {
    io.to(`account:${from_account_id}`).emit('account:balance_update', {
      account_id: from_account_id,
      transaction_id,
      amount: -amount,
      currency,
      timestamp: new Date().toISOString()
    });
  }
  
  if (to_account_id) {
    io.to(`account:${to_account_id}`).emit('account:balance_update', {
      account_id: to_account_id,
      transaction_id,
      amount,
      currency,
      timestamp: new Date().toISOString()
    });
  }
  
  logger.info(`Transaction event emitted: ${eventType} for user ${user_id}`);
}

async function handleNotificationEvent(data) {
  const {user_id, notification_type, subject, message, priority} = data;
  
  if (user_id) {
    io.to(`user:${user_id}`).to(`notifications:${user_id}`).emit('notification', {
      type: notification_type,
      subject,
      message,
      priority,
      timestamp: new Date().toISOString()
    });
    
    logger.info(`Notification emitted for user ${user_id}`);
  }
}

async function handleAccountEvent(data) {
  const {account_id, user_id, balance, available_balance, currency} = data;
  
  if (account_id) {
    io.to(`account:${account_id}`).emit('account:balance', {
      account_id,
      balance,
      available_balance,
      currency,
      timestamp: new Date().toISOString()
    });
  }
  
  if (user_id) {
    io.to(`user:${user_id}`).emit('account:update', {
      account_id,
      balance,
      available_balance,
      currency,
      timestamp: new Date().toISOString()
    });
  }
  
  logger.info(`Account event emitted for account ${account_id}`);
}

// ═══════════════════════════════════════════════════════════════════════════
// HTTP ENDPOINTS
// ═══════════════════════════════════════════════════════════════════════════

app.get('/health', (req, res) => {
  const health = {
    status: 'healthy',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    connections: io.engine.clientsCount,
    redis: redisClient.isOpen ? 'connected' : 'disconnected',
    rabbitmq: rabbitmqChannel ? 'connected' : 'disconnected'
  };
  
  res.json(health);
});

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', register.contentType);
  const metrics = await register.metrics();
  res.send(metrics);
});

app.get('/stats', (req, res) => {
  const stats = {
    connected_clients: io.engine.clientsCount,
    rooms: Object.keys(io.sockets.adapter.rooms).length,
    uptime_seconds: process.uptime(),
    memory: process.memoryUsage(),
    timestamp: new Date().toISOString()
  };
  
  res.json(stats);
});

// Catch-all for WebSocket upgrade
app.get('/ws', (req, res) => {
  res.json({
    message: 'WebSocket endpoint. Connect using socket.io client.',
    path: '/ws',
    authentication: 'Required: JWT token in auth.token or query.token'
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// STARTUP
// ═══════════════════════════════════════════════════════════════════════════

async function startup() {
  try {
    // Connect to Redis
    await redisClient.connect();
    
    // Connect to RabbitMQ
    await connectRabbitMQ();
    
    // Start HTTP server
    server.listen(config.port, () => {
      logger.info(`🚀 Realtime service listening on port ${config.port}`);
      logger.info(`   WebSocket path: /ws`);
      logger.info(`   Health check: http://localhost:${config.port}/health`);
      logger.info(`   Metrics: http://localhost:${config.port}/metrics`);
    });
  } catch (error) {
    logger.error('Failed to start service:', error);
    process.exit(1);
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// GRACEFUL SHUTDOWN
// ═══════════════════════════════════════════════════════════════════════════

process.on('SIGTERM', async () => {
  logger.info('SIGTERM received, shutting down gracefully');
  
  io.close(() => {
    logger.info('Socket.IO server closed');
  });
  
  if (rabbitmqChannel) {
    await rabbitmqChannel.close();
  }
  if (rabbitmqConnection) {
    await rabbitmqConnection.close();
  }
  
  await redisClient.quit();
  
  server.close(() => {
    logger.info('HTTP server closed');
    process.exit(0);
  });
});

process.on('SIGINT', async () => {
  logger.info('SIGINT received, shutting down gracefully');
  process.emit('SIGTERM');
});

// Start the service
startup();
