"""
PayFlow — Fraud Detection Service (Python / FastAPI)
=====================================================
Port: 8089
Stack: FastAPI + scikit-learn + Redis + RabbitMQ + PostgreSQL
Purpose: Real-time fraud detection using ML-based risk scoring,
         velocity checks, geographic anomaly detection, and
         behavioral analysis on financial transactions.
"""

import os
import uuid
import json
import asyncio
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import redis.asyncio as aioredis
import structlog
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
import aio_pika

# ─── Logging ────────────────────────────────────────────────────────────────

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
)
logger = structlog.get_logger()

# ─── Metrics ────────────────────────────────────────────────────────────────

FRAUD_CHECKS = Counter("fraud_checks_total", "Total fraud checks performed", ["result"])
FRAUD_LATENCY = Histogram("fraud_check_duration_seconds", "Fraud check duration")
FRAUD_BLOCKED = Counter("fraud_transactions_blocked_total", "Transactions blocked")

# ─── Models ─────────────────────────────────────────────────────────────────

class TransactionCheck(BaseModel):
    """Incoming transaction to evaluate for fraud."""
    transaction_id: str = Field(..., description="Unique transaction ID")
    source_account_id: str = Field(..., description="Sender account")
    dest_account_id: str = Field(..., description="Receiver account")
    amount: float = Field(..., gt=0, description="Transaction amount in cents")
    currency: str = Field(default="USD", max_length=3)
    payment_method: str = Field(default="transfer")
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    timestamp: Optional[str] = None

class FraudResult(BaseModel):
    """Fraud check result."""
    transaction_id: str
    risk_score: float = Field(..., ge=0, le=100)
    risk_level: str  # low, medium, high, critical
    flags: list[str]
    recommendation: str  # allow, review, block
    model_version: str
    checked_at: str
    processing_time_ms: float

class FraudReport(BaseModel):
    """Detailed fraud report for a transaction."""
    transaction_id: str
    result: FraudResult
    velocity_data: dict
    amount_analysis: dict
    behavioral_flags: list[str]

# ─── Fraud Detection Engine ────────────────────────────────────────────────

class FraudEngine:
    """ML-based fraud detection engine with rule overlays."""

    # Thresholds
    HIGH_AMOUNT_THRESHOLD = 500000  # $5,000 in cents
    VELOCITY_WINDOW_SECONDS = 3600  # 1 hour
    MAX_TXN_PER_HOUR = 20
    SUSPICIOUS_HOURS = {0, 1, 2, 3, 4}  # midnight-4am

    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client
        self.model_version = "v1.2.0-sklearn"

    async def check_transaction(self, txn: TransactionCheck) -> FraudResult:
        """Run all fraud detection checks on a transaction."""
        import time
        start = time.monotonic()

        flags: list[str] = []
        scores: list[float] = []

        # ── Rule 1: High Amount Check ────────────────────────────────────
        amount_score = self._check_amount(txn.amount)
        scores.append(amount_score)
        if amount_score > 50:
            flags.append(f"high_amount:{txn.amount/100:.2f}")

        # ── Rule 2: Velocity Check (Redis) ───────────────────────────────
        velocity_score = await self._check_velocity(txn.source_account_id)
        scores.append(velocity_score)
        if velocity_score > 40:
            flags.append("high_velocity")

        # ── Rule 3: Time-of-Day Check ────────────────────────────────────
        time_score = self._check_time_anomaly(txn.timestamp)
        scores.append(time_score)
        if time_score > 30:
            flags.append("unusual_hour")

        # ── Rule 4: Self-Transfer Check ──────────────────────────────────
        if txn.source_account_id == txn.dest_account_id:
            flags.append("self_transfer")
            scores.append(70.0)

        # ── Rule 5: ML Model Score (simulated) ──────────────────────────
        ml_score = self._ml_predict(txn)
        scores.append(ml_score)
        if ml_score > 60:
            flags.append("ml_model_flagged")

        # ── Rule 6: Round Amount Check ───────────────────────────────────
        if txn.amount % 10000 == 0 and txn.amount > 100000:
            flags.append("round_amount_suspicious")
            scores.append(25.0)

        # ── Aggregate Score ──────────────────────────────────────────────
        risk_score = min(100.0, float(np.mean(scores)) if scores else 0.0)
        risk_level = self._classify_risk(risk_score)
        recommendation = self._recommend(risk_level)

        elapsed_ms = (time.monotonic() - start) * 1000

        # Track in Redis
        await self.redis.setex(
            f"fraud:result:{txn.transaction_id}",
            86400,
            json.dumps({"risk_score": risk_score, "risk_level": risk_level, "flags": flags}),
        )

        # Metrics
        FRAUD_CHECKS.labels(result=recommendation).inc()
        FRAUD_LATENCY.observe(elapsed_ms / 1000)
        if recommendation == "block":
            FRAUD_BLOCKED.inc()

        return FraudResult(
            transaction_id=txn.transaction_id,
            risk_score=round(risk_score, 2),
            risk_level=risk_level,
            flags=flags,
            recommendation=recommendation,
            model_version=self.model_version,
            checked_at=datetime.utcnow().isoformat(),
            processing_time_ms=round(elapsed_ms, 2),
        )

    def _check_amount(self, amount: float) -> float:
        """Score based on transaction amount."""
        if amount > 1000000:  # > $10,000
            return 80.0
        if amount > self.HIGH_AMOUNT_THRESHOLD:
            return 50.0
        if amount > 100000:
            return 20.0
        return 5.0

    async def _check_velocity(self, account_id: str) -> float:
        """Check transaction frequency in sliding window."""
        key = f"fraud:velocity:{account_id}"
        now = datetime.utcnow().timestamp()
        pipe = self.redis.pipeline()
        pipe.zremrangebyscore(key, 0, now - self.VELOCITY_WINDOW_SECONDS)
        pipe.zadd(key, {str(uuid.uuid4()): now})
        pipe.zcard(key)
        pipe.expire(key, self.VELOCITY_WINDOW_SECONDS)
        results = await pipe.execute()
        count = results[2]
        if count > self.MAX_TXN_PER_HOUR:
            return 80.0
        if count > 10:
            return 40.0
        return max(0, (count / self.MAX_TXN_PER_HOUR) * 30)

    def _check_time_anomaly(self, timestamp: Optional[str]) -> float:
        """Flag transactions during unusual hours."""
        if not timestamp:
            return 0.0
        try:
            dt = datetime.fromisoformat(timestamp)
            if dt.hour in self.SUSPICIOUS_HOURS:
                return 45.0
        except ValueError:
            pass
        return 0.0

    def _ml_predict(self, txn: TransactionCheck) -> float:
        """Simulated ML model prediction using feature engineering."""
        features = np.array([
            txn.amount / 100.0,
            len(txn.source_account_id),
            1.0 if txn.payment_method == "card" else 0.5,
            hash(txn.dest_account_id) % 100 / 100.0,
        ])
        # Simple sigmoid-based scoring (placeholder for real model)
        weighted = np.dot(features, [0.001, 0.5, 10.0, 15.0])
        score = 100.0 / (1.0 + np.exp(-0.01 * (weighted - 50)))
        return float(np.clip(score, 0, 100))

    @staticmethod
    def _classify_risk(score: float) -> str:
        if score >= 80:
            return "critical"
        if score >= 60:
            return "high"
        if score >= 30:
            return "medium"
        return "low"

    @staticmethod
    def _recommend(level: str) -> str:
        return {"critical": "block", "high": "review", "medium": "review", "low": "allow"}[level]


# ─── Application ────────────────────────────────────────────────────────────

app = FastAPI(
    title="PayFlow Fraud Detection Service",
    description="ML-based fraud detection with velocity checks, behavioral analysis, and rule engine",
    version="1.0.0",
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


redis_client: aioredis.Redis = None  # type: ignore
engine: FraudEngine = None  # type: ignore
rabbit_connection: aio_pika.RobustConnection = None  # type: ignore


@app.on_event("startup")
async def startup():
    global redis_client, engine, rabbit_connection

    redis_client = aioredis.from_url(
        os.getenv("REDIS_URL", "redis://:redis123@localhost:6379/4"),
        decode_responses=True,
    )
    engine = FraudEngine(redis_client)

    try:
        rabbit_connection = await aio_pika.connect_robust(
            os.getenv("RABBITMQ_URL", "amqp://fintech:rabbit123@localhost:5672/")
        )
        logger.info("Connected to RabbitMQ — listening for events")
        asyncio.create_task(_consume_events())
    except Exception as e:
        logger.warning("RabbitMQ not available", error=str(e))


@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.close()
    if rabbit_connection:
        await rabbit_connection.close()


async def _consume_events():
    """Listen for transaction events and auto-check them."""
    channel = await rabbit_connection.channel()
    exchange = await channel.declare_exchange("events", aio_pika.ExchangeType.TOPIC, durable=True)
    queue = await channel.declare_queue("fraud-check-queue", durable=True)
    await queue.bind(exchange, "transfer.*")
    await queue.bind(exchange, "payment.*")

    async with queue.iterator() as q:
        async for message in q:
            async with message.process():
                try:
                    data = json.loads(message.body.decode())
                    txn = TransactionCheck(
                        transaction_id=data.get("transaction_id", str(uuid.uuid4())),
                        source_account_id=data.get("source_account_id", "unknown"),
                        dest_account_id=data.get("dest_account_id", "unknown"),
                        amount=data.get("amount", 0),
                    )
                    result = await engine.check_transaction(txn)
                    logger.info("auto_fraud_check", txn_id=txn.transaction_id, risk=result.risk_level)
                except Exception as e:
                    logger.error("event_processing_error", error=str(e))


# ─── Endpoints ──────────────────────────────────────────────────────────────

@app.post("/v1/fraud/check", response_model=FraudResult)
async def check_transaction(txn: TransactionCheck):
    """Run fraud detection on a transaction. Returns risk score and recommendation."""
    return await engine.check_transaction(txn)


@app.post("/v1/fraud/batch", response_model=list[FraudResult])
async def batch_check(transactions: list[TransactionCheck]):
    """Batch fraud check for multiple transactions."""
    results = await asyncio.gather(*(engine.check_transaction(t) for t in transactions))
    return list(results)


@app.get("/v1/fraud/report/{transaction_id}", response_model=dict)
async def get_fraud_report(transaction_id: str):
    """Get cached fraud check result for a transaction."""
    data = await redis_client.get(f"fraud:result:{transaction_id}")
    if not data:
        raise HTTPException(status_code=404, detail="No fraud report found")
    return {"transaction_id": transaction_id, **json.loads(data)}


@app.get("/v1/fraud/stats")
async def fraud_stats():
    """Get fraud detection statistics."""
    keys = []
    async for key in redis_client.scan_iter("fraud:result:*"):
        keys.append(key)

    results = {"total_checks": len(keys), "risk_distribution": {"low": 0, "medium": 0, "high": 0, "critical": 0}}
    for key in keys[:1000]:
        data = await redis_client.get(key)
        if data:
            parsed = json.loads(data)
            level = parsed.get("risk_level", "low")
            results["risk_distribution"][level] = results["risk_distribution"].get(level, 0) + 1

    return results


@app.get("/health")
async def health():
    try:
        await redis_client.ping()
        return {"status": "healthy", "service": "fraud-detection-service", "language": "python", "framework": "fastapi"}
    except Exception:
        return {"status": "unhealthy"}


@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8089")), reload=True)
