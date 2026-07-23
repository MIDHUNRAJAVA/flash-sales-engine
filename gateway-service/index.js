require('./tracing'); // MUST be first: patches http/express/redis at load time

const express = require('express');
const axios = require('axios');
const redis = require('redis');
const crypto = require('crypto');
const CircuitBreaker = require('opossum');
const promClient = require('prom-client');
const { trace, context } = require('@opentelemetry/api');
const { RateLimiter } = require('./ratelimit');
const { WaitingRoom } = require('./waitingroom');
const { BackPressure } = require('./backpressure');
const { logger, hashUid, maskIp } = require('./logger');

const app = express();
app.set('trust proxy', true);
app.use(express.json({ limit: '1kb' }));

// ── Config ────────────────────────────────────────────────────────────────────
const REDIS_RL_HOST = process.env.REDIS_RL_HOST || process.env.REDIS_HOST || 'localhost';
const REDIS_RL_PORT = process.env.REDIS_RL_PORT || process.env.REDIS_PORT || '6379';
const REDIS_QUEUE_HOST = process.env.REDIS_QUEUE_HOST || REDIS_RL_HOST;
const REDIS_QUEUE_PORT = process.env.REDIS_QUEUE_PORT || REDIS_RL_PORT;
const INVENTORY_SERVICE_URL = process.env.INVENTORY_SERVICE_URL || 'http://localhost:8080/buy';
const NATS_URL = process.env.NATS_URL || 'nats://localhost:4222';
const UPSTREAM_TIMEOUT_MS = parseInt(process.env.UPSTREAM_TIMEOUT_MS || '5000');
const MAX_INFLIGHT_BUY = parseInt(process.env.MAX_INFLIGHT_BUY || '5000');
const BUY_WINDOW_SECONDS = parseInt(process.env.BUY_WINDOW_SECONDS || '10');
const BUY_WINDOW_MAX = parseInt(process.env.BUY_WINDOW_MAX || '1');
const RL_SLOW_MS = parseInt(process.env.RL_SLOW_MS || '25');
const SALE_ID = process.env.SALE_ID || 'S1';
const ADMIT_RATE = parseInt(process.env.ADMIT_RATE || '500');

const rlRedis = redis.createClient({ url: `redis://${REDIS_RL_HOST}:${REDIS_RL_PORT}` });
rlRedis.on('error', (err) => logger.error({ event: 'rl_redis_error', error: err.message }));
const queueRedis = redis.createClient({ url: `redis://${REDIS_QUEUE_HOST}:${REDIS_QUEUE_PORT}` });
queueRedis.on('error', (err) => logger.error({ event: 'queue_redis_error', error: err.message }));

const limiter = new RateLimiter(rlRedis);
const waitingRoom = new WaitingRoom(queueRedis, { saleId: SALE_ID, admitRate: ADMIT_RATE });
const backpressure = new BackPressure({ natsUrl: NATS_URL });

// ── Metrics ───────────────────────────────────────────────────────────────────
promClient.collectDefaultMetrics();
const httpRequests = new promClient.Counter({
    name: 'http_requests_total', help: 'Requests by endpoint/status',
    labelNames: ['endpoint', 'method', 'status_code'],
});
const httpDuration = new promClient.Histogram({
    name: 'http_request_duration_seconds', help: 'Request latency',
    labelNames: ['endpoint', 'status_code'],
    buckets: [.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10],
});
const rlDecisions = new promClient.Counter({
    name: 'rate_limit_decisions_total', help: 'Limiter outcomes',
    labelNames: ['endpoint', 'result'],
});
const activeBuys = new promClient.Gauge({ name: 'active_buy_requests', help: 'In-flight /buy on this instance' });
const waitingRoomDepth = new promClient.Gauge({ name: 'waiting_room_depth', help: 'Users queued' });

// ── Middleware ────────────────────────────────────────────────────────────────

// Identity + trace attributes.
app.use((req, res, next) => {
    req.id = req.headers['x-request-id'] || crypto.randomUUID();
    res.setHeader('X-Request-ID', req.id);
    const span = trace.getSpan(context.active());
    if (span) {
        span.setAttribute('request_id', req.id);
        span.setAttribute('client.traceparent', req.headers['traceparent'] || '');
    }
    next();
});

// Response hardening.
app.use((req, res, next) => {
    res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
    res.setHeader('Content-Security-Policy', "default-src 'none'; frame-ancestors 'none'");
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('Referrer-Policy', 'no-referrer');
    res.setHeader('Cache-Control', 'no-store');
    next();
});

// Observe + canonically log every request. Sampling: 100% of >=400, 1% of 2xx
// (deterministic by request_id so a sampled request keeps a whole narrative).
app.use((req, res, next) => {
    const startNs = process.hrtime.bigint();
    const end = httpDuration.startTimer({ endpoint: req.path });
    res.on('finish', () => {
        httpRequests.inc({ endpoint: req.path, method: req.method, status_code: res.statusCode });
        end({ status_code: res.statusCode });
        const durMs = Number(process.hrtime.bigint() - startNs) / 1e6;
        const sampled = res.statusCode >= 400 ||
            (parseInt(req.id.replace(/-/g, '').slice(0, 8), 16) % 100 === 0);
        if (sampled) {
            const line = {
                event: 'response_sent', request_id: req.id,
                status_code: res.statusCode, endpoint: req.path,
                duration_ms: Math.round(durMs * 100) / 100,
                user_id: hashUid(req.body?.userID),
                ip: maskIp(req.ip),
                rate_limit_result: req.rlResult,
            };
            if (res.statusCode >= 500) logger.error(line);
            else if (res.statusCode >= 400) logger.warn(line);
            else logger.info(line);
        }
    });
    next();
});

// Per-instance in-flight cap: crash-safe, 0 RTT.
let inflight = 0;
function concurrencyCap(req, res, next) {
    if (inflight >= MAX_INFLIGHT_BUY) {
        rlDecisions.inc({ endpoint: '/buy', result: 'shed' });
        req.rlResult = 'shed';
        return shed(res, 'too many concurrent purchases in flight');
    }
    inflight++;
    activeBuys.set(inflight);
    res.on('finish', () => { inflight--; activeBuys.set(inflight); });
    next();
}

function shed(res, reason) {
    res.setHeader('Retry-After', '5');
    return res.status(503).json({ error: 'system_busy', detail: reason, queue_url: '/queue/join' });
}

// Back-pressure gate: only enforced when the consumer is falling behind. Then
// /buy demands an admission ticket; without one, auto-enqueue and return 503.
async function queueGate(req, res, next) {
    if (!backpressure.shouldQueue()) return next();
    const userId = req.body?.userID;
    if (!userId) return res.status(400).json({ error: 'userID is required' });
    try {
        if (await waitingRoom.isAdmitted(userId)) return next();
        const position = await waitingRoom.join(userId);
        req.rlResult = 'queued';
        res.setHeader('Retry-After', '5');
        return res.status(503).json({
            error: 'queued', position: position + 1,
            eta_seconds: Math.ceil((position + 1) / ADMIT_RATE),
            poll_url: `/queue/position?userID=${encodeURIComponent(userId)}`,
        });
    } catch (err) {
        // queueRedis is the same fail-loud (noeviction) instance as the rate
        // limiter (Bug #3) — mirror its fail-closed contract, never a hang/500.
        logger.error({ event: 'waiting_room_unavailable', error: err.message });
        req.rlResult = 'shed';
        return shed(res, 'waiting room unavailable');
    }
}

let slowCalls = 0, totalCalls = 0;
setInterval(() => { slowCalls = 0; totalCalls = 0; }, 3000).unref();

async function rateLimit(req, res, next) {
    const userId = req.body?.userID;
    if (!userId) return res.status(400).json({ error: 'userID is required' });

    const span = trace.getSpan(context.active());
    if (span) span.setAttribute('user_id_hash', hashUid(userId));

    // Adaptive shed: if the limiter itself is degraded, fail CLOSED.
    if (totalCalls > 20 && slowCalls / totalCalls > 0.5) {
        rlDecisions.inc({ endpoint: req.path, result: 'shed' });
        req.rlResult = 'shed';
        return shed(res, 'rate limiter degraded');
    }

    let decision;
    const start = process.hrtime.bigint();
    try {
        decision = await limiter.check(userId, req.path, BUY_WINDOW_SECONDS, BUY_WINDOW_MAX);
    } catch (err) {
        logger.error({ event: 'rate_limit_unavailable', error: err.message });
        rlDecisions.inc({ endpoint: req.path, result: 'shed' });
        req.rlResult = 'shed';
        return shed(res, 'rate limiter unavailable'); // fail closed, never 500
    } finally {
        const ms = Number(process.hrtime.bigint() - start) / 1e6;
        totalCalls++;
        if (ms > RL_SLOW_MS) slowCalls++;
    }

    res.setHeader('X-RateLimit-Remaining', decision.remaining);
    res.setHeader('X-RateLimit-Reset', decision.reset);

    if (decision.code !== 0) {
        const result = decision.code === 1 ? 'rate_limited' : 'quota_exceeded';
        req.rlResult = result;
        rlDecisions.inc({ endpoint: req.path, result });
        if (span) span.setAttribute('rate_limit_result', result);
        res.setHeader('Retry-After', Math.max(1, decision.reset - Math.floor(Date.now() / 1000)));
        return res.status(429).json({ error: 'Too many requests', reason: result });
    }
    req.rlResult = 'allowed';
    rlDecisions.inc({ endpoint: req.path, result: 'allowed' });
    if (span) span.setAttribute('rate_limit_result', 'allowed');
    next();
}

// ── Upstream circuit breaker ──────────────────────────────────────────────────
const inventoryBreaker = new CircuitBreaker(
    (body, headers) => axios.post(INVENTORY_SERVICE_URL, body, { timeout: UPSTREAM_TIMEOUT_MS, headers }),
    {
        timeout: UPSTREAM_TIMEOUT_MS + 500,
        errorThresholdPercentage: 50,
        volumeThreshold: 20,
        rollingCountTimeout: 10000,
        resetTimeout: 5000,
        errorFilter: (err) => err.response && err.response.status < 500,
    },
);
inventoryBreaker.on('open', () => logger.warn({ event: 'circuit_open', dep: 'inventory' }));
inventoryBreaker.on('halfOpen', () => logger.warn({ event: 'circuit_half_open', dep: 'inventory' }));

// ── Routes ────────────────────────────────────────────────────────────────────

app.get('/health', async (req, res) => {
    const ok = rlRedis.isReady && !inventoryBreaker.opened;
    res.status(ok ? 200 : 503).json({
        status: ok ? 'ok' : 'degraded',
        rl_redis: rlRedis.isReady,
        inventory_breaker: inventoryBreaker.opened ? 'open' : 'closed',
        queue_active: backpressure.shouldQueue(),
    });
});

app.get('/metrics', async (req, res) => {
    res.setHeader('Content-Type', promClient.register.contentType);
    res.send(await promClient.register.metrics());
});

app.post('/queue/join', async (req, res) => {
    const userId = req.body?.userID;
    if (!userId) return res.status(400).json({ error: 'userID is required' });
    try {
        const position = await waitingRoom.join(userId);
        res.json({ position: position + 1, eta_seconds: Math.ceil((position + 1) / ADMIT_RATE),
            poll_url: `/queue/position?userID=${encodeURIComponent(userId)}` });
    } catch (err) {
        logger.error({ event: 'waiting_room_unavailable', error: err.message });
        res.status(503).json({ error: 'queue temporarily unavailable' });
    }
});

app.get('/queue/position', async (req, res) => {
    const userId = req.query.userID;
    if (!userId) return res.status(400).json({ error: 'userID is required' });
    try {
        res.json(await waitingRoom.position(userId));
    } catch (err) {
        logger.error({ event: 'waiting_room_unavailable', error: err.message });
        res.status(503).json({ error: 'queue temporarily unavailable' });
    }
});

app.post('/buy', concurrencyCap, queueGate, rateLimit, async (req, res) => {
    const { productID, quantity } = req.body;
    if (!productID) return res.status(400).json({ error: 'productID is required' });
    if (!quantity || typeof quantity !== 'number' || quantity <= 0) {
        return res.status(400).json({ error: 'quantity must be a positive number' });
    }
    // Nonce minted HERE (not client): idempotency key must be stable across
    // retries of one attempt and bound to the authenticated user.
    if (!req.body.clientNonce) req.body.clientNonce = crypto.randomUUID();

    try {
        const response = await inventoryBreaker.fire(req.body, { 'X-Request-ID': req.id });
        res.status(response.status).json({ ...response.data, clientNonce: req.body.clientNonce });
    } catch (error) {
        if (inventoryBreaker.opened || error.code === 'EOPENBREAKER') {
            return shed(res, 'inventory temporarily unavailable');
        }
        if (error.code === 'ECONNABORTED' || error.name === 'TimeoutError') {
            return res.status(504).json({ error: 'Inventory service timed out' });
        }
        if (error.response) return res.status(error.response.status).json(error.response.data);
        return res.status(502).json({ error: 'Inventory service unavailable' });
    }
});

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
let server;

async function start() {
    await rlRedis.connect();
    await queueRedis.connect();
    await limiter.load();
    await waitingRoom.load();
    waitingRoom.startAdmitting((depth) => waitingRoomDepth.set(depth));
    await backpressure.start().catch((e) => logger.warn({ event: 'backpressure_unavailable', error: e.message }));
    server = app.listen(PORT, () => logger.info({ event: 'startup', port: PORT }));
    server.maxHeadersCount = 64;
    server.maxConnections = 8000;
}
start().catch((err) => { logger.error({ event: 'startup_failed', error: err.message }); process.exit(1); });

// Graceful shutdown: stop accepting, drain in-flight, 30s deadline.
function shutdown(signal) {
    logger.info({ event: 'shutdown', signal });
    waitingRoom.stop();
    server.close(async () => {
        await backpressure.close().catch(() => {});
        await rlRedis.quit().catch(() => {});
        await queueRedis.quit().catch(() => {});
        process.exit(0);
    });
    setTimeout(() => process.exit(1), 30000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
