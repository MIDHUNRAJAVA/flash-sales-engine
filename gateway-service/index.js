const express = require('express');
const axios = require('axios');
const redis = require('redis');

const app = express();
app.use(express.json());

// Redis Client for Rate Limiting
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = process.env.REDIS_PORT || '6379';
const redisClient = redis.createClient({
    url: `redis://${REDIS_HOST}:${REDIS_PORT}`
});
redisClient.connect().catch(console.error);

const INVENTORY_SERVICE_URL = process.env.INVENTORY_SERVICE_URL || 'http://localhost:8080/buy';

// Middleware: Rate Limiting
async function rateLimit(req, res, next) {
    const userId = req.body.user_id;
    if (!userId) return res.status(400).json({ error: 'user_id required' });

    const key = `rate_limit:${userId}`;
    const requests = await redisClient.incr(key);

    if (requests === 1) {
        await redisClient.expire(key, 60); // 1 minute window
    }

    if (requests > 5) { // Limit 5 requests per minute
        return res.status(429).json({ error: 'Too many requests' });
    }
    next();
}

app.post('/buy', rateLimit, async (req, res) => {
    try {
        // Forward to Go Inventory Service
        const response = await axios.post(INVENTORY_SERVICE_URL, req.body);
        res.status(response.status).json(response.data);
    } catch (error) {
        if (error.response) {
            res.status(error.response.status).json(error.response.data);
        } else {
            res.status(500).json({ error: 'Internal Server Error' });
        }
    }
});

const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Gateway Service running on port ${PORT}`);
});
