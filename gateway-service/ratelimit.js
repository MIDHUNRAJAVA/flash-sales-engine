// Hybrid rate limiter: token bucket (burst shaping) + sliding window (hard
// per-endpoint quota), evaluated atomically in one Lua script, one RTT.
// All timestamps come from Redis TIME — never gateway wall clock, so N gateway
// pods with skewed clocks still enforce one consistent window.
//
// Returns { code, remaining, reset }:
//   code 0 = allowed, 1 = rate_limited (bucket), 2 = quota_exceeded (window)

const LUA = `
local t      = redis.call('TIME')
local now_us = t[1] * 1000000 + t[2]

-- 1. token bucket
local cap  = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local b    = redis.call('HMGET', KEYS[1], 'tok', 'ts_us')
local tok  = tonumber(b[1]) or cap
local ts   = tonumber(b[2]) or now_us
tok = math.min(cap, tok + ((now_us - ts) / 1000000) * rate)
if tok < 1 then
  local reset = t[1] + math.ceil((1 - tok) / rate)
  return { 1, 0, reset }
end

-- 2. sliding window
local win_us = tonumber(ARGV[3]) * 1000000
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now_us - win_us)
if redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[4]) then
  local oldest = tonumber(redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')[2])
  return { 2, math.floor(tok), math.ceil((oldest + win_us) / 1000000) }
end

-- 3. commit both — only after BOTH checks pass (no token burn on window rejection)
redis.call('HSET', KEYS[1], 'tok', tok - 1, 'ts_us', now_us)
redis.call('EXPIRE', KEYS[1], math.ceil(cap / rate) + 1)
redis.call('ZADD', KEYS[2], now_us, now_us)
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]) + 1)
return { 0, math.floor(tok - 1), t[1] + tonumber(ARGV[3]) }
`;

class RateLimiter {
    constructor(redisClient, { bucketCapacity = 5, refillPerSec = 1 } = {}) {
        this.redis = redisClient;
        this.bucketCapacity = bucketCapacity;
        this.refillPerSec = refillPerSec;
        this.sha = null;
    }

    async load() {
        this.sha = await this.redis.scriptLoad(LUA);
    }

    // windowSeconds/windowMax are per-endpoint (e.g. /buy: 1 per 10s).
    async check(userId, endpoint, windowSeconds, windowMax) {
        const keys = [`rl:{${userId}}:bucket`, `rl:{${userId}}:win:${endpoint}`];
        const args = [
            String(this.bucketCapacity), String(this.refillPerSec),
            String(windowSeconds), String(windowMax),
        ];
        let raw;
        try {
            raw = await this.redis.evalSha(this.sha, { keys, arguments: args });
        } catch (err) {
            if (String(err).includes('NOSCRIPT')) {
                await this.load();
                raw = await this.redis.evalSha(this.sha, { keys, arguments: args });
            } else {
                throw err;
            }
        }
        return { code: Number(raw[0]), remaining: Number(raw[1]), reset: Number(raw[2]) };
    }
}

module.exports = { RateLimiter };
