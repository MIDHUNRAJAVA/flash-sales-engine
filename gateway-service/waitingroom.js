// Layer 3 — FIFO virtual queue. Lives on the rate-limit Redis (deliberately),
// so it survives a stock-Redis failover. Arrival order = fairness; bot
// resistance is Layers 0-2's job, not the queue discipline's.
//
// The queue is only *enforced* when back-pressure activates it (see
// backpressure.js). When inactive the admission loop still drains harmlessly.

const { logger } = require('./logger');

// Join: idempotent ZADD with a Redis-TIME score, returns 0-based position.
const JOIN_LUA = `
local t = redis.call('TIME')
local score = t[1] * 1000000 + t[2]
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  redis.call('ZADD', KEYS[1], score, ARGV[1])
  redis.call('EXPIRE', KEYS[1], 3600)
end
return redis.call('ZRANK', KEYS[1], ARGV[1])
`;

class WaitingRoom {
    constructor(redisClient, { saleId = 'S1', admitRate = 500, admittedTtl = 30 } = {}) {
        this.redis = redisClient;
        this.queueKey = `queue:{${saleId}}`;
        this.admittedPrefix = `admitted:{${saleId}}:`;
        this.admitterKey = `admitter:{${saleId}}`;
        this.admitRate = admitRate;
        this.admittedTtl = admittedTtl;
        this.podId = `${process.pid}-${Math.floor(Math.random() * 1e6)}`;
        this._joinSha = null;
        this._timer = null;
    }

    async load() {
        this._joinSha = await this.redis.scriptLoad(JOIN_LUA);
    }

    async join(userId) {
        const pos = await this.redis.evalSha(this._joinSha, {
            keys: [this.queueKey], arguments: [String(userId)],
        });
        return Number(pos); // 0-based
    }

    async position(userId) {
        if (await this.redis.exists(this.admittedPrefix + userId)) {
            return { admitted: true };
        }
        const rank = await this.redis.zRank(this.queueKey, String(userId));
        if (rank === null) return { queued: false };
        // Coarse ETA: place in line / admission rate. A position is a spot in
        // line, never a reservation — the copy must say so.
        return { queued: true, position: rank + 1, eta_seconds: Math.ceil((rank + 1) / this.admitRate) };
    }

    async isAdmitted(userId) {
        return (await this.redis.exists(this.admittedPrefix + userId)) === 1;
    }

    async depth() {
        return this.redis.zCard(this.queueKey);
    }

    // Leader-elected admission: only one pod drains, at admitRate/s.
    startAdmitting(onDepth) {
        this._timer = setInterval(async () => {
            try {
                const won = await this.redis.set(this.admitterKey, this.podId, { NX: true, EX: 5 });
                if (won === null) {
                    const holder = await this.redis.get(this.admitterKey);
                    if (holder !== this.podId) return; // another pod is the admitter
                }
                await this.redis.expire(this.admitterKey, 5); // renew our lease

                const popped = await this.redis.zPopMinCount(this.queueKey, this.admitRate);
                if (popped && popped.length) {
                    const multi = this.redis.multi();
                    for (const { value } of popped) {
                        multi.set(this.admittedPrefix + value, '1', { EX: this.admittedTtl });
                    }
                    await multi.exec();
                }
                if (onDepth) onDepth(await this.depth());
            } catch (err) {
                logger.error({ event: 'admit_loop_error', error: err.message });
            }
        }, 1000);
        this._timer.unref();
    }

    stop() { if (this._timer) clearInterval(this._timer); }
}

module.exports = { WaitingRoom };
