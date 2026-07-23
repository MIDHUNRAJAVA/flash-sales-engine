// Back-pressure signal (Phase 3e). order-service publishes its consumer lag to
// core-NATS `control.orders.lag` every 1s. The gateway subscribes; when lag
// stays above T (or beacons go silent — absence is treated as a bad signal),
// /buy is routed to the waiting room. Signal latency <= ~2s.
const { connect, JSONCodec } = require('nats');

const LAG_SUBJECT = 'control.orders.lag';

class BackPressure {
    constructor({ natsUrl, threshold = 1000, consecutive = 5, staleMs = 5000 }) {
        this.natsUrl = natsUrl;
        this.threshold = threshold;
        this.consecutive = consecutive;
        this.staleMs = staleMs;
        this.lastLag = 0;
        this.lastTs = 0;      // set to Date.now() only after the first beacon
        this.overCount = 0;
        this.nc = null;
        this._active = false;
    }

    async start() {
        this.nc = await connect({ servers: this.natsUrl, reconnect: true, maxReconnectAttempts: -1 });
        const jc = JSONCodec();
        (async () => {
            const sub = this.nc.subscribe(LAG_SUBJECT);
            for await (const m of sub) {
                try {
                    const { lag } = jc.decode(m.data);
                    this.lastLag = lag;
                    this.lastTs = Date.now();
                    this.overCount = lag > this.threshold ? this.overCount + 1 : 0;
                } catch { /* ignore malformed beacons */ }
            }
        })().catch(() => {});
    }

    // Route to the waiting room if lag is sustained-high, OR beacons have gone
    // stale after we'd started hearing them (consumer likely dead).
    shouldQueue() {
        if (this.overCount >= this.consecutive) return true;
        if (this.lastTs > 0 && Date.now() - this.lastTs > this.staleMs) return true;
        return false;
    }

    async close() { if (this.nc) await this.nc.drain(); }
}

module.exports = { BackPressure };
