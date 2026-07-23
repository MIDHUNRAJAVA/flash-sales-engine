// Canonical JSON log line (Phase 4). Every line carries trace_id/span_id lifted
// from the active OTel span so logs and traces are two views of one graph.
const pino = require('pino');
const crypto = require('crypto');
const { trace, context } = require('@opentelemetry/api');

const SERVICE = process.env.OTEL_SERVICE_NAME || 'gateway';
const VERSION = process.env.SERVICE_VERSION || '0.0.0';

// PII policy: user_id is sha256-hashed before it can reach the encoder.
function hashUid(u) {
    if (u == null) return undefined;
    return 'sha256:' + crypto.createHash('sha256').update(String(u)).digest('hex').slice(0, 16);
}

// IPs logged as /24 only (mask the last octet).
function maskIp(ip) {
    if (!ip) return undefined;
    const v4 = ip.replace('::ffff:', '');
    const parts = v4.split('.');
    return parts.length === 4 ? `${parts[0]}.${parts[1]}.${parts[2]}.0/24` : 'masked';
}

const base = pino({
    base: { service: SERVICE, version: VERSION },
    timestamp: () => `,"ts":"${new Date().toISOString()}"`,
    messageKey: 'event',
    formatters: {
        level: (label) => ({ level: label.toUpperCase() }),
        // Attach current trace context to every line.
        log: (obj) => {
            const span = trace.getSpan(context.active());
            if (span) {
                const sc = span.spanContext();
                obj.trace_id = sc.traceId;
                obj.span_id = sc.spanId;
            }
            return obj;
        },
    },
    redact: { paths: ['email', 'name', 'authorization', 'password'], remove: true },
});

module.exports = { logger: base, hashUid, maskIp };
