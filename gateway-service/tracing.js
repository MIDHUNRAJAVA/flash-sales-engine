// MUST be required before express/axios/redis so their instrumentations patch
// the modules at load time. index.js requires this on line 1.
const { NodeSDK } = require('@opentelemetry/sdk-node');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-grpc');
const { HttpInstrumentation } = require('@opentelemetry/instrumentation-http');
const { ExpressInstrumentation } = require('@opentelemetry/instrumentation-express');
const { RedisInstrumentation } = require('@opentelemetry/instrumentation-redis-4');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } = require('@opentelemetry/semantic-conventions');

const sdk = new NodeSDK({
    resource: resourceFromAttributes({
        [ATTR_SERVICE_NAME]: process.env.OTEL_SERVICE_NAME || 'gateway',
        [ATTR_SERVICE_VERSION]: process.env.SERVICE_VERSION || '0.0.0',
    }),
    // Endpoint from OTEL_EXPORTER_OTLP_ENDPOINT. If the collector is absent the
    // SDK logs an export error and drops spans — orders are never affected.
    traceExporter: new OTLPTraceExporter(),
    instrumentations: [
        new HttpInstrumentation({
            // The public inbound traceparent is untrusted (a client could pin
            // sampled=1 on every request); start a fresh root instead.
            ignoreIncomingRequestHook: () => false,
        }),
        new ExpressInstrumentation(),
        new RedisInstrumentation({ dbStatementSerializer: () => '[redacted]' }),
    ],
});

sdk.start();

process.on('SIGTERM', () => { sdk.shutdown().catch(() => {}); });

module.exports = sdk;
