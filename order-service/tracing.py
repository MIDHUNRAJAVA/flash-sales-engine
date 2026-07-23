import os

from opentelemetry import trace
from opentelemetry.propagate import set_global_textmap
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor

from utils.logger import get_logger

logger = get_logger()


def init_tracing(service_name, version, endpoint=None):
    endpoint = endpoint or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
    # grpc exporter wants host:port, not a URL scheme
    for scheme in ("http://", "https://"):
        if endpoint.startswith(scheme):
            endpoint = endpoint[len(scheme):]
            break

    resource = Resource.create({"service.name": service_name, "service.version": version})
    provider = TracerProvider(resource=resource)
    # BatchSpanProcessor buffers and drops on export failure — collector-absent
    # degrades gracefully, never blocks or crashes the hot path.
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
    trace.set_tracer_provider(provider)
    set_global_textmap(TraceContextTextMapPropagator())

    AsyncPGInstrumentor().instrument()
    logger.info("tracing_initialized", endpoint=endpoint, service=service_name, version=version)
