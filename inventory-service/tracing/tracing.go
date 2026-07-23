package tracing

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

// Init wires an OTLP/gRPC trace exporter + global TracerProvider with a W3C
// propagator. The gRPC exporter connects lazily, so an unreachable collector
// never crashes startup — the SDK buffers and drops.
func Init(ctx context.Context, serviceName, version, otlpEndpoint string) (func(context.Context) error, error) {
	// The grpc exporter wants host:port, not a URL scheme.
	endpoint := strings.TrimPrefix(strings.TrimPrefix(otlpEndpoint, "http://"), "https://")

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(endpoint),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", serviceName),
		attribute.String("service.version", version),
	))
	if err != nil {
		return nil, err
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown, nil
}
