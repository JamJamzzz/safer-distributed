// Package telemetry is SAFER's one shared OpenTelemetry bootstrap,
// used by cmd/worker, cmd/coordinator, and cmd/loadgen.
//
// The application depends on the OpenTelemetry API/SDK/exporters here,
// never on a Datadog-specific tracing library: Datadog is configured as
// this deployment's telemetry BACKEND, reached over the OTLP protocol a
// Datadog Agent can ingest directly (see deploy/kubernetes/datadog/), not
// as something application code imports or knows about. Swapping the
// backend for a different OTLP-compatible one needs no code change here.
//
// Observability is deliberately optional. Setup reads the standard
// OTEL_EXPORTER_OTLP_* environment variables itself, rather than letting
// the exporter constructors fall back to their spec-mandated default of
// "localhost:4317": if none of them name an endpoint, Setup does nothing
// -- it does not dial anywhere, does not hard-code a collector address,
// and leaves OpenTelemetry's global TracerProvider/MeterProvider at the
// SDK's own no-op implementations. Every instrumentation call site in
// this repository (otelgrpc stats handlers, the manual spans and metric
// instruments added in Phase 5) is unconditional -- it always calls
// otel.Tracer(...)/otel.Meter(...) -- and is therefore always safe: with
// no endpoint configured, those calls cost a few no-op allocations and
// nothing is ever sent anywhere. A telemetry backend outage after Setup
// HAS configured a real endpoint is likewise not fatal to SAFER: the
// OTLP gRPC exporters here dial non-blocking and their batch processors
// drop and retry in the background rather than blocking the request path
// that generated the telemetry.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// Standard OTel environment variables Setup consults. These are the
// spec's own names (https://opentelemetry.io/docs/specs/otel/protocol/exporter/),
// not something invented for this repository, so any OTLP-compatible
// backend (Datadog Agent included) is configured the same way any other
// OpenTelemetry application would be.
const (
	EnvEndpoint        = "OTEL_EXPORTER_OTLP_ENDPOINT"
	EnvTracesEndpoint  = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
	EnvMetricsEndpoint = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"
)

// shutdownTimeout bounds how long graceful shutdown waits for buffered
// telemetry to flush. It is generous enough for a healthy export to
// finish, and short enough that a stuck or unreachable backend cannot
// meaningfully delay process shutdown.
const shutdownTimeout = 5 * time.Second

// Config describes one process's identity for telemetry purposes.
type Config struct {
	// ServiceName becomes the OpenTelemetry Resource's service.name.
	// This repository uses exactly three: "safer-worker",
	// "safer-coordinator", "safer-loadgen".
	ServiceName string
	// ServiceVersion becomes service.version if non-empty. Left empty
	// when no build-time version is available; the attribute is then
	// omitted entirely rather than sent as a misleading placeholder.
	ServiceVersion string
}

// Shutdown flushes buffered telemetry and releases the exporters Setup
// created, bounded by shutdownTimeout regardless of the context passed
// in -- a caller's own longer deadline (or none at all) must never turn
// into an unbounded wait on a telemetry backend during process shutdown.
type Shutdown func(ctx context.Context) error

// noopShutdown is returned when Setup did not enable anything -- there
// is nothing to flush or close.
func noopShutdown(context.Context) error { return nil }

// Setup configures OpenTelemetry tracing and metrics for this process,
// if and only if the standard OTLP environment variables name an
// endpoint. It always installs a global trace-context propagator
// (W3C traceparent + baggage), even when exporting is disabled, so that
// a request already carrying trace context from an upstream caller
// (loadgen, or a future caller) is not silently dropped at this hop --
// propagation costs nothing whether or not this process itself exports
// anything.
//
// The returned Shutdown must be called during graceful shutdown, after
// this process stops accepting new work but before it exits.
func Setup(ctx context.Context, cfg Config) (Shutdown, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	tracesEndpoint := firstNonEmpty(os.Getenv(EnvTracesEndpoint), os.Getenv(EnvEndpoint))
	metricsEndpoint := firstNonEmpty(os.Getenv(EnvMetricsEndpoint), os.Getenv(EnvEndpoint))
	if tracesEndpoint == "" && metricsEndpoint == "" {
		return noopShutdown, nil
	}

	res, err := buildResource(ctx, cfg)
	if err != nil {
		return noopShutdown, fmt.Errorf("telemetry: building resource: %w", err)
	}

	var shutdownFuncs []func(context.Context) error

	if tracesEndpoint != "" {
		// otlptracegrpc.New reads OTEL_EXPORTER_OTLP_(TRACES_)* itself
		// (endpoint, headers, compression, TLS) for anything not passed
		// as an explicit option; nothing about the endpoint is
		// hard-coded here. The gRPC connection it opens is non-blocking
		// by default, so a misconfigured or unreachable backend does not
		// delay this call or, later, the request path.
		exporter, err := otlptracegrpc.New(ctx)
		if err != nil {
			return noopShutdown, fmt.Errorf("telemetry: creating trace exporter: %w", err)
		}
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
		)
		otel.SetTracerProvider(tp)
		shutdownFuncs = append(shutdownFuncs, tp.Shutdown)
	}

	if metricsEndpoint != "" {
		exporter, err := otlpmetricgrpc.New(ctx)
		if err != nil {
			return noopShutdown, fmt.Errorf("telemetry: creating metric exporter: %w", err)
		}
		mp := metric.NewMeterProvider(
			metric.WithReader(metric.NewPeriodicReader(exporter)),
			metric.WithResource(res),
		)
		otel.SetMeterProvider(mp)
		shutdownFuncs = append(shutdownFuncs, mp.Shutdown)
	}

	return func(_ context.Context) error {
		// Deliberately not derived from the caller's context: shutdown
		// must always get its own bounded window (shutdownTimeout),
		// never inherit a caller's already-cancelled or unbounded one.
		// (context.WithoutCancel would express "keep the values, drop
		// the cancellation" more precisely, but it needs Go 1.21; this
		// repository's go.mod stays at 1.20 -- see go.mod's own
		// comment history -- so a fresh background context is used
		// instead. Shutdown needs no values from the caller anyway.)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		var errs []error
		for _, fn := range shutdownFuncs {
			if err := fn(shutdownCtx); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}, nil
}

// buildResource describes this process to whatever backend receives its
// telemetry: which service it is, which build, which single running
// instance, and (via WithFromEnv, reading OTEL_RESOURCE_ATTRIBUTES /
// OTEL_SERVICE_NAME) whatever deployment-specific attributes an operator
// adds -- deployment.environment in particular is expected to arrive
// this way rather than through a bespoke environment variable, since
// OTEL_RESOURCE_ATTRIBUTES is itself the standard mechanism.
func buildResource(ctx context.Context, cfg Config) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceNameKey.String(cfg.ServiceName),
		// A fresh identifier per process start (not a hostname or PID
		// alone, which can collide or be reused): exactly what
		// service.instance.id means in the semantic conventions.
		semconv.ServiceInstanceIDKey.String(uuid.NewString()),
	}
	if cfg.ServiceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersionKey.String(cfg.ServiceVersion))
	}

	return resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithAttributes(attrs...),
	)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
