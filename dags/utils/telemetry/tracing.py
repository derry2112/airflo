"""Use the runtime OpenTelemetry provider without replacing its configuration."""

from opentelemetry import trace

tracer = trace.get_tracer("payment-and-collection-platform.acquiring")
