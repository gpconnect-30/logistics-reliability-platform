import os
from opentelemetry import metrics, trace
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.metrics.view import (
    View,
    ExplicitBucketHistogramAggregation,
)
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import make_asgi_app
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

def configure_telemetry(app, service_name: str):
    resource = Resource.create({"service.name": service_name, "deployment.environment": os.getenv("ENVIRONMENT", "local")})

    # Metrics: PrometheusMetricReader exposes an in-process /metrics endpoint.
    metric_reader = PrometheusMetricReader()
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[metric_reader],
        views=[
            View(
                instrument_name="http.server.request.duration",
                aggregation=ExplicitBucketHistogramAggregation(
                    boundaries=[
                        0.005, 0.01, 0.025, 0.05,
                        0.1, 0.25, 0.5, 1.0, 2.5, 5.0
                    ]
                ),
            )
        ],
    )
    metrics.set_meter_provider(meter_provider)
    meter = metrics.get_meter(service_name)
    request_count = meter.create_counter("http.server.request.count", unit="1", description="Completed HTTP requests")
    request_duration = meter.create_histogram(
        "http.server.request.duration",
        unit="s",
        description="HTTP request duration",
    )
    active_requests = meter.create_up_down_counter("http.server.active_requests", unit="1", description="In-flight HTTP requests")

    # Traces: OTLP/gRPC -> OTel Collector -> Jaeger.
    tracer_provider = TracerProvider(resource=resource)
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
    trace.set_tracer_provider(tracer_provider)

    FastAPIInstrumentor.instrument_app(app)
    HTTPXClientInstrumentor().instrument()

    @app.middleware("http")
    async def red_metrics(request, call_next):
        if request.url.path in {"/metrics", "/metrics/"}:
            return await call_next(request)
        import time
        start = time.perf_counter()
        method = request.method
        active_requests.add(1, {"http.request.method": method})
        status = "500"
        try:
            response = await call_next(request)
            status = str(response.status_code)
            return response
        finally:
            elapsed = time.perf_counter() - start
            route_obj = request.scope.get("route")
            route = getattr(route_obj, "path", None) or "unmatched"
            attrs = {"http.request.method": method, "http.route": route, "http.response.status_code": status}
            request_count.add(1, attrs)
            request_duration.record(elapsed, attrs)
            active_requests.add(-1, {"http.request.method": method})

    app.mount("/metrics", make_asgi_app())
    return tracer_provider, meter_provider
