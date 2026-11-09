# Logistics Reliability Platform — starter implementation

A local-first learning project with four FastAPI services, Redis, PostgreSQL and OpenTelemetry.

## Architecture

- **Frontend API**: customer-facing facade; calls Rating, Shipment and Tracking over REST.
- **Rating API**: calculates AIR / GROUND / EXPRESS options and stores short-lived rate quotes in Redis.
- **Shipment API**: validates a selected rate with Rating API; creates shipment + outbox row in one PostgreSQL transaction.
- **Tracking API**: consumes `shipment.events` from Redis Streams and stores tracking events; tracking reads do not call Shipment API.
- **Redis**: rate cache and Redis Streams transport.
- **PostgreSQL**: durable shipments, outbox records and tracking events.
- **OpenTelemetry**: request metrics + traces in each API. `/metrics` is scraped by Prometheus.
- **OTel Collector + Jaeger**: receive and display traces.
- **Prometheus + Grafana**: scrape and display metrics.

This is an educational starter, not a production-certified logistics system. Before production, add authentication/authorization, secrets management, schema migrations, rate signing, idempotency, outbox leasing/locking, dead-letter handling, event replay, TLS, resource limits, and robust integration tests.

## Requirements

- macOS (Apple Silicon or Intel)
- Docker Desktop with Kubernetes optional
- Docker Compose
- `kubectl` alias `k` if you prefer it

## Run locally with Docker Compose

From this directory:

```bash
docker compose up --build
```

Endpoints:
- Frontend facade: http://localhost:8000
- Rating API: http://localhost:8001
- Shipment API: http://localhost:8002
- Tracking API: http://localhost:8003
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin / admin)
- Jaeger UI: http://localhost:16686

Swagger docs:
- `/docs` on each API.

## Example workflow

1. Get rates:

```bash
curl -X POST http://localhost:8000/rates \
  -H 'content-type: application/json' \
  -d '{"origin_postal_code":"27529","destination_postal_code":"10001","weight_lb":10}'
```

2. Copy one returned `rate_id`, then create shipment:

```bash
curl -X POST http://localhost:8000/shipments \
  -H 'content-type: application/json' \
  -d '{"rate_id":"PASTE_RATE_ID","sender_name":"Sender","receiver_name":"Receiver","origin_postal_code":"27529","destination_postal_code":"10001","weight_lb":10}'
```

3. Use the returned `tracking_number`:

```bash
curl http://localhost:8000/tracking/PASTE_TRACKING_NUMBER
```

Tracking is asynchronous. The first lookup may briefly show `PENDING`; retry after the outbox publisher and tracking consumer process the event.

## Failure simulation

For local experiments, set environment variables on a service in `docker-compose.yml` and restart it:

- `FAILURE_MODE=latency` — add artificial delay.
- `FAILURE_MODE=error` — return HTTP 503 for API requests.
- `FAILURE_DELAY_SECONDS=2.5` — latency duration.

Do not enable these settings in production.

## Metrics and traces

Each API exposes:
- `http.server.request.count` (Counter)
- `http.server.request.duration` (Histogram, seconds)
- `http.server.active_requests` (UpDownCounter)

Prometheus scrapes each service's `/metrics` endpoint. Trace spans are exported over OTLP/gRPC to the OTel Collector, which forwards them to Jaeger.

Useful PromQL examples:

```promql
sum(rate(http_server_request_count_total[5m])) by (service_name)
```

```promql
histogram_quantile(
  0.95,
  sum by (le, service_name) (rate(http_server_request_duration_seconds_bucket[5m]))
)
```

```promql
sum(rate(http_server_request_count_total{http_status_code=~"5.."}[5m])) by (service_name)
```

Metric names may be normalized by the Prometheus exporter. Inspect `/metrics` if your installed SDK version renders a different suffix.

## Kubernetes

The `k8s/` directory contains namespace, ConfigMap, Deployments, Services, PostgreSQL, Redis, OTel Collector, Prometheus, Jaeger and Grafana manifests.

Build images locally from the repository root:

```bash
docker build --build-arg SERVICE=frontend -t logistics-frontend:dev .
docker build --build-arg SERVICE=rating -t logistics-rating:dev .
docker build --build-arg SERVICE=shipment -t logistics-shipment:dev .
docker build --build-arg SERVICE=tracking -t logistics-tracking:dev .
```

Then apply:

```bash
k apply -f k8s/
```

For Docker Desktop Kubernetes, `imagePullPolicy: IfNotPresent` lets the cluster use locally built images. If your cluster cannot see local images, push them to a registry and update the image names.

## Project layout

```text
common/                 shared telemetry, middleware and database helpers
services/frontend/      customer-facing API
services/rating/        rate quote API
services/shipment/      shipment API + transactional outbox publisher
services/tracking/      Redis Streams consumer + tracking API
observability/           Prometheus, OTel Collector and Grafana config
k8s/                     Kubernetes manifests
```
