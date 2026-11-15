# Kafka + Toxiproxy Lab

A local Kafka pipeline with network fault tests and observability. No cloud. Everything runs in Docker Compose.

## Features
- Kafka broker (KRaft mode) with topics created as code.
- Network fault injection with Toxiproxy.
- Tests for message loss, duplicates, and lag recovery.

## Requirements
- Docker with Compose v2

## Pipeline
```
producer --> [proxy A] --> kafka --> [proxy B] --> consumer
```
- Proxy A tests incoming data faults.
- Proxy B tests delivery faults.

## Roadmap
1. Kafka broker and topic init (done)
2. Python producer and consumer with numbered messages
3. Toxiproxy on both hops
4. Pytest fault scenarios: latency, outage, reset, low bandwidth
5. Prometheus and Grafana
6. CI with GitHub Actions