## SQLite Memory Engine Perf

This is just basically a load test for the SQLite memory engine, concurrently accessed by a thin network layer (Golang).
Tests are written using the Python Locust library. It also includes a Prometheus+Grafana setup to measure latency
quantiles and QPS. Use docker compose.

