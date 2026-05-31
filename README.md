# GPU Cluster Health & Utilization Streaming Monitor

A real-time data engineering pipeline that simulates, processes, and visualizes
GPU telemetry from a 12-GPU compute cluster built to demonstrate production-grade
streaming architecture using Apache Kafka, PySpark Structured Streaming, PostgreSQL,
and Streamlit.

---

## Architecture

```mermaid
flowchart TD
    A["🖥️ GPU Telemetry Simulator\nPython · Kafka Producer\n12 GPUs · 1 event/sec/GPU"] -->|"Apache Kafka\nTopic: gpu-telemetry\n12 events/sec"| B

    B["⚡ PySpark Structured Streaming\nMicro-batch every 10 seconds\nforeachBatch processor"]

    B --> C1["🌡️ Thermal Throttle Detection\nmax_temp > 83C AND util < 60%"]
    B --> C2["📊 Efficiency Scoring\n0-100 score per GPU"]
    B --> C3["💤 Underutilization Flagging\navg_util < 25%"]
    B --> C4["🔍 Anomaly Detection\nZ-score across cluster"]

    C1 & C2 & C3 & C4 --> D["🐘 PostgreSQL 15\nThree-tier storage"]

    D --> D1[("gpu_raw_telemetry\nEvery reading")]
    D --> D2[("gpu_analytics\nPer-batch aggregates")]
    D --> D3[("gpu_alerts\nFlagged events only")]

    D1 & D2 & D3 --> E["📈 Streamlit Dashboard\nAuto-refresh every 10s"]

    E --> E1["GPU Health Map"]
    E --> E2["Efficiency Rankings"]
    E --> E3["Thermal Alert Feed"]
    E --> E4["Rolling Time Series"]
```

---

## Features

- **Real-time ingestion** — Kafka producer emits correlated GPU telemetry (temperature rises with utilization, power follows load, errors cluster under stress)
- **Streaming analytics** — PySpark foreachBatch processes 120 rows every 10 seconds
- **Four detection algorithms** running every micro-batch:
  - Thermal throttling (temp threshold + utilization drop)
  - Efficiency scoring (weighted: utilization 50%, memory bandwidth 50%)
  - Underutilized node detection
  - Cluster-level anomaly detection via z-score
- **Three-tier storage** — raw, aggregated, and alerts-only tables in PostgreSQL
- **Live dashboard** — 5-panel Streamlit UI with Plotly charts, auto-refresh

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message Broker | Apache Kafka 7.5 + Zookeeper |
| Stream Processing | PySpark 3.5.1 Structured Streaming |
| Storage | PostgreSQL 15 |
| Dashboard | Streamlit 1.32 + Plotly |
| Orchestration | Docker Compose |
| Language | Python 3.11 |

---

## Project Structure
gpu-cluster-monitor/
├── producer/
│   ├── simulator.py          # GPU telemetry Kafka producer
│   ├── Dockerfile
│   └── requirements.txt
├── spark_consumer/
│   ├── consumer.py           # PySpark Structured Streaming analytics
│   ├── Dockerfile
│   └── requirements.txt
├── dashboard/
│   ├── app.py                # Streamlit live dashboard
│   ├── Dockerfile
│   └── requirements.txt
├── postgres/
│   └── init/
│       └── 01_schema.sql     # Auto-applied schema on first run
├── config/
│   └── kafka_config.py       # Shared constants
├── docker-compose.yml
└── .env
---

## Quick Start

**Prerequisites:** Docker Desktop, Git

```bash
git clone https://github.com/harshvekaria/gpu-cluster-monitor.git
cd gpu-cluster-monitor

# Start infrastructure
docker compose up -d zookeeper kafka postgres

# Wait 20 seconds, then start producer
docker compose up -d kafka-ui producer

# Wait 15 seconds for topic creation, then start consumer and dashboard
docker compose up -d --build spark-consumer dashboard
```

Open in browser:

| Service | URL |
|---|---|
| Streamlit Dashboard | http://localhost:8501 |
| Kafka UI | http://localhost:8080 |

---

## Dashboard Panels

| Panel | Description |
|---|---|
| Cluster KPIs | Total GPUs, avg utilization, avg temp, alert counts |
| GPU Health Map | 12-card grid colored by health status |
| Efficiency Rankings | Horizontal bar chart ranked by efficiency score |
| Temperature Time Series | Rolling 5-min lines with throttle threshold |
| Recent Alerts | Live feed of CRITICAL / WARNING / LOW_UTIL events |
| Utilization Bar Chart | All 12 GPUs with under/overload thresholds |

---

## Analytics Logic

### Efficiency Score (0-100)
score = (avg_utilization x 0.50) + ((avg_mem_bandwidth / 900 GB/s) x 100 x 0.50)

### Thermal Throttling
CRITICAL if max_temp > 83C AND avg_utilization < 60%

### Anomaly Detection
WARNING if (gpu_util - cluster_mean) / cluster_stddev > 2.5

### Underutilization
LOW_UTIL if avg_utilization < 25%

---

## Key Design Decisions

**Why Kafka + PySpark over Spark alone?**
Kafka decouples producers from consumers. The telemetry simulator runs independently,
and multiple consumers can subscribe to the same topic simultaneously.

**Why foreachBatch instead of native streaming sinks?**
foreachBatch gives full DataFrame API access inside each micro-batch, enabling
multi-table writes (raw + analytics + alerts) in a single pass with custom logic
per table, which is not possible with native sinks alone.

**Why three PostgreSQL tables?**
Raw telemetry preserves every reading for replay and audit. Analytics stores
aggregated results for fast dashboard queries. Alerts is a filtered subset
enabling instant alert lookups without scanning millions of raw rows.

---

## Author

**Harsh Vekaria**
MS Software Engineering -- University of Texas at Arlington

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat&logo=linkedin&logoColor=white)](https://linkedin.com/in/harshvekaria)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=flat&logo=github&logoColor=white)](https://github.com/harshvekaria)