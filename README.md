# GPU Cluster Health & Utilization Streaming Monitor

A real-time data engineering pipeline that simulates, processes, and visualizes GPU telemetry from a 12-GPU compute cluster using Apache Kafka, PySpark Structured Streaming, PostgreSQL, and Streamlit.

---

## Tech Stack

- **Apache Kafka** — real-time message streaming
- **PySpark Structured Streaming** — micro-batch stream processing
- **PostgreSQL** — three-tier data storage
- **Streamlit + Plotly** — live dashboard
- **Docker Compose** — full container orchestration
- **Python 3.11**

---

## What It Does

- Simulates 12 GPUs emitting telemetry every second (temperature, utilization, power draw, memory bandwidth, error codes)
- Streams all events through Kafka into a PySpark consumer
- PySpark runs four analytics algorithms every 10 seconds:
  - Thermal throttle detection (temp > 83C)
  - Efficiency scoring (utilization + memory bandwidth)
  - Underutilization flagging (util < 25%)
  - Anomaly detection (z-score across the cluster)
- Writes results to PostgreSQL across three tables
- Displays everything on a live Streamlit dashboard that auto-refreshes

---

## Project Structure
gpu-cluster-monitor/
├── producer/           # Kafka producer simulating GPU telemetry
├── spark_consumer/     # PySpark Structured Streaming analytics
├── dashboard/          # Streamlit live dashboard
├── postgres/init/      # PostgreSQL schema (auto-applied on startup)
├── config/             # Shared config constants
├── docker-compose.yml
└── .env

## How to Run

**Prerequisites:** Docker Desktop and Git installed.

**1. Clone the repo**
```bash
git clone https://github.com/harshvekaria/gpu-cluster-monitor.git
cd gpu-cluster-monitor
```

**2. Create the environment file**

Create a `.env` file in the root folder with the following content:
POSTGRES_USER=gpuadmin
POSTGRES_PASSWORD=gpupass123
POSTGRES_DB=gpu_monitor
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=gpu-telemetry
NUM_GPUS=12
**3. Start infrastructure**
```bash
docker compose up -d zookeeper kafka postgres
```

**4. Wait 20 seconds, then start the producer**
```bash
docker compose up -d kafka-ui producer
```

**5. Wait 15 seconds, then start the consumer and dashboard**
```bash
docker compose up -d --build spark-consumer dashboard
```

**6. Open in browser**

| Service | URL |
|---|---|
| Streamlit Dashboard | http://localhost:8501 |
| Kafka UI | http://localhost:8080 |

---

## Dashboard

The Streamlit dashboard includes:

- **Cluster KPIs** — total GPUs, average utilization, average temperature, alert counts
- **GPU Health Map** — 12-card grid colored by health status (green / yellow / orange / red)
- **Efficiency Rankings** — bar chart of all GPUs ranked by efficiency score
- **Temperature Time Series** — rolling 5-minute chart with throttle threshold line
- **Recent Alerts** — live feed of CRITICAL, WARNING, and LOW_UTIL events
- **Utilization Bar Chart** — all 12 GPUs with threshold markers

---

## Author

**Harsh Vekaria**
MS Software Engineering — University of Texas at Arlington
GitHub: https://github.com/harshvekaria
