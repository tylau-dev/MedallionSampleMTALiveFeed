# MTA Real-Time Lakehouse

## Project Overview
This project is a Proof of Concept (PoC) designed to explore the implementation of a scalable Data Lakehouse using a Medallion Architecture, to process real-time [MTA Subway public API](https://api.mta.info/#/subwayRealTimeFeeds) feeds through distinct quality layers and provide analytical insights.

The platform is containerized and optimized for a Local/Development environment, demonstrating the integration of stream processing.

## Technical Architecture
![Technical Architecture](/docs/architecture.drawio.png)

### Technical Stack
- Processing: Apache Spark (PySpark)
- Storage: Apache Hudi (Lakehouse capabilities, ACID transactions)
- Event Streaming: Confluent Kafka & Zookeeper
- Orchestration: Apache Airflow (with Postgres backend)
- Object Storage: MinIO (S3-Compatible)
- Language: Python 3 (Pydantic, HTTPX, confluent-kafka, gtfs-realtime-bindings)

**Note**: Due to component versioning requirements, specific legacy versions are utilized to ensure stack compatibility and simplify the local implementation.

### Data Pipeline flow
1. **Producer App**: Python Producer Application fetches GTFS Realtime feeds and publishes to **Kafka**.
2. **Bronze (Raw)**: A **Spark Structured Streaming** job consumes Kafka events and persists them as raw data in **Minio (S3)** using **Apache Hudi**.
3. **Silver Layer (Cleansed)**: A **Spark** streaming job that performs data modeling and flattening (e.g., exploding subway trip stop arrays into individual rows) to prepare the data for analysis
4. **Gold Layer (Aggregated)**: An **Airflow-scheduled Spark job** runs hourly to aggregate average delays and train frequency in 10-minute buckets.

## Installation & Deployment

### Pre-Requisites

- [Docker Desktop](https://docs.docker.com/desktop/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Clone the repository

```bash
git clone https://github.com/tylau-dev/MedallionSampleMTALiveFeed
```

### Environment variables

The Docker stack and all Python applications share a unified set of configuration variables.
Create a `.env` file in the root directory using the template below to initialize the environment

```
ZOOKEEPER_CLIENT_PORT=2181
ZOOKEEPER_TICK_TIME=2000

KAFKA_HOST_PORT=9092
KAFKA_BROKER_ID=1
KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
KAFKA_TOPIC_NAME=mta_subway

KAFKA_UI_PORT=8085
KAFKA_UI_CLUSTER_NAME=local
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181

MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9095
MINIO_ROOT_USER=
MINIO_ROOT_PASSWORD=

SPARK_WORKER_CORES=4
SPARK_WORKER_MEMORY=4g
SPARK_MASTER_PORT=7077

S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY=
S3_SECRET_KEY=

S3_BRONZE_CHECKPOINT_LOCATION=s3a://mta-bronze/checkpoints/
S3_BRONZE_OUTPUT_PATH=s3a://mta-bronze/data/mta_trips

S3_SILVER_CHECKPOINT_LOCATION=s3a://mta-silver/checkpoints/
S3_SILVER_OUTPUT_PATH=s3a://mta-silver/data/mta_trips

S3_GOLD_CHECKPOINT_LOCATION=s3a://mta-gold/checkpoints/
S3_GOLD_OUTPUT_PATH=s3a://mta-gold/data/mta_trips

AIRFLOW_PORT=8086
AIRFLOW_FERNET_KEY=
AIRFLOW_DATABASE_SQL_CONN_STRING=
AIRFLOW_USERNAME=
AIRFLOW_PASSWORD=

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
```


### Installation
```bash
docker compose up -d
```

## Key Takeaways
- Medallion Architecture: Ensures clear separation between raw ingestion and business logic
- Watermarking: Crucial for handling late-arriving data without memory leaks
- Hybrid Orchestration: Uses Docker for submiting continuous streams jobs and Airflow for discrete batch jobs
- Apache Hudi: Provides ACID guarantees and reliable "Upserts" on a Data Lake
- Resiliency: Uses Hudi checkpointing to resume jobs without reprocessing all data

## To-dos
- Production Readiness: Shift from local Docker to managed Cloud services
- IaC: Implement Terraform for automated infrastructure provisioning
- Visualization: Build a Streamlit dashboard or FastAPI to consume Gold-layer data
- Enforce stronger Data Validation in Silver Layer
