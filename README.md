# Medalliion Sample MTA Live Feed

## Preamble
This project is first and foremost an explorative project for getting a first hand and testing the capabilities of Data Engineering technologies.
As such, the proposed platform of this repository is intended to run only in a **local/Development environment**, and is quite far from a state-of-the-art implementation of a Data Lakehouse Platform.

## Project Description
This project objective is to consume the [MTA Subway public API Realtime Feeds](https://api.mta.info/#/subwayRealTimeFeeds), process and store the data in a Lakehouse.

In order to do so, the following containerized Data Platform was developed:
- A Producer Application developed in Python producing Events from the different realtime feeds and publishing them to Kafka topics
- A Kafka instance for handling events streaming and enabling subscribers to consume events from the different topics
- A Spark instance, the distributed computing engine for analystics, for executing Data Jobs
- An S3 comptaible Object Storage.
  In the proposed stack, Minio is used and needs to be replaced as the project is not longer supported.
  The use of Spark facilitates the implementation of a Lakehouse Platform through [Apache Hudi](https://hudi.apache.org/docs/overview). As such, all the data written in the Storage are available in Hudi Tables
- The following Spark Jobs, based on the Data Medallion Architecture, are written in Python and are submitted to the Spark Mater Node for processing
  - A Bronze Streaming Job: for ingesting Events from Kafka topics and saving raw data to an Object Store
  - A Silver Streaming Job for cleaning the raw data: in our case, a flattening of an array of objects into separate rows
  - A Gold Job for aggregating the cleaned data: in our case, a simple count of the number of trips per lane and the average delay in 10 minutes buckets. This job is scheduled to run every hour.
- An Airflow instance for orchestrating jobs, in particular the Gold Job

# Technical Architecture
![Technical Architecture](/docs/architecture.drawio.png)

# Technical Stack
- Apache Spark
- Confluent Kafka and Confluent Zookeeper
- Minio
- Apache Airflow with a required Postgres DB to run
- Python 3 with the following packages:
    - pydantic for handling Environment Variables
    - pyspark for building Spark jobs
    - httpx for a simple HTTP client with asynchronism implementation
    - confluent-kafka for subscribing/publishing events to Kafka
    - gtfs-realtime-bindings for facilitating the conversion from GTFS protobuf format into objects

*Due to compatibility issues between the different components of the stack, older versions are used in order to simplify the implementation*

## Pre-Requisite
- [Docker Desktop](https://docs.docker.com/desktop/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Installation
```
docker compose up -d
```
