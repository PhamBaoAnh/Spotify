# Streamify

A data pipeline built with Kafka, Spark Streaming, dbt, Docker, Airflow, MinIO, ClickHouse, and Superset.

## Description

### 🎯 Objective
This project streams events generated from a fake music streaming service (similar to Spotify) and builds a complete data pipeline to consume and process real-time data.  

- Events simulate user actions such as listening to a song, navigating the website, or logging in.  
- Data is processed in real time and stored in the data lake every minute.  
- A batch job (every 5 minutes) consumes this data, applies transformations, and creates tables for analytics.  
- The dashboard (built with Superset) visualizes user activity and insights.
  
### 🚀 Features
- Real-time streaming with Kafka + Spark Streaming  
- Automated orchestration with Airflow  
- Modular transformations using dbt  
- Scalable data storage with MinIO (Data Lake) and ClickHouse (Data Warehouse)  
- Interactive dashboards in Superset for analytics  
- Containerized deployment with Docker & Docker Compose  

### 📀 Dataset

[Eventsim](https://github.com/Interana/eventsim) is a program that generates event data to replicate page requests for a fake music web site. Eventsim uses song data from [Million Songs Dataset](http://millionsongdataset.com) to generate events. I have used a [subset](http://millionsongdataset.com/pages/getting-dataset/#subset) of 10000 songs
### Tools & Technologies

- Infrastructure as Code - [**Terraform**](https://www.terraform.io)  
- Containerization - [**Docker**](https://www.docker.com), [**Docker Compose**](https://docs.docker.com/compose/)  
- Stream Processing - [**Kafka**](https://kafka.apache.org), [**Spark Streaming**](https://spark.apache.org/docs/latest/streaming-programming-guide.html)  
- Orchestration - [**Airflow**](https://airflow.apache.org)  
- Transformation - [**dbt**](https://www.getdbt.com)  
- Data Lake - [**MinIO**](https://min.io)  
- Data Warehouse - [**ClickHouse**](https://clickhouse.com)  
- Data Visualization - [**Superset**](https://superset.apache.org)  
- Programming Language - [**Python**](https://www.python.org)  

### 🏗 Architecture

![Image](https://github.com/user-attachments/assets/9f55ecb3-b3c8-40dd-8ccf-2343cdd99613)

### 📊 Final Result

![Image](https://github.com/user-attachments/assets/90e728a9-378b-45f5-a0ac-eae4caff2706)


## ⚙️ Setup

#### Set kafka
Open a new terminal and run the following commands:
```bash
cd kafka
docker-compose build && \
docker-compose up -d
```
After this, you can forward port 9021 to see kafka ui.


#### 🔧Set eventsim 
```bash

docker build -t events:1.0 .
```
To run the eventsim container, run the following command:
```bash

docker run -it --rm --network kafka_spark-network events:1.0 `
  -c examples/example-config.json `
  --start-time "2025-08-26T00:00:00" `
  --end-time "2025-08-26T23:59:59" `
  --nusers 150 `
  --kafkaBrokerList broker:29092 `
```

#### 🔧 Set spark
Open a new terminal and run the following commands:
```bash

cd spark_streaming
docker-compose build && \
docker-compose up -d
```
Run spark streaming job in background:
```bash

docker exec -it spark-master /spark/bin/spark-submit --packages `
  org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.2.0 `
  /opt/stream_all_events.py

```
You can forward port 8088 or 9870 or 18080 to see spark ui. (http://localhost:8088/)

#### 🔧 Set airflow
Open a new terminal and run the following commands:
```bash
cd airflow
docker-compose build && \
docker-compose up -d
```
You can go to http://localhost:8080 to see airflow ui

#### 🔧 Run dags in Airflow
Username: airflow
Password: airflow

Run the following dags in order:

* Step1: load_songs_dag

* Step2: streamify_dag

* Step3: dbt_transform_dag
  

#### 🔧 Set connect superset
Open a new terminal and run the following commands:
```bash
cd superset
docker-compose build && \
docker-compose up -d
```
You can go to http://localhost:8088 to see superset ui
```
clickhousedb+connect://default:@host.docker.internal:8123/streamify_prod
```
### 📊  Workflow

<img width="1457" height="710" alt="Image" src="https://github.com/user-attachments/assets/f801ea0f-c02c-4d2d-a8dd-93e454d34e6a" />
### 📊  Minio Storage
<img width="1905" height="577" alt="Image" src="https://github.com/user-attachments/assets/64305aa8-ecdf-417d-bdaa-2030067273f3" />

### 📊  ClickHouse 
<img width="1900" height="925" alt="Image" src="https://github.com/user-attachments/assets/d9b39be4-b203-45ff-8860-fcf7785e7407" />
