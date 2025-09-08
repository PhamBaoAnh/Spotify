# Streamify

A data pipeline built with Kafka, Spark Streaming, dbt, Docker, Airflow, MinIO, ClickHouse, and Superset.

## Description

### Objective

The project will stream events generated from a fake music streaming service (like Spotify) and create a data pipeline that consumes the real-time data. The data coming in would be similar to an event of a user listening to a song, navigating on the website, authenticating. The data would be processed in real-time and stored to the data lake periodically (every one minutes). The 5-minutes batch job will then consume this data, apply transformations, and create the desired tables for our dashboard to generate analytics. The dashboard will be created using Data Studio and will be used to visualize the data.
### Dataset

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

### Architecture

![Image](https://github.com/user-attachments/assets/9f55ecb3-b3c8-40dd-8ccf-2343cdd99613)

### Final Result

![Image](https://github.com/user-attachments/assets/90e728a9-378b-45f5-a0ac-eae4caff2706)

## Setup

