# Projects decription

This repository contains DE projects.

Tech stack:
- **Postgres**
- **Hadoop**
- **pySpark**
- **Kafka**
- **Spark Streaming**
- **Airflow**
- **MongoDb**
- **Vertica**
- **Docker**
- **Kubernetes**
- **Redis**

# Brief information about the projects:

| Name                                           | Description                                                                                               | Tech stack                                            |
|----------------------------------------------------|---------------------------------------------------------------------------------------------------------|-------------------------------------------------|
| **[Microservice architecture and cloud technologies](https://github.com/SomeBadDecisions/Data_engineering/tree/main/microservices_pg_py_kube/)** | Deploying cloud infrastructure and developing microservices for creating a Data Warehouse from STG, DDS, and CDM layers | Docker, Kubernetes,   Python, PostgreSQL, Redis |
| **[DataLake architecture with Hadoop](https://github.com/SomeBadDecisions/Data_engineering/tree/main/pyspark-hadoop-datalake)**                              | Development of a data lake based on Apache Hadoop and automation of layer filling through Apache Airflow   Airflow | PySpark, Hadoop,   Airflow                      |
| **[Data stream processing with SparkStreaming](https://github.com/SomeBadDecisions/Data_engineering/tree/main/kafka_sparkstreaming_pipeline)**     | Receiving and processing messages from Kafka, followed by sending them to Postgres and a new Kafka topic         | Kafka,   SparkStreaming, PySpark, PostgreSQL    |
| **[Data Vault with Vertica](https://github.com/SomeBadDecisions/Data_engineering/tree/main/vertica-data-vault)**                            | Getting data from Amazon S3 and creating a Data Warehouse using the Data Vault model on the analytical database Vertica     | Vertica, Python,   Airflow                      |
| **[Building an ETL pipeline](https://github.com/SomeBadDecisions/Data_engineering/tree/main/postgres-mongo-etl-snowflake)**                      | Getting data through API, their subsequent processing and saving in Postgres                              | MongoDB, PostgreSQL,   Python, Airflow          |
| **[Changing an ETL pipeline](https://github.com/SomeBadDecisions/Data_engineering/tree/main/postgres-airflow-pipeline-update)**                        | Changing the existing data pipeline for DWH in Postgres                               | Airflow, Python,   Postgres                     |
| **[DWH refactoring](https://github.com/SomeBadDecisions/Data_engineering/tree/main/postgres-datamodel-refactoring)**                             | Changing the existing DWH structure in Postgres                                                      | Postgres                                        |
| **[Simple datamart with Postgres](https://github.com/SomeBadDecisions/Data_engineering/tree/main/postgres-rfm-table)**                              | RFM-datamart with Postgres                                                                   | Postgres                                        |
