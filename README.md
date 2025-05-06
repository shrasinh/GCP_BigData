# Big Data Processing Projects on Google Cloud Platform

This repository showcases a collection of projects focused on implementing big data processing using **Google Cloud Platform (GCP)** services and complementary technologies such as **Apache Spark** and **Kafka**.

## Project Summaries

### Image Classification with Spark Streaming

* **Directory**: `/image_classification_spark_streaming/`
* **Description**: Real-time image classification pipeline using Apache Spark Streaming.
* **Files**: `image_classification_streaming.py`, `producer.py`, `Report.pdf`

### Line Count with Cloud Run & GCS

* **Directory**: `/line_count_cloud_run_functions_and_gcs/`
* **Description**: Serverless line count implementation using Cloud Run Functions and Cloud Storage.
* **Files**: `main.py`, `Report.pdf`
  
### Line Count with VM & GCS

* **Directory**: `/line_count_vm_and_gcs/`
* **Description**: Line count implementation using Compute Engine VM and Cloud Storage.
* **Files**: `line_counter.py`, `Report.pdf`

### Slowly Changing Dimension (SCD) Type II with PySpark

* **Directory**: `/SCD_Type_II_pyspark/`
* **Description**: Implements SCD Type II using PySpark for handling historical data changes.
* **Files**: `main.py`, `Report.pdf`

### SCD Type II with Spark SQL

* **Directory**: `/SCD_Type_II_spark_sql/`
* **Description**: Similar to the PySpark version, using Spark SQL instead.
* **Files**: `main.py`, `Report.pdf`

### Stock Thresholding with Spark

* **Directory**: `/stock_thresholding_spark/`
* **Description**: Batch-mode stock data filtering using Apache Spark.
* **Files**: `helper.py`, `main.py`, `Report.pdf`

### Streaming with Cloud Run & Pub/Sub

* **Directory**: `/streaming_cloud_run_function_and_pub_sub/`
* **Description**: Real-time data processing pipeline using Cloud Run Functions and Pub/Sub.
* **Files**: `publisher.py`, `subscriber.py`, `Report.pdf`

### Streaming with Kafka & Spark Streaming

* **Directory**: `/streaming_kafka_and_spark_streaming/`
* **Description**: Data pipeline using Apache Kafka and Spark Streaming.
* **Files**: `producer.py`, `consumer.py`, `Report.pdf`

### Stock Analysis with Spark, Kafka, and Pub/Sub

* **Directory**: `/stock_analysis_spark_kafka_google_pub_sub/`
* **Description**: Hybrid batch and streaming stock analysis using Spark, Kafka, and Pub/Sub.
* **Files**: `producer.py`, `consumer.py`, `download_file_from_gdrive.py`, `Report.pdf`

### Hashing with Spark

* **Directory**: `/hashing_spark/`
* **Description**: Data hashing technique implemented with Apache Spark.
* **Files**: `main.py`, `Report.pdf`

### Hyperparameter Tuning with Spark MLlib

* **Directory**: `/hyperparameter_tuning_spark_mllib/`
* **Description**: Hyperparameter tuning of decision tree using Spark MLlib.
* **Files**: `DecisionTreeCrossValidation.ipynb`, `Report.pdf`

## Documentation

Each project includes a comprehensive `Report.pdf` that covers:

* Environment Setup
* Implementation Details
* Results and Outputs

## Technologies Used

### Google Cloud Platform:

* Google Cloud Storage
* Cloud Run Functions
* Compute Engine VMs
* Google Pub/Sub
* Google Dataproc

### Big Data:

* Spark Streaming
* PySpark
* Spark SQL
* Spark MLlib
* Apache Kafka

## Getting Started

To run any project:

1. Ensure you have a GCP account with the required services enabled.
2. Navigate to the desired project directory.
3. Follow the setup and execution instructions in the respective `Report.pdf`.
