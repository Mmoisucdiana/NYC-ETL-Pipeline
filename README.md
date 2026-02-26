NYC ETL Pipeline

This project implements a production-ready ETL pipeline for NYC taxi trip data.  
It extracts, transforms, and loads taxi trip data from raw parquet files into a processed format ready for analysis or ML models.


Features

-Data Extraction:Reads specific columns from raw parquet files.
-Data Transformation: Cleans data, computes trip duration and average speed, downcasts numeric columns for memory efficiency.
-Data Loading: Saves processed parquet file.
-Logging:Tracks pipeline steps and memory usage.
-Docker-ready: Can run in containerized environment.
-Airflow DAG: Supports scheduling of ETL pipeline.
-Unit Tests: Tests for extraction and transformation functions.

How to Run Locally:

1.Clone the repository

git clone https://github.com/Mmoisucdiana/NYC-ETL-pipeline.git


2.Create a virtual environment:

python -m venv venv
source venv/bin/activate   -on Linux/Mac
venv\Scripts\activate      -on Windows



3.Install dependencies:

pip install -r requirements.txt


4.Place the raw parquet file:

raw_data/tripdata_2026-01.parquet


5.Run the ETL  pipeline:

python src/main.py


Airflow

airflow/etl_dag.py


#It schedules the ETL pipeline automatically. Adjust DAG parameters as needed.
