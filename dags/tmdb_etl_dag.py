from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import json
import sys

# Load the configuration file
with open("./config/gcp_constants.json", "r") as f:
    config = json.load(f)

sys.path.append("/opt/airflow/scripts")
from extract_data import extract_movie_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "movie_etl_pipeline",
    default_args=default_args,
    description="ETL for movies from tmdb api.",
    schedule_interval=timedelta(days=7),
    catchup=False,
) as dag:

    # Task 1: Create GCS Bucket
    create_gcs_bucket = GCSCreateBucketOperator(
        task_id="create_gcs_bucket",
        bucket_name=config["GCP_BUCKET_NAME"],
        project_id=config["PROJECT_ID"],
        storage_class="REGIONAL",
        location=config["GCP_REGION"],
    )
    # Task 2: Extract movie data from api
    extract_movie_data = PythonOperator(
        task_id="extract_movie_data",
        python_callable=extract_movie_data,
        op_kwargs={"bucket_name": config["GCP_BUCKET_NAME"]},
    )
    # Task 3: Transform movie data by spark submit operator
    transform_movie_data = SparkSubmitOperator(
        task_id="transform_movie_data",
        application="./scripts/transform_data.py",
        conn_id='spark_default', #connection between airflow and spark define in docker compose
        jars='https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar',
        conf={
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "5",
        "spark.dynamicAllocation.maxExecutors": "50"
    },
        executor_cores =4,
        executor_memory="4g",
        application_args=["gs://" + config["GCP_BUCKET_NAME"]], #additional parameter passed to pyspark script
    )

    # Task 4: Create BigQuery Dataset
    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        dataset_id=config["GCP_DATASET_NAME"],
        project_id=config["PROJECT_ID"],
        location=config["GCP_REGION"]
    )


    # Task 5: Load transformed movie parquet data from GCS to BigQuery. This operator also create a BigQuery Table if needed.
    load_transformed_movies_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_transformed_movies_gcs_to_bq',
    bucket=config["GCP_BUCKET_NAME"],
    source_objects=[f"transformed/trending_movies_{datetime.now().strftime('%Y%m%d')}.parquet/*"],
    location=config["GCP_REGION"],
    destination_project_dataset_table=f"{config['PROJECT_ID']}.{config["GCP_DATASET_NAME"]}.fct_trending_movies",
    source_format='PARQUET',
    create_disposition='CREATE_IF_NEEDED',  #This tells BigQuery to create the table if it doesn't exist.
    write_disposition='WRITE_TRUNCATE', #This parameter value mean operator deletes all existing data and replaces it with the new data.
    autodetect=True,
    
    )

    # Task 6: Load transformed movie genres parquet data from GCS to BigQuery. This operator also create a BigQuery Table if needed.
    load_transformed_movie_genres_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_transformed_movie_genres_gcs_to_bq',
    bucket=config["GCP_BUCKET_NAME"],
    source_objects=[f"transformed/movie_genres_{datetime.now().strftime('%Y%m%d')}.parquet/*"],
    location=config["GCP_REGION"],
    destination_project_dataset_table=f"{config['PROJECT_ID']}.{config["GCP_DATASET_NAME"]}.dict_movie_genres",
    source_format='PARQUET',
    create_disposition='CREATE_IF_NEEDED',  #This tells BigQuery to create the table if it doesn't exist.
    write_disposition='WRITE_TRUNCATE', #This parameter value mean operator deletes all existing data and replaces it with the new data.
    autodetect=True,
    
    )

(
    create_gcs_bucket
    >> extract_movie_data
    >> transform_movie_data
    >> create_bq_dataset
    >> [load_transformed_movies_gcs_to_bq, load_transformed_movie_genres_gcs_to_bq] #These are two independent tasks, so they can be called simultaneously
)
