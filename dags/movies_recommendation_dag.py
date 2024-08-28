from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import json

# Load the configuration file
with open("./config/gcp_constants.json", "r") as f:
    config = json.load(f)

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
    "movie_recommend_system_pipeline",
    default_args=default_args,
    description="Recommendation system (content based) pipeline for trending tmdb movies .",
    schedule_interval=timedelta(days=7),
    catchup=False,
) as dag:

    # Task 1: Check, if fct_trending_movies table exist.
    check_table_existence = BigQueryCheckOperator(
        task_id="check_table_existence",
        sql=f"""
            SELECT COUNT(1)
            FROM  `{config['PROJECT_ID']}.{config["GCP_DATASET_NAME"]}.INFORMATION_SCHEMA.TABLES`
            WHERE table_name = 'fct_trending_movies'
            """,
        use_legacy_sql=False,
        location=config['GCP_REGION'],  # location oF BigQuery dataset
        gcp_conn_id='google_cloud_default', #connection between airflow and google cloud platform define in docker compose file
        retries=0
    )
    # Task 2: Trigger run_tmdb_etl_dag if previous task failed
    run_tmdb_etl_dag = TriggerDagRunOperator(
        task_id="run_tmdb_etl_dag",
        trigger_dag_id="movie_etl_pipeline",
        wait_for_completion=True,
        trigger_rule='all_failed', #all_failed - in our case only one previous failed then run this task
        poke_interval=60,
        retries=0
    )
    # Task 3: Trigger to rerun this DAG when Task 2 finished
    rerun_dag = TriggerDagRunOperator(
        task_id="rerun_dag",
        trigger_dag_id=dag.dag_id,
        wait_for_completion=True,
        poke_interval=60,
        retries=0
    )
    # Task 4: Running pyspark script with based content recommendation system for our trending movies 
    build_recommendation_system =SparkSubmitOperator(
        task_id="build_recommendation_system",
        application="./scripts/recommendation_system.py",
        conn_id='spark_default',
        jars='https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar,https://storage.googleapis.com/spark-lib/bigquery/spark-3.3-bigquery-0.40.0.jar',
        executor_cores =4,
        executor_memory="4g",
        application_args=[config["PROJECT_ID"],config["GCP_BUCKET_NAME"],config["GCP_DATASET_NAME"],"fct_trending_movies"],
    )

    # Task 5: Load top 10 recommended movies data from GCS to BigQuery in parquet format. This operator also create a BigQuery Table if needed.
    load_recommendation_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_recommendation_gcs_to_bq',
    bucket=config["GCP_BUCKET_NAME"],
    source_objects=[f"analysis/movie_recommendations_top3_{datetime.now().strftime('%Y%m%d')}.parquet/*"],
    location=config["GCP_REGION"],
    destination_project_dataset_table=f"{config['PROJECT_ID']}.{config["GCP_DATASET_NAME"]}.fct_top_3_recommended_movies",
    source_format='PARQUET',
    create_disposition='CREATE_IF_NEEDED',  #This tells BigQuery to create the table if it doesn't exist.
    write_disposition='WRITE_TRUNCATE', #This parameter value mean operator deletes all existing data and replaces it with the new data.
    autodetect=True,
    
    )

check_table_existence >> [build_recommendation_system,run_tmdb_etl_dag] #These are two conflicting tasks (trigger rule from task 2), so they can be triggered simultaneously,because only one of them will actually be executed.
build_recommendation_system >> load_recommendation_gcs_to_bq
run_tmdb_etl_dag >> rerun_dag
