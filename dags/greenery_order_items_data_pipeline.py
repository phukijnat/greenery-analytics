import csv
import json

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowSkipException
from airflow.utils import timezone

from google.cloud import bigquery, storage
from google.oauth2 import service_account

from airflow.providers.postgres.hooks.postgres import PostgresHook


BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
GCP_PROJECT_ID = "dataengineer-bootcamp"
DAGS_FOLDER = "/opt/airflow/dags"
DATA = "order_items"
bucket_name = "deb-bootcamp-37"


def _extract_data():
    pg_hook = PostgresHook(
        postgres_conn_id="postgres_source",
        schema="greenery"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    sql = f"""
        SELECT order_id, product_id, quantity
        FROM {DATA}
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    if not rows:
        raise AirflowSkipException("No data found in Postgres for today. Skipping...")

    with open(f"{DAGS_FOLDER}/extracted_data/{DATA}.csv", "w") as f:
        writer = csv.writer(f)
        header = [
            "order_id",
            "product_id",
            "quantity"
        ]
        writer.writerow(header)
        writer.writerows(rows)


def _load_data_to_gcs():
    hook = GCSHook(gcp_conn_id="load_data_to_gcs")
    file_path = f"{DAGS_FOLDER}/extracted_data/{DATA}.csv"
    destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"
    hook.upload(
        bucket_name=bucket_name,
        object_name=destination_blob_name,
        filename=file_path,
    )


def _load_data_from_gcs_to_bigquery():
    hook = BigQueryHook(gcp_conn_id="load_data_from_gcs_to_bigquery")
    DATASET_ID = "greenery_dataset"

    hook.create_empty_dataset(
        project_id=GCP_PROJECT_ID,
        dataset_id=DATASET_ID,
        location=LOCATION,
        exists_ok=True,
    )

    job_config = {
        "load": {
            "destinationTable": {
                "projectId": GCP_PROJECT_ID, 
                "datasetId": DATASET_ID,
                  "tableId": DATA
            },
            "sourceFormat": "PARQUET",
            "writeDisposition": "WRITE_TRUNCATE",
            "sourceUris": [f"gs://{bucket_name}/cleaned/{BUSINESS_DOMAIN}/{DATA}/*.parquet"],
        }
    }
    job = hook.insert_job(configuration=job_config)
    job.result()

    table_metadata = hook.get_client().get_table(f"{GCP_PROJECT_ID}.{DATASET_ID}.{DATA}")
    print(f"Loaded {table_metadata.num_rows} rows and {len(table_metadata.schema)} columns to {GCP_PROJECT_ID}.{DATASET_ID}.{DATA}")


default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2026, 3, 1),
}
with DAG(
    dag_id="greenery_order_items_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["greenery","data_pipeline"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
    )

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs
    )
    
    # Submit a Spark app to transform data
    transform_data = SparkSubmitOperator(
        task_id="transform_data",
        application=f"/opt/spark/pyspark/transform_{DATA}.py",
        conn_id="my_spark",
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> transform_data >> load_data_from_gcs_to_bigquery