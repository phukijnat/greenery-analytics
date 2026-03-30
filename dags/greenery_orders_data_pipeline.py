import csv
import json

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
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
DATA = "orders"
bucket_name = "deb-bootcamp-37"


def _extract_data(ds):
    pg_hook = PostgresHook(
        postgres_conn_id="postgres_source",
        schema="greenery"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    sql = f"""
            SELECT                 
                order_id,
                user_id,
                promo_id,
                address_id,
                created_at,
                order_cost,
                shipping_cost,
                order_total,
                tracking_id,
                shipping_service,
                estimated_delivery_at,
                delivered_at,
                status
            FROM {DATA}
            WHERE created_at >= '{ds} 00:00:00' AND created_at <= '{ds} 23:59:59'
        """
    cursor.execute(sql)
    rows = cursor.fetchall()
    if not rows:
        raise AirflowSkipException("No data found in Postgres for today. Skipping...")

    if rows:
        with open(f"{DAGS_FOLDER}/extracted_data/{DATA}-{ds}.csv", "w") as f:
            writer = csv.writer(f)
            header = [
                "order_id",
                "user_id",
                "promo_id",
                "address_id",
                "created_at",
                "order_cost",
                "shipping_cost",
                "order_total",
                "tracking_id",
                "shipping_service",
                "estimated_delivery_at",
                "delivered_at",
                "status"
            ]
            writer.writerow(header)
            writer.writerows(rows)
        return "load_data_to_gcs"
    else:
        return "do_nothing"

def _load_data_to_gcs(ds):
    hook = GCSHook(gcp_conn_id="load_data_to_gcs")
    file_path = f"{DAGS_FOLDER}/extracted_data/{DATA}-{ds}.csv"
    destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}.csv"
    hook.upload(
        bucket_name=bucket_name,
        object_name=destination_blob_name,
        filename=file_path,
    )


def _load_data_from_gcs_to_bigquery(ds):
    hook = BigQueryHook(gcp_conn_id="load_data_from_gcs_to_bigquery")
    DATASET_ID = "greenery_dataset"
    partition = ds.replace("-", "")

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
                  "tableId": f"{DATA}${partition}",
            },
            "sourceFormat": "PARQUET",
            "writeDisposition": "WRITE_TRUNCATE",
            "sourceUris": [f"gs://{bucket_name}/cleaned/{BUSINESS_DOMAIN}/{DATA}/{ds}/*.parquet"],
            "timePartitioning": {
                "type": "DAY",
                "field": "created_at",
            },
        }
    }
    job = hook.insert_job(configuration=job_config)
    job.result()

    table_metadata = hook.get_client().get_table(f"{GCP_PROJECT_ID}.{DATASET_ID}.{DATA}")
    print(f"Loaded {table_metadata.num_rows} rows and {len(table_metadata.schema)} columns to {GCP_PROJECT_ID}.{DATASET_ID}.{DATA}")


default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2021, 2, 10),
    "end_date": timezone.datetime(2021, 2, 12),
}
with DAG(
    dag_id="greenery_orders_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["greenery","data_pipeline"],
):

    # Extract data from Postgres
    extract_data = BranchPythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
    )

    do_nothing = EmptyOperator(task_id="do_nothing")

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs,
    )
    
    # Submit a Spark app to transform data
    transform_data = SparkSubmitOperator(
        task_id="transform_data",
        application=f"/opt/spark/pyspark/transform_{DATA}.py",
        conn_id="my_spark",
        env_vars={"EXECUTION_DATE": "{{ ds }}"}
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,
    )

    end = EmptyOperator(task_id="end", trigger_rule="one_success")

    # Task dependencies
    extract_data >> load_data_to_gcs >> transform_data >> load_data_from_gcs_to_bigquery >> end
    extract_data >> do_nothing >> end