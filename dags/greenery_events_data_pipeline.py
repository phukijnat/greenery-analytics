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
DATA = "events"
bucket_name = "deb-bootcamp-37"
# keyfile_gcs = f"{DAGS_FOLDER}/deb-uploading-files-to-gcs.json"
# keyfile_bigquery = f"{DAGS_FOLDER}/deb-loading-data-to-bq.json"

def _extract_data(ds):
    pg_hook = PostgresHook(
        postgres_conn_id="postgres_source",
        schema="greenery"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    sql = f"""
            SELECT                 
                event_id,
                session_id,
                page_url,
                created_at,
                event_type,
                user_id,
                order_id,
                product_id
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
                "event_id","session_id","page_url","created_at",
                "event_type","user_id","order_id","product_id",
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
    # service_account_info_gcs = json.load(open(keyfile_gcs))
    # credentials_gcs = service_account.Credentials.from_service_account_info(
    #     service_account_info_gcs
    # )

    # # Load data from Local to GCS
    # bucket_name = "deb-bootcamp-37"
    # storage_client = storage.Client(
    #     project=GCP_PROJECT_ID,
    #     credentials=credentials_gcs,
    # )
    # bucket = storage_client.bucket(bucket_name)

    # file_path = f"{DAGS_FOLDER}/{DATA}-{ds}.csv"
    # destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}.csv"
    # blob = bucket.blob(destination_blob_name)
    # blob.upload_from_filename(file_path)


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
    # service_account_info_bigquery = json.load(open(keyfile_bigquery))
    # credentials_bigquery = service_account.Credentials.from_service_account_info(
    #     service_account_info_bigquery
    # )

    # bigquery_client = bigquery.Client(
    #     project=GCP_PROJECT_ID,
    #     credentials=credentials_bigquery,
    #     location=LOCATION,
    # )

    # job_config = bigquery.LoadJobConfig(
    #     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    #     source_format=bigquery.SourceFormat.PARQUET,
    #     schema=[
    #         bigquery.SchemaField("event_id", bigquery.SqlTypeNames.STRING),
    #         bigquery.SchemaField("session_id", bigquery.SqlTypeNames.STRING),
    #         bigquery.SchemaField("page_url", bigquery.SqlTypeNames.STRING),
    #         bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
    #         bigquery.SchemaField("event_type", bigquery.SqlTypeNames.STRING),
    #         bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
    #         bigquery.SchemaField("order_id", bigquery.SqlTypeNames.STRING),
    #         bigquery.SchemaField("product_id", bigquery.SqlTypeNames.STRING),
    #     ],
    #     time_partitioning=bigquery.TimePartitioning(
    #         type_=bigquery.TimePartitioningType.DAY,
    #         field="created_at",
    #     ),
    # )

    # partition = ds.replace("-", "")
    # bucket_name = "deb-bootcamp-37"
    # destination_blob_name = f"cleaned/{BUSINESS_DOMAIN}/{DATA}/{ds}/*.parquet"
    # table_id = f"{GCP_PROJECT_ID}.deb_bootcamp.{DATA}${partition}"
    # job = bigquery_client.load_table_from_uri(
    #     f"gs://{bucket_name}/{destination_blob_name}",
    #     table_id,
    #     job_config=job_config,
    #     location=LOCATION,
    # )
    # job.result()

    # table = bigquery_client.get_table(table_id)
    # print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2021, 2, 9),
    "end_date": timezone.datetime(2021, 2, 12),
}
with DAG(
    dag_id="greenery_events_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["greenery","data_pipeline"],
):

    # Extract data from Postgres, API, or SFTP
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