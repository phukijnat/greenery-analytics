from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime


TABLES = ['addresses', 'users', 'promos', 'products', 'orders', 'order_items', 'events']

with DAG(
    dag_id='ingest_greenery_csv_to_postgres_v2',
    start_date=datetime(2026, 3, 1),
    schedule='@once',
    catchup=False,
    tags=['greenery', 'ingest']
) as dag:

    prev_task = None

    for table_name in TABLES:
        current_task = SQLExecuteQueryOperator(
            task_id=f'ingest_{table_name}_csv_to_postgres',
            conn_id='postgres_source', 
            sql=f"""
                TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;
                COPY {table_name}
                FROM '/docker-entrypoint-initdb.d/{table_name}.csv'
                DELIMITER ','
                CSV HEADER;
            """
        )
        
        if prev_task:
            prev_task >> current_task
        prev_task = current_task