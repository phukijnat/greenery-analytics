from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id='ingest_greenery_csv_to_postgres',
    start_date=datetime(2026, 3, 1),
    schedule_interval='@once',
    catchup=False,
    tags=['greenery', 'ingest']
) as dag:

    greenery_addresses = PostgresOperator(
        task_id='ingest_addresses_csv_to_postgres',
        postgres_conn_id='postgres_source', 
        sql="""
            TRUNCATE TABLE addresses RESTART IDENTITY;
                COPY addresses
                FROM '/docker-entrypoint-initdb.d/addresses.csv'
                DELIMITER ','
                CSV HEADER;
        """
    )

    greenery_events = PostgresOperator(
        task_id='ingest_events_csv_to_postgres',
        postgres_conn_id='postgres_source', 
        sql="""
            TRUNCATE TABLE events RESTART IDENTITY;
                COPY events
                FROM '/docker-entrypoint-initdb.d/events.csv'
                DELIMITER ','
                CSV HEADER;
        """
    )


    greenery_order_items = PostgresOperator(
        task_id='ingest_order_items_csv_to_postgres',
        postgres_conn_id='postgres_source', 
        sql="""
            TRUNCATE TABLE order_items RESTART IDENTITY;
                COPY order_items
                FROM '/docker-entrypoint-initdb.d/order_items.csv'
                DELIMITER ','
                CSV HEADER;
        """
    )

    greenery_orders = PostgresOperator(
        task_id='ingest_orders_csv_to_postgres',
        postgres_conn_id='postgres_source', 
        sql="""
            TRUNCATE TABLE orders RESTART IDENTITY;
                COPY orders
                FROM '/docker-entrypoint-initdb.d/orders.csv'
                DELIMITER ',')
            CSV HEADER;
        """
    )

    greenery_products = PostgresOperator(
        task_id='ingest_products_csv_to_postgres',
        postgres_conn_id='postgres_source', 
        sql="""
            TRUNCATE TABLE products RESTART IDENTITY;
                COPY products
                FROM '/docker-entrypoint-initdb.d/products.csv'
                DELIMITER ','
                CSV HEADER;
        """
    )

    greenery_promos = PostgresOperator(
        task_id='ingest_promos_csv_to_postgres',
        postgres_conn_id='postgres_source', 
        sql="""
            TRUNCATE TABLE promos RESTART IDENTITY;
                COPY promos
                FROM '/docker-entrypoint-initdb.d/promos.csv'
                DELIMITER ','
            CSV HEADER;
        """
    )

    greenery_user = PostgresOperator(
        task_id='ingest_users_csv_to_postgres',
        postgres_conn_id='postgres_source', 
        sql="""
            TRUNCATE TABLE users RESTART IDENTITY;
                COPY users
                FROM '/docker-entrypoint-initdb.d/users.csv'
                DELIMITER ','
                CSV HEADER;
        """
    )

    