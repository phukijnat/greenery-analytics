# Greenery Analytics

End-to-end batch data pipeline for a fictional e-commerce platform. ดึงข้อมูลจาก PostgreSQL, แปลงด้วย PySpark, เก็บไว้ใน GCS แล้ว load เข้า BigQuery และสร้าง analytical models ด้วย dbt

## Architecture

```
PostgreSQL → (Airflow) → PySpark → GCS (Parquet) → BigQuery → dbt marts
```

แต่ละ entity (orders, users, products, events, promos, addresses) มี Airflow DAG เป็นของตัวเอง รัน daily incremental load ผ่าน `SparkSubmitOperator` ไปยัง Spark cluster (master/worker) แล้ว upload ผลลัพธ์เป็น Parquet ไปที่ GCS ก่อน load เข้า BigQuery

dbt รับช่วงต่อจาก BigQuery โดยแบ่ง model เป็น 3 ชั้น:

```
staging/       ← rename columns, cast types, remove duplicates
intermediate/  ← join across entities (orders × products, orders × addresses)
marts/         ← final models ที่ตอบ business questions
```

**Mart models ตอบคำถามเหล่านี้:**
- How many users / orders do we have?
- Which state has the highest number of orders?
- What is the user repeat rate?
- What is the add-to-cart rate?
- What is the conversion rate by product?

## Stack

- **Orchestration** — Apache Airflow 3.0.1
- **Processing** — Apache Spark / PySpark
- **Transformation** — dbt Core 1.11.7 + BigQuery adapter
- **Storage** — Google Cloud Storage (Parquet), Google BigQuery
- **Source DB** — PostgreSQL 13
- **Infra** — Docker Compose (Airflow + Spark cluster + PostgreSQL)
- **Language** — Python 3.12, managed with Poetry

## Run Locally

**Prerequisites:** Docker, GCP Service Account JSON (BigQuery + GCS access)

```bash
# install dependencies
poetry install

# set environment variables
cp .env.example .env  # แก้ไข GCP_PROJECT_ID, bucket_name, service account path

# start all services (Airflow, Spark, PostgreSQL)
make build
make up
```

Airflow UI จะขึ้นที่ `http://localhost:8080`

Trigger DAGs ตามลำดับ:
1. `ingest_data_to_postgres` — load seed data เข้า source DB
2. `greenery_*_data_pipeline` — extract → spark → GCS → BigQuery
3. `greenery_dbt_dag` — run dbt models

```bash
# หรือ run dbt แยก
cd dbt/greenery
dbt run
dbt test
```
