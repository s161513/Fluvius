import os
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def ingest_fluvius_data():
    """
    Reads the parquet file and ingests it into postgres, similar to the original ingest script.
    """
    # 1) Read parquet
    # Ensure PARQUET_PATH is configured correctly for the airflow container path
    parquet_path_env = os.environ.get("PARQUET_PATH")
    # if it's not set correctly via the main .env, we fallback to the mounted path
    parquet_path = parquet_path_env if parquet_path_env else "/app/data/1-19-totaal-gealloceerd-volume.parquet"
    
    print(f"Reading from {parquet_path}")
    df = pd.read_parquet(parquet_path)

    # 2) Postgres connection
    # Note: Inside Docker compose, the host is 'postgres', not 'localhost' or '127.0.0.1'.
    # Because ingest/.env might specify PGHOST=localhost, we override it here conceptually,
    # or ensure ingest/.env uses PGHOST=postgres.
    pg_host = os.environ.get("PGHOST", "postgres")
    if pg_host in ["localhost", "127.0.0.1"]:
        # Fallback override for docker network
        pg_host = "postgres"
        print("Overriding PGHOST to 'postgres' for Docker network resolution.")

    pg_port = require_env("PGPORT")
    pg_db = require_env("PGDATABASE")
    pg_user = require_env("PGUSER")
    pg_password = require_env("PGPASSWORD")
    table_name = require_env("PGTABLE")

    engine = create_engine(
        f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    )

    # 3) Write to Postgres
    df.to_sql(table_name, engine, schema="public", if_exists="replace", index=False)

    print(f"Wrote {len(df)} rows to public.{table_name}")
    return f"Successfully ingested {len(df)} rows."


# Default settings for the DAG
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='fluvius_data_ingestion',
    default_args=default_args,
    description='Haalt automatisch Fluvius data op en zet het in Postgres',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['fluvius', 'ingest'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_parquet_to_postgres',
        python_callable=ingest_fluvius_data,
    )

