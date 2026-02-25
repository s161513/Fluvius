import os
import re
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _get_engine():
    pg_host = os.environ.get("PGHOST", "postgres")
    if pg_host in ["localhost", "127.0.0.1"]:
        pg_host = "postgres"
    return create_engine(
        f"postgresql+psycopg2://{require_env('PGUSER')}:{require_env('PGPASSWORD')}"
        f"@{pg_host}:{require_env('PGPORT')}/{require_env('PGDATABASE')}"
    )


def _find_versioned_files(data_dir: str, base_name: str):
    """
    Scan data_dir for files matching '<base_name>-<number>'.
    Returns a sorted list of (version_int, full_path), lowest version first.
    e.g. 1-19-totaal-gealloceerd-volume.parquet-2 -> (2, '/app/data/...-2')
    """
    pattern = re.compile(r"^" + re.escape(base_name) + r"-(\d+)$")
    versions = []
    for fname in os.listdir(data_dir):
        m = pattern.match(fname)
        if m:
            versions.append((int(m.group(1)), os.path.join(data_dir, fname)))
    versions.sort(key=lambda x: x[0])
    return versions


def _get_current_db_version(engine, table_name: str) -> int:
    """Return the _file_version currently loaded in the table (0 if table doesn't exist)."""
    with engine.connect() as conn:
        table_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :t
            )
        """), {"t": table_name}).scalar()
        if not table_exists:
            return 0
        has_col = conn.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = :t
                  AND column_name = '_file_version'
            )
        """), {"t": table_name}).scalar()
        if not has_col:
            return 0
        result = conn.execute(
            text(f"SELECT COALESCE(MAX(_file_version), 0) FROM public.{table_name}")
        ).scalar()
    return result or 0


def check_for_new_version(**context):
    parquet_path = os.environ.get("PARQUET_PATH",
                                  "/app/data/1-19-totaal-gealloceerd-volume.parquet")
    data_dir = os.path.dirname(parquet_path)
    base_name = os.path.basename(parquet_path)
    table_name = require_env("PGTABLE")

    versioned_files = _find_versioned_files(data_dir, base_name)
    if not versioned_files:
        print("Geen versiebestanden gevonden in de data map.")
        raise AirflowSkipException("Geen versiebestanden beschikbaar.")

    latest_version, latest_path = versioned_files[-1]
    print(f"Hoogste beschikbare versie: {latest_version} ({latest_path})")

    engine = _get_engine()
    current_version = _get_current_db_version(engine, table_name)
    print(f"Huidige versie in database: {current_version}")

    if latest_version <= current_version:
        print("Database is al up-to-date, ingest overgeslagen.")
        raise AirflowSkipException(f"Versie {current_version} is al ingeladen.")

    print(f"Nieuwe versie {latest_version} gevonden, ingest starten.")
    context['ti'].xcom_push(key='ingest_path', value=latest_path)
    context['ti'].xcom_push(key='ingest_version', value=latest_version)


def ingest_new_version(**context):
    ti = context['ti']
    ingest_path = ti.xcom_pull(task_ids='check_for_new_version', key='ingest_path')
    ingest_version = ti.xcom_pull(task_ids='check_for_new_version', key='ingest_version')

    if not ingest_path:
        raise ValueError("Geen ingest pad ontvangen via XCom.")

    print(f"Inlezen van versie {ingest_version}: {ingest_path}")
    df = pd.read_parquet(ingest_path)
    df["_file_version"] = ingest_version

    table_name = require_env("PGTABLE")
    engine = _get_engine()

    # Delete current data and load the new version
    df.to_sql(table_name, engine, schema="public", if_exists="replace", index=False)
    print(f"Tabel '{table_name}' vervangen met {len(df)} rijen uit versie {ingest_version}.")

    return f"Ingested {len(df)} rows from version {ingest_version}."


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
    description='Detecteert nieuwe versies van Fluvius data en laadt ze in Postgres',
    schedule_interval='*/1 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['fluvius', 'ingest'],
) as dag:

    check_task = PythonOperator(
        task_id='check_for_new_version',
        python_callable=check_for_new_version,
    )

    ingest_task = PythonOperator(
        task_id='ingest_new_version',
        python_callable=ingest_new_version,
    )

    check_task >> ingest_task
