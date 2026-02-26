# Electricity Data Ingestion

This project runs a PostgreSQL database + Apache Airflow that ingests Fluvius electricity allocation data from parquet files.

## Goal

- You put parquet files in `ingest/data/`.
- Files are **versioned by suffix**: `<base>.parquet-1`, `<base>.parquet-2`, …
- Airflow runs every minute and:
  1) detects the **highest** version in the folder
  2) compares it with what’s already loaded
  3) if it’s newer, it **replaces** the current data in the standard table (no table versioning)

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.

## Quickstart (one command)

From the repo root:

```bash
docker compose -f app/docker-compose.yml up -d --build
```

That starts:
- `postgres` (database)
- `airflow` (scheduler + web UI)
- `streamlit` (dashboard)

## Where to drop data (versioning)

Put your parquet versions in:

- `ingest/data/`

Example (as in this repo):
- `1-19-totaal-gealloceerd-volume.parquet-1`
- `1-19-totaal-gealloceerd-volume.parquet-2`
- `1-19-totaal-gealloceerd-volume.parquet-3`

Airflow searches for:

- `PARQUET_PATH` basename + `-<number>`

So if `PARQUET_PATH=/app/data/1-19-totaal-gealloceerd-volume.parquet`, it will detect versions like:

- `/app/data/1-19-totaal-gealloceerd-volume.parquet-<n>`

## Airflow

Open the Airflow UI:

- http://localhost:8080
- username: `admin`
- password: `admin`

DAG:
- `fluvius_data_ingestion`

Schedule:
- every minute (`*/1 * * * *`)

What happens on a new version:
- task `check_for_new_version` finds the highest `-<n>`
- task `ingest_new_version` loads that parquet and writes it to Postgres with `if_exists="replace"`

## How to verify in Postgres (top 20 + version)

Start a psql session inside the Postgres container:

```bash
docker compose -f app/docker-compose.yml exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

In psql:

Top 20 rows:

```sql
SELECT *
FROM public.elektriciteit
LIMIT 20;
```

Check which file version is currently loaded (Airflow adds `_file_version` during ingest):

```sql
SELECT MAX(_file_version) AS loaded_version
FROM public.elektriciteit;
```

If you want to see the newest rows for the newest version:

```sql
SELECT *
FROM public.elektriciteit
ORDER BY _file_version DESC
LIMIT 20;
```

## Notes / troubleshooting

- If you add a new file like `...parquet-4`, wait up to ~1 minute for the scheduler to run.
- You can always trigger the DAG manually in the Airflow UI to ingest immediately.
