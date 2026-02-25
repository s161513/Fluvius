import os

import pandas as pd
from sqlalchemy import create_engine


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


# 1) Read parquet (must be provided)
parquet_path = require_env("PARQUET_PATH")
df = pd.read_parquet(parquet_path)

# 2) Postgres connection (all required; no secrets/defaults in code)
pg_host = require_env("PGHOST")
pg_port = require_env("PGPORT")
pg_db = require_env("PGDATABASE")
pg_user = require_env("PGUSER")
pg_password = require_env("PGPASSWORD")
table_name = require_env("PGTABLE")

engine = create_engine(
    f"postgresql+psycopg://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
)

# 3) Write to Postgres
df.to_sql(table_name, engine, schema="public", if_exists="replace", index=False)

print(f"Wrote {len(df)} rows to public.{table_name}")
