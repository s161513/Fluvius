# Electricity Data Ingestion

This project sets up a PostgreSQL database and ingests electricity allocation data using Docker Compose.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.

## Usage

Navigate to the application directory:

```bash
cd app
```

### 1. Start the Database

Run the database container in the background:

```bash
docker compose up -d postgres
```

### 2. Run Data Ingestion

Build and run the ingestion service to load the data into PostgreSQL:

```bash
docker compose --profile ingest up ingest --build
```
