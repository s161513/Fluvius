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

Je hebt twee opties om de data in de PostgreSQL database in te laden: de handmatige methode via de ingest container, of automatisch via Apache Airflow.

#### Optie A: Manueel (Eenmalig)
Bouw en draai de specifieke ingestie service om de data eenmalig in PostgreSQL in te laden:
```bash
docker compose --profile ingest up ingest --build
```

### 3. derde titel
