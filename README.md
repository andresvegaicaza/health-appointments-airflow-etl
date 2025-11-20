# Health Appointments ETL with Airflow & PostgreSQL

Dockerized Airflow + Python ETL that ingests fake hospital data from an API, generates synthetic appointments, and loads analytics tables into PostgreSQL. Includes a Power BI dashboard for provider performance and no-show insights.


Simple portfolio project that shows how to:
- Orchestrate an ETL pipeline with **Apache Airflow**
- Ingest fake "patient" data from an external **HTTP API**
- Transform it into synthetic **appointments**
- Load into **PostgreSQL** for analytics
- Expose a clean `fact_appointments` table for BI tools (e.g. Power BI)

## Architecture

1. **Extract**  
   Airflow calls a Python function that hits a fake API (`fakerapi.it`) to fetch patient records.

2. **Transform**  
   We enrich the data with synthetic appointment info (doctor, specialty, status, start/end time).

3. **Load**  
   Data is loaded into PostgreSQL:
   - `stg_patients`
   - `fact_appointments`

4. **Schedule & Monitor**  
   An Airflow DAG (`appointments_etl`) runs hourly, with retries and basic logging.

## Tech stack

- Apache Airflow 2.x
- Python 3.x
- PostgreSQL
- `pandas`, `requests`, `sqlalchemy`

## Running locally (Docker)

```bash
docker-compose up -d
