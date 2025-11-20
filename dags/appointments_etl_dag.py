from datetime import datetime, timedelta
import logging

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.api_client import fetch_patients
from etl.transform import normalize_patients, generate_appointments
from etl.load import load_to_postgres

logger = logging.getLogger(__name__)


def extract_task(**context):
    """Extract raw patients from API and push to XCom."""
    ti = context["ti"]
    df = fetch_patients()
    ti.xcom_push(key="raw_patients", value=df.to_dict(orient="records"))
    logger.info("Pushed %d raw patients to XCom", len(df))


def transform_task(**context):
    ti = context["ti"]
    raw = ti.xcom_pull(key="raw_patients", task_ids="extract_patients")

    if not raw:
        logger.error("No raw patients in XCom, aborting transform.")
        raise ValueError("No raw patient data to transform")

    raw_df = pd.DataFrame(raw)
    logger.info("Transform: raw_df has columns: %s", list(raw_df.columns))

    patients_df = normalize_patients(raw_df)
    appointments_df = generate_appointments(patients_df)

    # Helper: make datetimes JSON-serializable for XCom
    def df_for_xcom(df: pd.DataFrame) -> list[dict]:
        df2 = df.copy()
        for col in df2.columns:
            if pd.api.types.is_datetime64_any_dtype(df2[col]):
                df2[col] = df2[col].astype(str)  # ISO strings
        return df2.to_dict(orient="records")

    ti.xcom_push(key="patients", value=df_for_xcom(patients_df))
    ti.xcom_push(key="appointments", value=df_for_xcom(appointments_df))

    logger.info(
        "Transform: produced %d patients and %d appointments",
        len(patients_df),
        len(appointments_df),
    )



def load_task(**context):
    ti = context["ti"]
    patients_records = ti.xcom_pull(key="patients", task_ids="transform_patients")
    appointments_records = ti.xcom_pull(
        key="appointments", task_ids="transform_patients"
    )

    patients_df = pd.DataFrame(patients_records)
    appointments_df = pd.DataFrame(appointments_records)

    # Convert back to datetimes where relevant
    for col in ["created_at_utc"]:
        if col in patients_df.columns:
            patients_df[col] = pd.to_datetime(patients_df[col])

    for col in ["appointment_start_utc", "appointment_end_utc", "created_at_utc"]:
        if col in appointments_df.columns:
            appointments_df[col] = pd.to_datetime(appointments_df[col])

    load_to_postgres(patients_df, appointments_df)



default_args = {
    "owner": "andres",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="appointments_etl",
    default_args=default_args,
    description="ETL pipeline for fake hospital appointments",
    start_date=datetime(2024, 1, 1),
    schedule="0 * * * *",  # hourly
    catchup=False,
    max_active_runs=1,
    tags=["health", "etl", "airflow"],
) as dag:

    extract = PythonOperator(
        task_id="extract_patients",
        python_callable=extract_task,
    )

    transform = PythonOperator(
        task_id="transform_patients",
        python_callable=transform_task,
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_task,
    )

    extract >> transform >> load
