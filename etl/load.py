import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from .config import get_engine

logger = logging.getLogger(__name__)


def run_sql(conn, path: Path):
    if path.exists():
        logger.info("Running SQL file: %s", path)
        conn.execute(text(path.read_text()))
    else:
        logger.warning("SQL file not found: %s", path)


def load_to_postgres(patients_df: pd.DataFrame, appointments_df: pd.DataFrame):
    engine = get_engine()
    sql_dir = Path(__file__).resolve().parents[1] / "sql"

    staging_dir = sql_dir / "staging"
    marts_dir = sql_dir / "marts"

    logger.info("Starting load_to_postgres")
    logger.info("Patients rows: %d", len(patients_df))
    logger.info("Appointments rows: %d", len(appointments_df))

    with engine.begin() as conn:
        logger.info("Running staging DDL...")
        run_sql(conn, staging_dir / "create_stg_patients.sql")

        logger.info("Running marts DDL...")
        run_sql(conn, marts_dir / "create_fact_appointments.sql")

        logger.info("Loading patients into staging.stg_patients...")
        patients_df.to_sql(
            "stg_patients",
            conn,
            schema="staging",
            if_exists="append",
            index=False,
        )

        logger.info("Loading appointments into marts.fact_appointments...")
        appointments_df.to_sql(
            "fact_appointments",
            conn,
            schema="marts",
            if_exists="append",
            index=False,
        )

        logger.info("Creating analytics views...")
        run_sql(conn, marts_dir / "create_views_analytics.sql")

    logger.info("Load finished OK")

