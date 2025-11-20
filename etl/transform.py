import logging
import uuid
import random
from datetime import datetime, timedelta
import pandas as pd

logger = logging.getLogger(__name__)

DOCTORS = [
    ("Dr. Smith", "Cardiology"),
    ("Dr. Lopez", "Internal Medicine"),
    ("Dr. Kim", "Pediatrics"),
    ("Dr. Rossi", "Dermatology"),
]

STATUSES = ["scheduled", "completed", "no_show"]

def normalize_patients(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza los datos del API a la forma de stg_patients.
    Tolera cambios de nombres de columnas.
    """
    logger.info("Normalizing %d raw patients", len(raw_df))
    logger.info("Raw columns: %s", list(raw_df.columns))

    df = raw_df.copy()

    # patient_id: intenta uuid, luego id, y si no, usa el índice
    if "uuid" in df.columns:
        df["patient_id"] = df["uuid"]
    elif "id" in df.columns:
        df["patient_id"] = df["id"]
    else:
        df["patient_id"] = df.index.astype(str)

    # Campos básicos con get() para evitar KeyError
    df["first_name"] = df.get("firstname", "")
    df["last_name"]  = df.get("lastname", "")
    df["email"]      = df.get("email", "")
    df["phone"]      = df.get("phone", "")

    # Address: puede venir plano o anidado
    if "address.street" in df.columns:
        df["address"] = df["address.street"]
    elif "address" in df.columns:
        df["address"] = df["address"]
    else:
        df["address"] = ""

    df["city"]    = df.get("address.city", "")
    df["state"]   = df.get("address.state", "")
    df["country"] = df.get("address.country", "")

    df["created_at_utc"] = datetime.utcnow()

    cols = [
        "patient_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "address",
        "city",
        "state",
        "country",
        "created_at_utc",
    ]
    normalized = df[cols]
    logger.info("Normalized to %d staged patients", len(normalized))
    return normalized


def generate_appointments(patients_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in patients_df.iterrows():
        for _ in range(random.randint(1, 3)):
            doctor_name, specialty = random.choice(DOCTORS)
            start = datetime.utcnow() + timedelta(days=random.randint(-7, 7))
            start = start.replace(minute=0, second=0, microsecond=0)
            end = start + timedelta(minutes=30)

            rows.append(
                {
                    "appointment_id": str(uuid.uuid4()),
                    "patient_id": row["patient_id"],
                    "doctor_name": doctor_name,
                    "specialty": specialty,
                    "status": random.choice(STATUSES),
                    "appointment_start_utc": start,
                    "appointment_end_utc": end,
                    "created_at_utc": datetime.utcnow(),
                }
            )

    return pd.DataFrame(rows)
