import logging
import pandas as pd
import requests
from typing import Any, Dict

from .config import FAKER_API_URL, PATIENT_QUANTITY

logger = logging.getLogger(__name__)

def fetch_patients() -> pd.DataFrame:
    """Fetch fake patients from Faker API."""
    params = {"_quantity": PATIENT_QUANTITY}
    resp = requests.get(FAKER_API_URL, params=params, timeout=10)
    resp.raise_for_status()
    data: Dict[str, Any] = resp.json()
    return pd.json_normalize(data.get("data", []))
