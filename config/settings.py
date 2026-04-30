import os
from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")


@dataclass(frozen=True)
class APIConfig:
    SPORTS_KEY:                     str  = os.getenv("API_SPORTS_KEY", "")
    FOOTBALL_KEY:                   str  = os.getenv("API_FOOTBALL_KEY", "")
    USE_BIGQUERY:                   bool = os.getenv("USE_BIGQUERY", "false").lower() == "true"
    GCP_PROJECT_ID:                 str  = os.getenv("GCP_PROJECT_ID", "")
    BIGQUERY_DATASET:               str  = os.getenv("BIGQUERY_DATASET", "sports_etl")
    GOOGLE_APPLICATION_CREDENTIALS: str  = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
