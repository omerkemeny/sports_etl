import logging

import pandas as pd

from config.consts import EXPORTS_DIR, LOG_CSV, MONITORING_TABLE
from config.settings import APIConfig

logger = logging.getLogger(__name__)


class RunLogger:
    def __init__(self):
        self.run_id     = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
        self.started_at = pd.Timestamp.utcnow().floor("s")

    def log(
        self,
        table_name: str,
        source: str,
        rows_loaded: int,
        status: str,
        run_status: str,
        error_count: int,
        api_failure_count: int,
        output_mode: str,
    ) -> None:
        finished_at = pd.Timestamp.utcnow().floor("s")
        record = pd.DataFrame([{
            "run_id":            self.run_id,
            "source":            source,
            "table_name":        table_name,
            "rows_loaded":       rows_loaded,
            "status":            status,
            "run_status":        run_status,
            "started_at":        self.started_at,
            "finished_at":       finished_at,
            "duration_seconds":  (finished_at - self.started_at).total_seconds(),
            "error_count":       error_count,
            "api_failure_count": api_failure_count,
            "output_mode":       output_mode,
        }])
        self._write_csv(record)
        if APIConfig().USE_BIGQUERY:
            self._write_bigquery(record)

    def _write_csv(self, record: pd.DataFrame) -> None:
        EXPORTS_DIR.mkdir(exist_ok=True)
        record.to_csv(LOG_CSV, mode="a", header=not LOG_CSV.exists(), index=False)
        logger.info(f"RunLogger: appended monitoring row to {LOG_CSV.name}")

    def _write_bigquery(self, record: pd.DataFrame) -> None:
        try:
            from google.cloud import bigquery
            cfg      = APIConfig()
            client   = bigquery.Client(project=cfg.GCP_PROJECT_ID)
            table_id = f"{cfg.GCP_PROJECT_ID}.{cfg.BIGQUERY_DATASET}.{MONITORING_TABLE}"
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            client.load_table_from_dataframe(record, table_id, job_config=job_config).result()
            logger.info(f"RunLogger: appended monitoring row to {table_id}")
        except Exception as e:
            logger.warning(f"RunLogger: BigQuery write failed (monitoring only): {e}")
