import logging

import pandas as pd
from google.cloud import bigquery

from config.settings import APIConfig

logger = logging.getLogger(__name__)


class BigQueryLoader:
    def __init__(self):
        cfg = APIConfig()
        self._project = cfg.GCP_PROJECT_ID
        self._dataset = cfg.BIGQUERY_DATASET
        self._client  = bigquery.Client(project=self._project)

    def save(self, table_name: str, df: pd.DataFrame) -> None:
        if df.empty:
            logger.warning(f"BigQueryLoader: skipping empty DataFrame for {table_name}")
            return
        table_id   = f"{self._project}.{self._dataset}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        try:
            job = self._client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            logger.info(f"BigQueryLoader: loaded {len(df)} rows → {table_id}")
        except Exception as e:
            logger.error(f"BigQueryLoader: failed to load {table_id}: {e}")
            raise
