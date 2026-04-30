import logging
from dataclasses import dataclass, field
from typing import List

import pandas as pd

from config.settings import APIConfig
from etl.extract.api_sports_extractor import ApiSportsExtractor
from etl.extract.api_football_extractor import ApiFootballExtractor
from etl.transform.api_sports_transformer import ApiSportsTransformer
from etl.transform.api_football_transformer import ApiFootballTransformer
from etl.load.bigquery_loader import BigQueryLoader
from etl.load.csv_loader import CsvLoader
from utils.run_logger import RunLogger
from config.consts import SEASON
from config.consts import FINAL_COLUMNS, SCHEMA_VERSION
from utils.validation import validate_dataframe

logger = logging.getLogger(__name__)


@dataclass
class PipelineStats:
    api_failures: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class ETLPipeline:
    def __init__(self):
        self.stats       = PipelineStats()
        self._run_logger = RunLogger()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def run(self) -> bool:
        logger.info("Starting ETL pipeline")
        self._run_logger = RunLogger()
        extracted        = self._extract()
        transformed      = self._transform(extracted)
        self._load(transformed, self._run_logger)
        return not self.stats.errors

    def _extract(self) -> dict:
        logger.info("PHASE 1: EXTRACTION")
        raw = {
            "sports":   ApiSportsExtractor().extract(),
            "football": ApiFootballExtractor().extract(),
        }

        for source, tables in raw.items():
            for table, data in tables.items():
                key = f"{source}.{table}"
                if data is None:
                    self.stats.api_failures.append(key)
                    logger.warning(f"Source failed: {key}")
                else:
                    logger.info(f"Source OK: {key} ({len(data)} records)")

        return raw

    def _merge_source(self, standings: pd.DataFrame, teams: pd.DataFrame) -> pd.DataFrame:
        if standings.empty or teams.empty:
            logger.warning("_merge_source: empty standings or teams, returning empty DataFrame")
            return pd.DataFrame()
        merged = standings.merge(
            teams.drop(columns="source", errors="ignore"),
            on="team_id",
            how="left",
            suffixes=("", "_team"),
        )
        return merged.drop(columns=[c for c in merged.columns if c.endswith("_team")])

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=FINAL_COLUMNS)
        df = df.copy()
        df["run_id"]        = self._run_logger.run_id
        df["schema_version"] = SCHEMA_VERSION
        df["season"]        = SEASON
        df["last_updated"]  = pd.Timestamp.utcnow().floor("s")
        return df[FINAL_COLUMNS]

    def _transform(self, data: dict) -> dict:
        logger.info("PHASE 2: TRANSFORM")
        sports   = ApiSportsTransformer().transform(data["sports"])
        football = ApiFootballTransformer().transform(data["football"])
        return {
            "api_sports_standardized":   self._add_metadata(self._merge_source(sports["standings"],   sports["teams"])),
            "api_football_standardized": self._add_metadata(self._merge_source(football["standings"], football["teams"])),
        }

    def _load(self, data: dict, run_logger: RunLogger) -> None:
        logger.info("PHASE 3: LOAD")
        cfg         = APIConfig()
        loader      = BigQueryLoader() if cfg.USE_BIGQUERY else CsvLoader()
        output_mode = "bigquery" if cfg.USE_BIGQUERY else "csv"
        pending     = []

        for table_name, df in data.items():
            source = table_name.replace("_standardized", "")

            if not validate_dataframe(table_name, df):
                self.stats.errors.append(f"{table_name}: validation failed")
                pending.append({"table_name": table_name, "source": source, "rows_loaded": 0, "status": "failed"})
                continue

            try:
                loader.save(table_name, df)
                pending.append({"table_name": table_name, "source": source, "rows_loaded": len(df), "status": "success"})
            except Exception as e:
                logger.error(f"Load failed for {table_name}: {e}")
                self.stats.errors.append(f"{table_name}: load failed")
                pending.append({"table_name": table_name, "source": source, "rows_loaded": 0, "status": "failed"})

        statuses = [p["status"] for p in pending]
        if all(s == "success" for s in statuses):
            run_status = "success"
        elif all(s == "failed" for s in statuses):
            run_status = "failed"
        else:
            run_status = "partial"

        for entry in pending:
            run_logger.log(
                table_name=entry["table_name"], source=entry["source"],
                rows_loaded=entry["rows_loaded"], status=entry["status"],
                run_status=run_status, error_count=len(self.stats.errors),
                api_failure_count=len(self.stats.api_failures), output_mode=output_mode,
            )
