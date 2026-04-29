import datetime
import logging
from dataclasses import dataclass, field
from typing import List

import pandas as pd

from config.settings import APIConfig
from etl.extract.api_sports_extractor import ApiSportsExtractor
from etl.extract.api_football_extractor import ApiFootballExtractor
from etl.transform.api_sports_transformer import ApiSportsTransformer
from etl.transform.api_football_transformer import ApiFootballTransformer
from etl.load.csv_loader import CsvLoader

logger = logging.getLogger(__name__)

_REQUIRED_COLUMNS = {"team_id", "team_name", "rank", "points"}


@dataclass
class PipelineStats:
    api_failures: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class ETLPipeline:
    def __init__(self):
        self.stats = PipelineStats()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def run(self) -> bool:
        logger.info("Starting ETL pipeline")
        extracted   = self._extract()
        transformed = self._transform(extracted)
        self._load(transformed)
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
        merged = standings.merge(
            teams.drop(columns="source"),
            on="team_id",
            how="left",
            suffixes=("", "_team"),
        )
        return merged.drop(columns=[c for c in merged.columns if c.endswith("_team")])

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["season"]       = APIConfig.SEASON
        df["league_name"]  = "Premier League"
        df["last_updated"] = datetime.datetime.now(datetime.UTC).isoformat()
        return df

    def _transform(self, data: dict) -> dict:
        logger.info("PHASE 2: TRANSFORM")
        sports   = ApiSportsTransformer().transform(data["sports"])
        football = ApiFootballTransformer().transform(data["football"])
        return {
            "api_sports_standardized":   self._add_metadata(self._merge_source(sports["standings"],   sports["teams"])),
            "api_football_standardized": self._add_metadata(self._merge_source(football["standings"], football["teams"])),
        }

    def _validate(self, table_name: str, df: pd.DataFrame) -> bool:
        if df.empty:
            logger.warning(f"Validation [{table_name}]: empty DataFrame, skipping")
            return False
        missing = _REQUIRED_COLUMNS - set(df.columns)
        if missing:
            logger.error(f"Validation [{table_name}]: missing required columns {missing}")
            self.stats.errors.append(f"{table_name}: missing columns {missing}")
            return False
        null_cols = [c for c in _REQUIRED_COLUMNS if df[c].isnull().any()]
        if null_cols:
            logger.error(f"Validation [{table_name}]: nulls in required columns {null_cols}")
            self.stats.errors.append(f"{table_name}: nulls in {null_cols}")
            return False
        return True

    def _load(self, data: dict) -> None:
        logger.info("PHASE 3: LOAD")
        loader = CsvLoader()
        for table_name, df in data.items():
            if not self._validate(table_name, df):
                continue
            loader.save(table_name, df)
