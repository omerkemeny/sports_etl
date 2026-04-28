import logging
from dataclasses import dataclass, field
from typing import List

from etl.extract.api_sports_extractor import ApiSportsExtractor
from etl.extract.api_football_extractor import ApiFootballExtractor
from etl.transform.api_sports_transformer import ApiSportsTransformer
from etl.transform.api_football_transformer import ApiFootballTransformer
from etl.load.csv_loader import CsvLoader

logger = logging.getLogger(__name__)


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

    def _transform(self, data: dict) -> dict:
        logger.info("PHASE 2: TRANSFORM")
        sports   = ApiSportsTransformer().transform(data["sports"])
        football = ApiFootballTransformer().transform(data["football"])
        return {
            "api_sports_standardized":   sports["standings"].merge(sports["teams"].drop(columns="source"),   on="team_id", how="left"),
            "api_football_standardized": football["standings"].merge(football["teams"].drop(columns="source"), on="team_id", how="left"),
        }

    def _load(self, data: dict) -> None:
        logger.info("PHASE 3: LOAD")
        loader = CsvLoader()
        for table_name, df in data.items():
            loader.save(table_name, df)
