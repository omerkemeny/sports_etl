import logging

import pandas as pd

from config.consts import EXPORTS_DIR

logger = logging.getLogger(__name__)


class CsvLoader:
    def __init__(self):
        EXPORTS_DIR.mkdir(exist_ok=True)

    def save(self, table_name: str, df: pd.DataFrame) -> None:
        path = EXPORTS_DIR / f"{table_name}.csv"
        df.to_csv(path, index=False)
        logger.info(f"Saved {len(df)} rows to {path}")
