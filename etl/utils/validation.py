import logging

import pandas as pd

logger = logging.getLogger(__name__)

_REQUIRED_COLUMNS = {"team_id", "team_name", "rank", "points"}


def validate_dataframe(table_name: str, df: pd.DataFrame) -> bool:
    if df.empty:
        logger.warning(f"Validation [{table_name}]: empty DataFrame, skipping")
        return False
    missing = _REQUIRED_COLUMNS - set(df.columns)
    if missing:
        logger.error(f"Validation [{table_name}]: missing required columns {missing}")
        return False
    null_cols = [c for c in _REQUIRED_COLUMNS if df[c].isnull().any()]
    if null_cols:
        logger.error(f"Validation [{table_name}]: nulls in required columns {null_cols}")
        return False
    return True
