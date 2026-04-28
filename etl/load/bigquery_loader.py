import logging
import pandas as pd

logger = logging.getLogger(__name__)


class BigQueryLoader:
    def load(self, table: str, df: pd.DataFrame) -> None:
        raise NotImplementedError
