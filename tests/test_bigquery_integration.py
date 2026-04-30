import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from google.cloud import bigquery

from config.settings import APIConfig
from config.consts import FINAL_COLUMNS

_cfg = APIConfig()

TABLES = ["api_sports_standardized", "api_football_standardized"]

requires_bigquery = pytest.mark.skipif(
    not _cfg.USE_BIGQUERY,
    reason="USE_BIGQUERY is not enabled — set USE_BIGQUERY=true in .env to run these tests",
)


def _client():
    return bigquery.Client(project=_cfg.GCP_PROJECT_ID)


def _table_id(table_name: str) -> str:
    return f"{_cfg.GCP_PROJECT_ID}.{_cfg.BIGQUERY_DATASET}.{table_name}"


@requires_bigquery
@pytest.mark.parametrize("table_name", TABLES)
def test_table_exists_and_has_rows(table_name):
    client = _client()
    query  = f"SELECT COUNT(*) AS row_count FROM `{_table_id(table_name)}`"
    result = list(client.query(query).result())
    assert result[0]["row_count"] > 0, f"{table_name} is empty or does not exist"


@requires_bigquery
@pytest.mark.parametrize("table_name", TABLES)
def test_table_columns_match_final_schema(table_name):
    client = _client()
    table  = client.get_table(_table_id(table_name))
    bq_columns = [f.name for f in table.schema]
    assert bq_columns == FINAL_COLUMNS, (
        f"{table_name} schema mismatch:\n"
        f"  expected: {FINAL_COLUMNS}\n"
        f"  got:      {bq_columns}"
    )


@requires_bigquery
@pytest.mark.parametrize("table_name", TABLES)
def test_table_has_20_teams(table_name):
    client = _client()
    query  = f"SELECT COUNT(DISTINCT team_id) AS n FROM `{_table_id(table_name)}`"
    result = list(client.query(query).result())
    assert result[0]["n"] == 20, f"{table_name}: expected 20 teams, got {result[0]['n']}"


@requires_bigquery
@pytest.mark.parametrize("table_name", TABLES)
def test_no_nulls_in_required_columns(table_name):
    client   = _client()
    required = ["team_id", "team_name", "rank", "points"]
    checks   = " OR ".join(f"{c} IS NULL" for c in required)
    query    = f"SELECT COUNT(*) AS null_count FROM `{_table_id(table_name)}` WHERE {checks}"
    result   = list(client.query(query).result())
    assert result[0]["null_count"] == 0, f"{table_name}: nulls found in required columns"


@requires_bigquery
@pytest.mark.parametrize("table_name", TABLES)
def test_season_is_consistent(table_name):
    client = _client()
    query  = f"SELECT COUNT(DISTINCT season) AS n FROM `{_table_id(table_name)}`"
    result = list(client.query(query).result())
    assert result[0]["n"] == 1, f"{table_name}: multiple seasons found in data"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
