import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from config.settings import APIConfig
from etl.extract.api_football_extractor import ApiFootballExtractor
from etl.load.bigquery_loader import BigQueryLoader
from etl.pipeline import ETLPipeline
from etl.transform.standard_schema import FINAL_COLUMNS
from etl.utils.validation import validate_dataframe


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _standings(source: str) -> pd.DataFrame:
    return pd.DataFrame({
        "rank":          [1, 2],
        "team_id":       ["10", "20"],
        "team_name":     ["Team A", "Team B"],
        "points":        [90, 80],
        "played":        [38, 38],
        "won":           [28, 25],
        "drawn":         [6,  5],
        "lost":          [4,  8],
        "goals_for":     [95, 80],
        "goals_against": [30, 40],
        "source":        [source, source],
    })


def _teams(source: str) -> pd.DataFrame:
    return pd.DataFrame({
        "team_id":    ["10", "20"],
        "team_name":  ["Team A", "Team B"],
        "country":    ["England", "England"],
        "venue_name": ["Stadium A", "Stadium B"],
        "source":     [source, source],
    })


def _merged(source: str) -> pd.DataFrame:
    """Standings + teams merged — ready for _add_metadata."""
    p = ETLPipeline()
    return p._merge_source(_standings(source), _teams(source))


def _valid_df() -> pd.DataFrame:
    return pd.DataFrame({
        "team_id":   ["10"],
        "team_name": ["Team A"],
        "rank":      [1],
        "points":    [90],
    })


# ---------------------------------------------------------------------------
# _merge_source
# ---------------------------------------------------------------------------

class TestMergeSource:
    def setup_method(self):
        self.p = ETLPipeline()

    def test_no_team_suffixed_columns(self):
        merged = self.p._merge_source(_standings("x"), _teams("x"))
        assert not any(c.endswith("_team") for c in merged.columns)

    def test_standings_team_name_wins(self):
        teams = _teams("x")
        teams.loc[0, "team_name"] = "Different Name"
        merged = self.p._merge_source(_standings("x"), teams)
        assert merged.loc[0, "team_name"] == "Team A"

    def test_source_column_singular(self):
        merged = self.p._merge_source(_standings("x"), _teams("x"))
        assert merged.columns.tolist().count("source") == 1

    def test_country_and_venue_preserved(self):
        merged = self.p._merge_source(_standings("x"), _teams("x"))
        assert "country" in merged.columns
        assert "venue_name" in merged.columns

    def test_row_count_preserved(self):
        merged = self.p._merge_source(_standings("x"), _teams("x"))
        assert len(merged) == 2

    def test_goal_diff_not_present(self):
        merged = self.p._merge_source(_standings("x"), _teams("x"))
        assert "goal_diff" not in merged.columns

    def test_empty_standings_returns_empty(self):
        merged = self.p._merge_source(pd.DataFrame(), _teams("x"))
        assert merged.empty

    def test_empty_teams_returns_empty(self):
        merged = self.p._merge_source(_standings("x"), pd.DataFrame())
        assert merged.empty

    def test_missing_source_column_in_teams_does_not_crash(self):
        teams_no_source = _teams("x").drop(columns="source")
        merged = self.p._merge_source(_standings("x"), teams_no_source)
        assert not merged.empty


# ---------------------------------------------------------------------------
# _add_metadata
# ---------------------------------------------------------------------------

class TestAddMetadata:
    def setup_method(self):
        self.p = ETLPipeline()

    def test_season_equals_config(self):
        df = self.p._add_metadata(_merged("x"))
        assert (df["season"] == APIConfig.SEASON).all()

    def test_league_name_not_present(self):
        df = self.p._add_metadata(_merged("x"))
        assert "league_name" not in df.columns

    def test_last_updated_is_timestamp(self):
        df = self.p._add_metadata(_merged("x"))
        assert isinstance(df["last_updated"].iloc[0], pd.Timestamp)

    def test_last_updated_has_no_microseconds(self):
        df = self.p._add_metadata(_merged("x"))
        assert df["last_updated"].iloc[0].microsecond == 0

    def test_original_not_mutated(self):
        original = _merged("x")
        self.p._add_metadata(original)
        assert "season" not in original.columns

    def test_output_columns_match_final_schema(self):
        df = self.p._add_metadata(_merged("x"))
        assert list(df.columns) == FINAL_COLUMNS


# ---------------------------------------------------------------------------
# validate_dataframe (etl/utils/validation.py)
# ---------------------------------------------------------------------------

class TestValidateDataframe:
    def test_valid_df_passes(self):
        assert validate_dataframe("t", _valid_df()) is True

    def test_empty_df_fails(self):
        assert validate_dataframe("t", pd.DataFrame()) is False

    def test_missing_required_column_fails(self):
        assert validate_dataframe("t", _valid_df().drop(columns="points")) is False

    def test_null_in_required_column_fails(self):
        df = _valid_df()
        df.loc[0, "team_name"] = None
        assert validate_dataframe("t", df) is False


# ---------------------------------------------------------------------------
# BigQueryLoader
# ---------------------------------------------------------------------------

class TestBigQueryLoader:
    def _make_loader(self, mock_client):
        mock_cfg = MagicMock()
        mock_cfg.return_value.GCP_PROJECT_ID = "test-project"
        mock_cfg.return_value.BIGQUERY_DATASET = "test_dataset"
        with patch("etl.load.bigquery_loader.bigquery.Client", return_value=mock_client):
            with patch("etl.load.bigquery_loader.APIConfig", mock_cfg):
                return BigQueryLoader()

    def test_save_calls_load_table_from_dataframe(self):
        mock_client = MagicMock()
        mock_client.load_table_from_dataframe.return_value = MagicMock()
        loader = self._make_loader(mock_client)
        loader.save("my_table", _valid_df())
        mock_client.load_table_from_dataframe.assert_called_once()
        args, _ = mock_client.load_table_from_dataframe.call_args
        assert args[1] == "test-project.test_dataset.my_table"

    def test_save_uses_write_truncate(self):
        from google.cloud.bigquery import WriteDisposition
        mock_client = MagicMock()
        mock_client.load_table_from_dataframe.return_value = MagicMock()
        loader = self._make_loader(mock_client)
        loader.save("my_table", _valid_df())
        _, kwargs = mock_client.load_table_from_dataframe.call_args
        assert kwargs["job_config"].write_disposition == WriteDisposition.WRITE_TRUNCATE

    def test_save_skips_empty_df(self):
        mock_client = MagicMock()
        loader = self._make_loader(mock_client)
        loader.save("my_table", pd.DataFrame())
        mock_client.load_table_from_dataframe.assert_not_called()

    def test_save_raises_on_error(self):
        mock_client = MagicMock()
        mock_job = MagicMock()
        mock_job.result.side_effect = Exception("BQ error")
        mock_client.load_table_from_dataframe.return_value = mock_job
        loader = self._make_loader(mock_client)
        with pytest.raises(Exception, match="BQ error"):
            loader.save("my_table", _valid_df())


# ---------------------------------------------------------------------------
# _load — loader routing
# ---------------------------------------------------------------------------

class TestLoadRouting:
    def test_uses_bigquery_loader_when_flag_true(self):
        p = ETLPipeline()
        with patch("etl.pipeline.APIConfig") as MockCfg:
            MockCfg.return_value.USE_BIGQUERY = True
            with patch("etl.pipeline.BigQueryLoader") as MockBQ:
                MockBQ.return_value.save = MagicMock()
                p._load({"good": _valid_df()})
                MockBQ.assert_called_once()

    def test_uses_csv_loader_when_flag_false(self):
        p = ETLPipeline()
        with patch("etl.pipeline.APIConfig") as MockCfg:
            MockCfg.return_value.USE_BIGQUERY = False
            with patch("etl.pipeline.CsvLoader") as MockCSV:
                MockCSV.return_value.save = MagicMock()
                p._load({"good": _valid_df()})
                MockCSV.assert_called_once()

    def test_empty_df_not_saved(self):
        p = ETLPipeline()
        with patch("etl.pipeline.APIConfig") as MockCfg:
            MockCfg.return_value.USE_BIGQUERY = False
            with patch("etl.pipeline.CsvLoader") as MockCSV:
                p._load({"t": pd.DataFrame()})
                MockCSV.return_value.save.assert_not_called()


# ---------------------------------------------------------------------------
# _transform end-to-end
# ---------------------------------------------------------------------------

class TestTransform:
    def _run(self):
        p = ETLPipeline()
        with (
            patch("etl.pipeline.ApiSportsTransformer") as MockST,
            patch("etl.pipeline.ApiFootballTransformer") as MockFT,
        ):
            MockST.return_value.transform.return_value = {
                "standings": _standings("api-sports"),
                "teams":     _teams("api-sports"),
            }
            MockFT.return_value.transform.return_value = {
                "standings": _standings("api-football"),
                "teams":     _teams("api-football"),
            }
            return p._transform({"sports": {}, "football": {}})

    def test_both_output_keys_present(self):
        assert set(self._run().keys()) == {
            "api_sports_standardized", "api_football_standardized"
        }

    def test_output_matches_final_schema(self):
        for df in self._run().values():
            assert list(df.columns) == FINAL_COLUMNS

    def test_no_goal_diff_in_output(self):
        for df in self._run().values():
            assert "goal_diff" not in df.columns

    def test_no_league_name_in_output(self):
        for df in self._run().values():
            assert "league_name" not in df.columns

    def test_no_suffix_artifacts(self):
        for name, df in self._run().items():
            for col in df.columns:
                assert not col.endswith(("_team", "_x", "_y")), f"{name}: found '{col}'"


# ---------------------------------------------------------------------------
# API-Football extractor — season_id in request params
# ---------------------------------------------------------------------------

class TestApiFootballSeasonParam:
    def _capture(self):
        captured = {}
        def fake_fetch(source_configs, **kwargs):
            captured["configs"] = source_configs
            return {}
        with patch("etl.extract.api_football_extractor.FootballAPIExtractor") as MockHTTP:
            MockHTTP.return_value.fetch_all_sources.side_effect = fake_fetch
            ApiFootballExtractor().extract()
        return captured["configs"]

    def test_standings_has_season_id(self):
        s = next(c for c in self._capture() if c["name"] == "api-football-standings")
        assert "season_id" in s["params"]

    def test_teams_has_season_id(self):
        t = next(c for c in self._capture() if c["name"] == "api-football-teams")
        assert "season_id" in t["params"]

    def test_season_id_value_matches_config(self):
        s = next(c for c in self._capture() if c["name"] == "api-football-standings")
        assert s["params"]["season_id"] == APIConfig.SEASON


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
