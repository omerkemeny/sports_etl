import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import datetime
import pandas as pd
import pytest
from unittest.mock import patch

from config.settings import APIConfig
from etl.extract.api_football_extractor import ApiFootballExtractor
from etl.pipeline import ETLPipeline


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
        "goal_diff":     [65, 40],
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

    def test_source_from_teams_dropped(self):
        merged = self.p._merge_source(_standings("x"), _teams("x"))
        assert merged["source"].tolist() == ["x", "x"]
        assert merged.columns.tolist().count("source") == 1

    def test_country_and_venue_preserved(self):
        merged = self.p._merge_source(_standings("x"), _teams("x"))
        assert "country" in merged.columns
        assert "venue_name" in merged.columns

    def test_row_count_preserved(self):
        merged = self.p._merge_source(_standings("x"), _teams("x"))
        assert len(merged) == 2


# ---------------------------------------------------------------------------
# _add_metadata
# ---------------------------------------------------------------------------

class TestAddMetadata:
    def setup_method(self):
        self.p = ETLPipeline()

    def test_season_equals_config(self):
        df = self.p._add_metadata(_standings("x"))
        assert (df["season"] == APIConfig.SEASON).all()

    def test_league_name_is_premier_league(self):
        df = self.p._add_metadata(_standings("x"))
        assert (df["league_name"] == "Premier League").all()

    def test_last_updated_is_valid_iso(self):
        df = self.p._add_metadata(_standings("x"))
        assert "last_updated" in df.columns
        datetime.datetime.fromisoformat(df["last_updated"].iloc[0])

    def test_original_not_mutated(self):
        original = _standings("x")
        self.p._add_metadata(original)
        assert "season" not in original.columns


# ---------------------------------------------------------------------------
# _validate
# ---------------------------------------------------------------------------

class TestValidate:
    def setup_method(self):
        self.p = ETLPipeline()

    def _valid(self):
        return pd.DataFrame({
            "team_id":   ["10"],
            "team_name": ["Team A"],
            "rank":      [1],
            "points":    [90],
        })

    def test_valid_df_passes(self):
        assert self.p._validate("t", self._valid()) is True

    def test_empty_df_fails(self):
        assert self.p._validate("t", pd.DataFrame()) is False

    def test_missing_required_column_fails(self):
        assert self.p._validate("t", self._valid().drop(columns="points")) is False

    def test_missing_column_recorded_in_errors(self):
        self.p._validate("my_table", self._valid().drop(columns="rank"))
        assert any("my_table" in e for e in self.p.stats.errors)

    def test_null_in_required_column_fails(self):
        df = self._valid()
        df.loc[0, "team_name"] = None
        assert self.p._validate("t", df) is False

    def test_null_recorded_in_errors(self):
        df = self._valid()
        df.loc[0, "points"] = None
        self.p._validate("my_table", df)
        assert any("my_table" in e for e in self.p.stats.errors)


# ---------------------------------------------------------------------------
# _load
# ---------------------------------------------------------------------------

class TestLoad:
    def setup_method(self):
        self.p = ETLPipeline()

    def _valid(self):
        return pd.DataFrame({
            "team_id":   ["10"],
            "team_name": ["Team A"],
            "rank":      [1],
            "points":    [90],
        })

    def test_empty_df_not_saved(self):
        with patch("etl.pipeline.CsvLoader") as MockLoader:
            self.p._load({"t": pd.DataFrame()})
            MockLoader.return_value.save.assert_not_called()

    def test_invalid_df_not_saved(self):
        with patch("etl.pipeline.CsvLoader") as MockLoader:
            self.p._load({"t": pd.DataFrame({"col": [1]})})
            MockLoader.return_value.save.assert_not_called()

    def test_valid_df_saved(self):
        df = self._valid()
        with patch("etl.pipeline.CsvLoader") as MockLoader:
            self.p._load({"good": df})
            MockLoader.return_value.save.assert_called_once_with("good", df)

    def test_mixed_skips_invalid_saves_valid(self):
        valid = self._valid()
        with patch("etl.pipeline.CsvLoader") as MockLoader:
            self.p._load({"bad": pd.DataFrame(), "good": valid})
            MockLoader.return_value.save.assert_called_once_with("good", valid)


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
        result = self._run()
        assert "api_sports_standardized" in result
        assert "api_football_standardized" in result

    def test_metadata_columns_present(self):
        result = self._run()
        for df in result.values():
            assert "season" in df.columns
            assert "league_name" in df.columns
            assert "last_updated" in df.columns

    def test_no_suffix_artifacts(self):
        result = self._run()
        for name, df in result.items():
            for col in df.columns:
                assert not col.endswith("_team"), f"{name}: found '{col}'"
                assert not col.endswith("_x"),    f"{name}: found '{col}'"
                assert not col.endswith("_y"),    f"{name}: found '{col}'"

    def test_team_name_and_venue_in_output(self):
        result = self._run()
        for df in result.values():
            assert "team_name" in df.columns
            assert "venue_name" in df.columns


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
        cfg = self._capture()
        s = next(c for c in cfg if c["name"] == "api-football-standings")
        assert "season_id" in s["params"]

    def test_teams_has_season_id(self):
        cfg = self._capture()
        t = next(c for c in cfg if c["name"] == "api-football-teams")
        assert "season_id" in t["params"]

    def test_season_id_value_matches_config(self):
        cfg = self._capture()
        s = next(c for c in cfg if c["name"] == "api-football-standings")
        assert s["params"]["season_id"] == APIConfig.SEASON


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
