import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pytest
from unittest.mock import patch

from config.settings import APIConfig
from etl.extract.api_football_extractor import ApiFootballExtractor
from etl.pipeline import ETLPipeline
from etl.transform.standard_schema import STANDINGS_COLUMNS, TEAMS_COLUMNS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_standings(source: str) -> pd.DataFrame:
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


def _fake_teams(source: str) -> pd.DataFrame:
    return pd.DataFrame({
        "team_id":    ["10", "20"],
        "team_name":  ["Team A", "Team B"],
        "country":    ["England", "England"],
        "venue_name": ["Stadium A", "Stadium B"],
        "source":     [source, source],
    })


# ---------------------------------------------------------------------------
# 1. API-Football season_id parameter
# ---------------------------------------------------------------------------

class TestApiFootballSeasonParam:
    def _capture_source_configs(self):
        captured = {}

        def fake_fetch(source_configs, **kwargs):
            captured["configs"] = source_configs
            return {}

        with patch("etl.extract.api_football_extractor.FootballAPIExtractor") as MockHTTP:
            MockHTTP.return_value.fetch_all_sources.side_effect = fake_fetch
            ApiFootballExtractor().extract()

        return captured["configs"]

    def test_standings_includes_season_id(self):
        configs = self._capture_source_configs()
        standings = next(c for c in configs if c["name"] == "api-football-standings")
        assert "season_id" in standings["params"], "season_id missing from standings params"

    def test_teams_includes_season_id(self):
        configs = self._capture_source_configs()
        teams = next(c for c in configs if c["name"] == "api-football-teams")
        assert "season_id" in teams["params"], "season_id missing from teams params"

    def test_season_id_value_matches_config(self):
        configs = self._capture_source_configs()
        standings = next(c for c in configs if c["name"] == "api-football-standings")
        assert standings["params"]["season_id"] == APIConfig.SEASON


# ---------------------------------------------------------------------------
# 2. Merge produces no duplicate team_name columns
# ---------------------------------------------------------------------------

class TestMergeNoDuplicateTeamName:
    def _merged(self, source: str) -> pd.DataFrame:
        standings = _fake_standings(source)
        teams = _fake_teams(source)
        return standings.merge(
            teams.drop(columns=["source", "team_name"]),
            on="team_id",
            how="left",
        )

    def test_sports_has_single_team_name(self):
        df = self._merged("api-sports")
        assert "team_name" in df.columns
        assert "team_name_x" not in df.columns
        assert "team_name_y" not in df.columns

    def test_football_has_single_team_name(self):
        df = self._merged("api-football")
        assert "team_name" in df.columns
        assert "team_name_x" not in df.columns
        assert "team_name_y" not in df.columns

    def test_merged_row_count_preserved(self):
        df = self._merged("api-sports")
        assert len(df) == 2


# ---------------------------------------------------------------------------
# 3. Pipeline._transform end-to-end with mocked transformers
# ---------------------------------------------------------------------------

class TestPipelineTransform:
    def _run_transform(self):
        fake_sports = {
            "standings": _fake_standings("api-sports"),
            "teams":     _fake_teams("api-sports"),
        }
        fake_football = {
            "standings": _fake_standings("api-football"),
            "teams":     _fake_teams("api-football"),
        }

        with (
            patch("etl.pipeline.ApiSportsTransformer") as MockST,
            patch("etl.pipeline.ApiFootballTransformer") as MockFT,
        ):
            MockST.return_value.transform.return_value = fake_sports
            MockFT.return_value.transform.return_value = fake_football

            pipeline = ETLPipeline()
            result = pipeline._transform({"sports": {}, "football": {}})

        return result

    def test_output_keys_present(self):
        result = self._run_transform()
        assert "api_sports_standardized" in result
        assert "api_football_standardized" in result

    def test_no_x_y_suffixes_in_output(self):
        result = self._run_transform()
        for name, df in result.items():
            for col in df.columns:
                assert not col.endswith("_x"), f"{name}: unexpected column '{col}'"
                assert not col.endswith("_y"), f"{name}: unexpected column '{col}'"

    def test_team_name_present_in_output(self):
        result = self._run_transform()
        for name, df in result.items():
            assert "team_name" in df.columns, f"{name} missing team_name column"

    def test_country_and_venue_present(self):
        result = self._run_transform()
        for name, df in result.items():
            assert "country" in df.columns, f"{name} missing country"
            assert "venue_name" in df.columns, f"{name} missing venue_name"
