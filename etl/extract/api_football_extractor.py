import logging

from config.settings import APIConfig
from config.consts import (
    FOOTBALL_URL, FOOTBALL_LEAGUE, FOOTBALL_STANDINGS_ACTION, FOOTBALL_TEAMS_ACTION, SEASON
)
from etl.extract.api_extractor import FootballAPIExtractor

logger = logging.getLogger(__name__)


def _unwrap_football(data):
    if isinstance(data, dict) and 'error' in data:
        logger.warning(f"api-football error: {data.get('message')}")
        return None
    return data


class ApiFootballExtractor:
    def __init__(self):
        self._http = FootballAPIExtractor()
        self._key  = APIConfig().FOOTBALL_KEY

    def extract(self) -> dict:
        source_configs = [
            {
                "name":   "api-football-standings",
                "url":    FOOTBALL_URL,
                "params": {
                    "action":    FOOTBALL_STANDINGS_ACTION,
                    "league_id": FOOTBALL_LEAGUE,
                    "season_id": SEASON,
                    "APIkey":    self._key,
                },
                "unwrap": _unwrap_football,
            },
            {
                "name":   "api-football-teams",
                "url":    FOOTBALL_URL,
                "params": {
                    "action":    FOOTBALL_TEAMS_ACTION,
                    "league_id": FOOTBALL_LEAGUE,
                    "season_id": SEASON,
                    "APIkey":    self._key,
                },
                "unwrap": _unwrap_football,
            },
        ]
        results = self._http.fetch_all_sources(source_configs, max_workers=2)
        return {
            "standings": results.get("api-football-standings"),
            "teams":     results.get("api-football-teams"),
        }
