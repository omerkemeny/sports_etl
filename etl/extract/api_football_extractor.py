from config.settings import APIConfig
from config.consts import (
    FOOTBALL_URL, FOOTBALL_LEAGUE, FOOTBALL_STANDINGS_ACTION, FOOTBALL_TEAMS_ACTION, SEASON
)
from etl.extract.api_extractor import FootballAPIExtractor


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
            },
        ]
        results = self._http.fetch_all_sources(source_configs, max_workers=2)
        return {
            "standings": results.get("api-football-standings"),
            "teams":     results.get("api-football-teams"),
        }
