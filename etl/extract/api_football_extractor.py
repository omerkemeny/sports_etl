from config.settings import APIConfig
from etl.extract.api_extractor import FootballAPIExtractor


class ApiFootballExtractor:
    def __init__(self):
        self._http = FootballAPIExtractor()
        self._cfg = APIConfig()

    def extract(self) -> dict:
        cfg = self._cfg
        source_configs = [
            {
                "name": "api-football-standings",
                "url": cfg.FOOTBALL_URL,
                "params": {
                    "action": cfg.FOOTBALL_STANDINGS_ACTION,
                    "league_id": cfg.FOOTBALL_LEAGUE,
                    "season_id": cfg.SEASON,
                    "APIkey": cfg.FOOTBALL_KEY,
                },
            },
            {
                "name": "api-football-teams",
                "url": cfg.FOOTBALL_URL,
                "params": {
                    "action": cfg.FOOTBALL_TEAMS_ACTION,
                    "league_id": cfg.FOOTBALL_LEAGUE,
                    "season_id": cfg.SEASON,
                    "APIkey": cfg.FOOTBALL_KEY,
                },
            },
        ]
        results = self._http.fetch_all_sources(source_configs, max_workers=2)
        return {
            "standings": results.get("api-football-standings"),
            "teams": results.get("api-football-teams"),
        }
