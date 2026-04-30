from config.settings import APIConfig
from config.consts import SPORTS_URL, SPORTS_LEAGUE, SPORTS_STANDINGS_PATH, SPORTS_TEAMS_PATH, SEASON
from etl.extract.api_extractor import FootballAPIExtractor


class ApiSportsExtractor:
    def __init__(self):
        self._http = FootballAPIExtractor()
        self._key  = APIConfig().SPORTS_KEY

    def extract(self) -> dict:
        headers = {"x-apisports-key": self._key}
        source_configs = [
            {
                "name":    "api-sports-standings",
                "url":     f"{SPORTS_URL}{SPORTS_STANDINGS_PATH}",
                "headers": headers,
                "params":  {"league": SPORTS_LEAGUE, "season": SEASON},
            },
            {
                "name":    "api-sports-teams",
                "url":     f"{SPORTS_URL}{SPORTS_TEAMS_PATH}",
                "headers": headers,
                "params":  {"league": SPORTS_LEAGUE, "season": SEASON},
            },
        ]
        results = self._http.fetch_all_sources(source_configs, max_workers=2)
        return {
            "standings": results.get("api-sports-standings"),
            "teams":     results.get("api-sports-teams"),
        }
