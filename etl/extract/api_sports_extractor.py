import logging
from config.settings import APIConfig
from etl.extract.api_extractor import FootballAPIExtractor

logger = logging.getLogger(__name__)


class ApiSportsExtractor:
    def __init__(self):
        self._http = FootballAPIExtractor()
        self._cfg = APIConfig()

    def extract(self) -> dict:
        cfg = self._cfg
        headers = {"x-apisports-key": cfg.SPORTS_KEY}
        source_configs = [
            {
                "name": "api-sports-standings",
                "url": f"{cfg.SPORTS_URL}{cfg.SPORTS_STANDINGS_PATH}",
                "headers": headers,
                "params": {"league": cfg.SPORTS_LEAGUE, "season": cfg.SEASON},
            },
            {
                "name": "api-sports-teams",
                "url": f"{cfg.SPORTS_URL}{cfg.SPORTS_TEAMS_PATH}",
                "headers": headers,
                "params": {"league": cfg.SPORTS_LEAGUE, "season": cfg.SEASON},
            },
        ]
        results = self._http.fetch_all_sources(source_configs, max_workers=2)
        return {
            "standings": results.get("api-sports-standings"),
            "teams": results.get("api-sports-teams"),
        }
