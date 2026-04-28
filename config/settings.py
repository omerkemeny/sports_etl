import os
from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

@dataclass(frozen=True)
class APIConfig:
    # --- API-Sports (Football API v3) ---
    SPORTS_URL: str = "https://v3.football.api-sports.io"
    SPORTS_KEY: str = os.getenv("API_SPORTS_KEY", "")
    SPORTS_LEAGUE: int = 39  # Premier League
    SPORTS_STANDINGS_PATH: str = "/standings"
    SPORTS_TEAMS_PATH: str = "/teams"

    # --- API-Football ---
    FOOTBALL_URL: str = "https://apiv3.apifootball.com/"
    FOOTBALL_KEY: str = os.getenv("API_FOOTBALL_KEY", "")
    FOOTBALL_LEAGUE: int = 152  # Premier League
    FOOTBALL_STANDINGS_ACTION: str = "get_standings"
    FOOTBALL_TEAMS_ACTION: str = "get_teams"

    # --- Common Settings ---
    SEASON: int = 2023
    TIMEOUT: int = 30
    MAX_RETRIES: int = 1
    RETRY_DELAY: int = 2
