from pathlib import Path

EXPORTS_DIR      = Path(__file__).resolve().parent.parent / "exports"
LOG_CSV          = EXPORTS_DIR / "pipeline_run_log.csv"
MONITORING_TABLE = "pipeline_run_log"

REQUIRED_COLUMNS = {"team_id", "team_name", "rank", "points"}

# --- API-Sports ---
SPORTS_URL            = "https://v3.football.api-sports.io"
SPORTS_LEAGUE         = 39  # Premier League
SPORTS_STANDINGS_PATH = "/standings"
SPORTS_TEAMS_PATH     = "/teams"

# --- API-Football ---
FOOTBALL_URL              = "https://apiv3.apifootball.com/"
FOOTBALL_LEAGUE           = 152  # Premier League
FOOTBALL_STANDINGS_ACTION = "get_standings"
FOOTBALL_TEAMS_ACTION     = "get_teams"

# --- Pipeline ---
SEASON      = 2023
TIMEOUT     = 30
MAX_RETRIES = 3
RETRY_DELAY = 2
