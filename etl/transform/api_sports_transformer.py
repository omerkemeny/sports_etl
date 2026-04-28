import logging
import pandas as pd
from etl.transform.standard_schema import STANDINGS_COLUMNS, TEAMS_COLUMNS

logger = logging.getLogger(__name__)


class ApiSportsTransformer:
    def transform_standings(self, raw: list) -> pd.DataFrame:
        standings = raw[0]["league"]["standings"][0]
        df = pd.json_normalize(standings)
        return pd.DataFrame({
            "rank":          df["rank"],
            "team_id":       df["team.id"].astype(str),
            "team_name":     df["team.name"],
            "points":        df["points"],
            "played":        df["all.played"],
            "won":           df["all.win"],
            "drawn":         df["all.draw"],
            "lost":          df["all.lose"],
            "goals_for":     df["all.goals.for"],
            "goals_against": df["all.goals.against"],
            "goal_diff":     df["goalsDiff"],
            "source":        "api-sports",
        })[STANDINGS_COLUMNS]

    def transform_teams(self, raw: list) -> pd.DataFrame:
        df = pd.json_normalize(raw)
        return pd.DataFrame({
            "team_id":    df["team.id"].astype(str),
            "team_name":  df["team.name"],
            "country":    df["team.country"],
            "venue_name": df["venue.name"],
            "source":     "api-sports",
        })[TEAMS_COLUMNS]

    def transform(self, data: dict) -> dict:
        return {
            "standings": self.transform_standings(data["standings"]) if data.get("standings") else pd.DataFrame(),
            "teams":     self.transform_teams(data["teams"])         if data.get("teams")     else pd.DataFrame(),
        }
