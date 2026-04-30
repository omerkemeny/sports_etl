import pandas as pd
from config.consts import STANDINGS_COLUMNS, TEAMS_COLUMNS


class ApiFootballTransformer:
    def transform_standings(self, raw: list) -> pd.DataFrame:
        df = pd.DataFrame(raw)
        return pd.DataFrame({
            "rank":          df["overall_league_position"].astype(int),
            "team_id":       df["team_id"].astype(str),
            "team_name":     df["team_name"],
            "points":        df["overall_league_PTS"].astype(int),
            "played":        df["overall_league_payed"].astype(int),
            "won":           df["overall_league_W"].astype(int),
            "drawn":         df["overall_league_D"].astype(int),
            "lost":          df["overall_league_L"].astype(int),
            "goals_for":     df["overall_league_GF"].astype(int),
            "goals_against": df["overall_league_GA"].astype(int),
            "source":        "api-football",
        })[STANDINGS_COLUMNS]

    def transform_teams(self, raw: list) -> pd.DataFrame:
        df = pd.DataFrame(raw)
        venue_name = df["venue"].apply(lambda v: v.get("venue_name", "") if isinstance(v, dict) else "")
        return pd.DataFrame({
            "team_id":    df["team_key"].astype(str),
            "team_name":  df["team_name"],
            "country":    df["team_country"],
            "venue_name": venue_name,
            "source":     "api-football",
        })[TEAMS_COLUMNS]

    def transform(self, data: dict) -> dict:
        return {
            "standings": self.transform_standings(data["standings"]) if data.get("standings") else pd.DataFrame(),
            "teams":     self.transform_teams(data["teams"])         if data.get("teams")     else pd.DataFrame(),
        }
