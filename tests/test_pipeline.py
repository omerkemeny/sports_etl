import logging
import os
import sys
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

from etl.pipeline import ETLPipeline


def _to_df_sports_standings(data: list) -> pd.DataFrame:
    standings = data[0]["league"]["standings"][0]
    return pd.json_normalize(standings)


def _to_df_sports_teams(data: list) -> pd.DataFrame:
    return pd.json_normalize(data)


def _to_df_football(data: list) -> pd.DataFrame:
    return pd.DataFrame(data)


def test_extract() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    with ETLPipeline() as pipeline:
        result = pipeline._extract()

    df_sports_standings = _to_df_sports_standings(result["api_sports_standings"]) if result.get("api_sports_standings") else pd.DataFrame()
    df_sports_teams     = _to_df_sports_teams(result["api_sports_teams"])         if result.get("api_sports_teams")     else pd.DataFrame()
    df_football_standings = _to_df_football(result["api_football_standings"])     if result.get("api_football_standings") else pd.DataFrame()
    df_football_teams     = _to_df_football(result["api_football_teams"])         if result.get("api_football_teams")     else pd.DataFrame()

    for label, df in [
        ("API-SPORTS STANDINGS",  df_sports_standings),
        ("API-SPORTS TEAMS",      df_sports_teams),
        ("API-FOOTBALL STANDINGS", df_football_standings),
        ("API-FOOTBALL TEAMS",    df_football_teams),
    ]:
        print(f"\n=== {label} ===")
        print(df.head(2))

    return df_sports_standings, df_sports_teams, df_football_standings, df_football_teams


df_sports_standings = df_sports_teams = df_football_standings = df_football_teams = pd.DataFrame()

if __name__ == "__main__":
    df_sports_standings, df_sports_teams, df_football_standings, df_football_teams = test_extract()
    df_sports_standings.to_csv("api_sports_standings.csv", index=False)
    df_sports_teams.to_csv("api_sports_teams.csv", index=False)
    df_football_standings.to_csv("api_football_standings.csv", index=False)
    df_football_teams.to_csv("api_football_teams.csv", index=False)
    print("\nSaved 4 CSV files.")
