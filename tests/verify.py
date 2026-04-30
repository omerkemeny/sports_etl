"""
End-to-end verification script.
Runs the pipeline twice — once in CSV mode, once in BigQuery mode —
and checks that outputs are correct.

Usage:
    python tests/verify.py
"""
import os
import sys
import subprocess
import pandas as pd
from google.cloud import bigquery

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.consts import FINAL_COLUMNS
from config.settings import APIConfig

TABLES    = ["api_sports_standardized", "api_football_standardized"]
CSV_PATHS = [f"exports/{t}.csv" for t in TABLES]
PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
errors = []


def check(label, condition, detail=""):
    tag = PASS if condition else FAIL
    print(f"  [{tag}] {label}" + (f"  ({detail})" if detail else ""))
    if not condition:
        errors.append(label)
    return condition


def run_pipeline(use_bigquery: bool):
    env = {**os.environ, "USE_BIGQUERY": "true" if use_bigquery else "false"}
    result = subprocess.run([sys.executable, "main.py"], env=env, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr[-800:])
    return result.returncode == 0


# ── CSV mode ──────────────────────────────────────────────────────────────────
print("\n=== CSV MODE ===")
ok = run_pipeline(use_bigquery=False)
check("Pipeline exited cleanly", ok)

for path in CSV_PATHS:
    if not os.path.exists(path):
        check(f"{path} exists", False)
        continue
    df = pd.read_csv(path)
    check(f"{path} — 20 rows",       len(df) == 20,                    f"{len(df)} rows")
    check(f"{path} — correct columns", list(df.columns) == FINAL_COLUMNS)
    check(f"{path} — no nulls",       df[["team_id","team_name","rank","points"]].notna().all().all())

# ── BigQuery mode ─────────────────────────────────────────────────────────────
print("\n=== BIGQUERY MODE ===")
ok = run_pipeline(use_bigquery=True)
check("Pipeline exited cleanly", ok)

if ok:
    cfg    = APIConfig()
    client = bigquery.Client(project=cfg.GCP_PROJECT_ID)
    for table in TABLES:
        table_id = f"{cfg.GCP_PROJECT_ID}.{cfg.BIGQUERY_DATASET}.{table}"
        try:
            bq_table = client.get_table(table_id)
            bq_cols  = [f.name for f in bq_table.schema]
            rows     = next(iter(client.query(f"SELECT COUNT(*) AS n FROM `{table_id}`").result()))["n"]
            check(f"{table} — exists in BigQuery",    True)
            check(f"{table} — 20 rows",               rows == 20,              f"{rows} rows")
            check(f"{table} — correct columns",       bq_cols == FINAL_COLUMNS)
        except Exception as e:
            check(f"{table} — BigQuery check", False, str(e))

# ── Summary ───────────────────────────────────────────────────────────────────
print()
if errors:
    print(f"FAILED ({len(errors)} checks): {', '.join(errors)}")
    sys.exit(1)
else:
    print("All checks passed.")
