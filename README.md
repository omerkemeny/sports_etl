# Premier League Standings ETL

## Objective

Build a robust ETL pipeline that ingests, standardises, and stores Premier League team and standings data from two external football APIs, with BigQuery as the primary data warehouse and CSV as a local fallback.

---

## Architecture

```
API-Sports ──┐                         ┌── BigQuery (optional)
             ├── Transform ── Validate ─┤
API-Football ┘                         └── CSV (default)
```

Two independent source flows fetch data from separate APIs in parallel. Each flow produces a standardised DataFrame with the same 15-column schema. A shared validation step guards the load phase. The pipeline writes to BigQuery when `USE_BIGQUERY=true`, otherwise to local CSV.

---

## Pipeline Flow

1. **Extract** — Fetch `standings` and `teams` endpoints from both APIs concurrently using `ThreadPoolExecutor`. Retries on failure; logs which sources succeed or fail.
2. **Transform** — Normalise each source's raw JSON into the standard schema. Merge standings with team metadata. Add `season` and `last_updated` metadata columns. Enforce final column order.
3. **Validate** — Before loading, check that required columns (`team_id`, `team_name`, `rank`, `points`) are present and non-null. Skip invalid or empty DataFrames; log errors.
4. **Load** — Write each source to its own table (`api_sports_standardized`, `api_football_standardized`). Uses `WRITE_TRUNCATE` so each run refreshes the data.

---

## Standard Schema

| Column | Type | Notes |
|---|---|---|
| `source` | STRING | `api-sports` or `api-football` |
| `season` | INT64 | e.g. `2023` |
| `team_id` | STRING | Source-specific team identifier |
| `team_name` | STRING | Team name as returned by the API |
| `country` | STRING | Country of the team |
| `venue_name` | STRING | Home stadium |
| `rank` | INT64 | League position |
| `played` | INT64 | Games played |
| `won` | INT64 | Wins |
| `drawn` | INT64 | Draws |
| `lost` | INT64 | Losses |
| `goals_for` | INT64 | Goals scored |
| `goals_against` | INT64 | Goals conceded |
| `points` | INT64 | Total points |
| `last_updated` | TIMESTAMP | UTC timestamp of the pipeline run |

**Schema decisions:**
- `goal_diff` is excluded — it is directly derivable from `goals_for - goals_against` and adds no information.
- `league_name` is excluded — it is constant across all rows and belongs in query filters, not data.
- Both sources are kept in separate tables to preserve provenance and allow independent querying.

---

## Error Handling

- **API failures** — each source is fetched independently; if one fails, the other continues. Failures are logged and tracked in `PipelineStats.api_failures`.
- **JSON / network errors** — caught at the HTTP layer (`api_extractor.py`) with retry and exponential back-off.
- **Validation failures** — missing required columns or nulls cause the table to be skipped at load time. Errors are logged and tracked in `PipelineStats.errors`.
- **BigQuery errors** — raised and logged with the full table ID for traceability.

---

## Running Locally

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Fill in your API keys
```

### 3. Run the pipeline (CSV mode)

```bash
python main.py
```

Output is written to `exports/api_sports_standardized.csv` and `exports/api_football_standardized.csv`.

---

## BigQuery Setup

1. Create a GCP project and a BigQuery dataset named `sports_etl`.
2. Create a service account with the **BigQuery Data Editor** and **BigQuery Job User** roles. Both are required: Data Editor grants table write access, Job User allows the client to execute load jobs.
3. Download the JSON key and save it as `config/gcp_key.json` (never committed).
4. Update `.env`:

```
USE_BIGQUERY=true
GCP_PROJECT_ID=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=config/gcp_key.json
```

5. Run:

```bash
python main.py
```

Tables are created automatically on first run. The DDL is in `schema.sql`.

---

## Monitoring

Every pipeline run appends one row per loaded table to `exports/pipeline_run_log.csv` and, when `USE_BIGQUERY=true`, to the `pipeline_run_log` BigQuery table.

Tracked fields: `run_id`, `source`, `table_name`, `rows_loaded`, `status`, `run_status`, `started_at`, `finished_at`, `duration_seconds`, `error_count`, `api_failure_count`, `output_mode`.

### Monitoring Dashboard

A Looker Studio dashboard was created and connected to the BigQuery `pipeline_run_log` table. This satisfies the optional monitoring bonus requirement.

**Dashboard:** https://datastudio.google.com/reporting/69327344-bcf0-4a0e-af62-09136ab595a2

The dashboard tracks:
- Pipeline success rate
- Load success rate per source
- API health (API failure count)
- Total rows processed
- Average pipeline duration
- Total errors
- Per-run table details

---

## Scheduling

Scheduling is not implemented in this submission. The natural approach would be:

- **Cloud Run** to containerise and run the pipeline on demand.
- **Cloud Scheduler** to trigger the Cloud Run job on a cron schedule (e.g. weekly, or daily during the season).

---

## Assumptions and Limitations

- **Season** is hardcoded to `2023` in `config/settings.py`. Changing it requires a config update.
- **API-Football free tier** returns a mid-season snapshot (~34 gameweeks) rather than the full 38-game season, and does not filter reliably by `season_id`. This is a known free-tier limitation; the assignment explicitly allows it.
- **Team ID mismatch** — `team_id` values differ between the two sources and cannot be used to join the two tables directly. Each table is self-contained.
- **Authentication** — BigQuery authentication requires the system clock to be in sync with Google's servers (within ~5 minutes). A drifted clock will cause `invalid_grant` JWT errors.
