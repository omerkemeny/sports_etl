# Premier League Standings ETL

## Objective

Build a robust ETL pipeline that ingests, standardizes, and stores Premier League team and standings data from two
external football APIs, with BigQuery as the primary data warehouse and CSV as a local fallback.

---

## Architecture

```
API-Sports ──┐                         ┌── BigQuery (optional)
             ├── Transform ── Validate ─┤
API-Football ┘                         └── CSV (default)
```

Two independent source flows fetch data from separate APIs in parallel. Each flow produces a standardized DataFrame with
the same 15-column schema. A shared validation step guards the load phase. The pipeline writes to BigQuery when
`USE_BIGQUERY=true`, otherwise to a local CSV.

---

## Pipeline Flow

1. **Extract** — Fetch `standings` and `teams` endpoints from both APIs concurrently using `ThreadPoolExecutor`. Since
   API calls spend most of their time waiting for the network, running them in parallel threads cuts total fetch time
   from ~4s to ~1s. Retries on failure; logs which sources succeed or fail.
2. **Transform** — Normalise each source's raw JSON into the standard schema. Merge standings with team metadata. Add
   `season` and `last_updated` metadata columns. Enforce final column order.
3. **Validate** — Before loading, check that required columns (`team_id`, `team_name`, `rank`, `points`) are present and
   non-null. Skip invalid or empty DataFrames; log errors.
4. **Load** — Write each source to its own table (`api_sports_standardized`, `api_football_standardized`). Uses
   `WRITE_TRUNCATE` so each run refreshes the data.

---

## Standard Schema

| Column          | Type      | Notes                             |
|-----------------|-----------|-----------------------------------|
| `source`        | STRING    | `api-sports` or `api-football`    |
| `season`        | INT64     | e.g. `2023`                       |
| `team_id`       | STRING    | Source-specific team identifier   |
| `team_name`     | STRING    | Team name as returned by the API  |
| `country`       | STRING    | Country of the team               |
| `venue_name`    | STRING    | Home stadium                      |
| `rank`          | INT64     | League position                   |
| `played`        | INT64     | Games played                      |
| `won`           | INT64     | Wins                              |
| `drawn`         | INT64     | Draws                             |
| `lost`          | INT64     | Losses                            |
| `goals_for`     | INT64     | Goals scored                      |
| `goals_against` | INT64     | Goals conceded                    |
| `points`        | INT64     | Total points                      |
| `last_updated`  | TIMESTAMP | UTC timestamp of the pipeline run |

**Schema decisions:**

- `goal_diff` is excluded — it is directly derivable from `goals_for - goals_against` and adds no information.
- `league_name` is excluded — it is constant across all rows and belongs in query filters, not data.
- Both sources are kept in separate tables to preserve provenance and allow independent querying.

---

## Error Handling

- **API failures** — each source is fetched independently; if one fails, the other continues. Failures are logged and
  tracked in `PipelineStats.api_failures`.
- **JSON / network errors** — caught at the HTTP layer (`api_extractor.py`) with retry and exponential back-off.
- **Validation failures** — missing required columns or nulls cause the table to be skipped at load time. Errors are
  logged and tracked in `PipelineStats.errors`.
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
2. Create a service account with the **BigQuery Data Editor** and **BigQuery Job User** roles. Both are required: Data
   Editor grants table write access, Job User allows the client to execute load jobs.
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

Every pipeline run appends one row per loaded table to `exports/pipeline_run_log.csv` and, when `USE_BIGQUERY=true`, to
the `pipeline_run_log` BigQuery table.

Tracked fields: `run_id`, `source`, `table_name`, `rows_loaded`, `status`, `run_status`, `started_at`, `finished_at`,
`duration_seconds`, `error_count`, `api_failure_count`, `output_mode`.

### Monitoring Dashboard

A Looker Studio dashboard was created and connected to the BigQuery `pipeline_run_log` table. This satisfies the
optional monitoring bonus requirement.

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

## Schema Versioning

For lightweight versioning, each standardized row includes `run_id` and `schema_version`. This allows records to be
traced back to a specific pipeline execution and schema mapping version. Since BigQuery is used as the storage layer,
native table history/time travel can also be used for short-term recovery of previous table states. For longer-term
audit requirements, this could be extended to append-only snapshot tables.

---

## Scheduling

Scheduling is not implemented in this submission. The natural deployment approach is **Cloud Run Jobs** + **Cloud Scheduler**.

### 1. Build and push the container

```bash
gcloud builds submit --tag gcr.io/<PROJECT_ID>/sports-etl
```

### 2. Create the Cloud Run job

```bash
gcloud run jobs create sports-etl-job \
  --image gcr.io/<PROJECT_ID>/sports-etl \
  --region europe-west1 \
  --service-account sports-etl-sa@<PROJECT_ID>.iam.gserviceaccount.com \
  --set-env-vars USE_BIGQUERY=true,GCP_PROJECT_ID=<PROJECT_ID>,BIGQUERY_DATASET=sports_etl,API_SPORTS_KEY=<KEY>,API_FOOTBALL_KEY=<KEY>
```

On Cloud Run, Application Default Credentials are provided automatically by the attached service account —
`GOOGLE_APPLICATION_CREDENTIALS` is not required and should not be set.

The service account needs the **BigQuery Data Editor** and **BigQuery Job User** roles (same as local setup).

### 3. Schedule with Cloud Scheduler

```bash
gcloud scheduler jobs create http sports-etl-weekly \
  --location europe-west1 \
  --schedule "0 9 * * 1" \
  --uri "https://run.googleapis.com/v2/projects/<PROJECT_ID>/locations/europe-west1/jobs/sports-etl-job:run" \
  --http-method POST \
  --oauth-service-account-email sports-etl-sa@<PROJECT_ID>.iam.gserviceaccount.com
```

This triggers the job every Monday at 09:00 UTC. Adjust the cron expression for a different cadence.

---

## Assumptions and Limitations

- **Season** is hardcoded to `2023` in `config/consts.py`. Changing it requires a config update.
- **API-Football free tier** returns a mid-season snapshot (~34 gameweeks) rather than the full 38-game season, and does
  not filter reliably by `season_id`. This is a known free-tier limitation; the assignment explicitly allows it.
- **Team ID mismatch** — `team_id` values differ between the two sources and cannot be used to join the two tables
  directly. Each table is self-contained.
- **Authentication** — BigQuery authentication requires the system clock to be in sync with Google's servers (within ~5
  minutes). A drifted clock will cause `invalid_grant` JWT errors.
