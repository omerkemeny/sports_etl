# Premier League Standings ETL

## Objective

Build a robust ETL pipeline that ingests, standardizes, stores, and exposes Premier League team and standings data from two external football APIs, with BigQuery as the cloud storage layer and CSV as the default local output.

---

## Architecture

```text
API-Sports ──┐                         ┌── BigQuery
             ├── Transform ── Validate ─┤
API-Football ┘                         └── CSV
```

Two independent source flows fetch data from separate APIs in parallel. Each flow uses its own extractor and transformer because the APIs use different authentication methods, response structures, and field names.

Both sources are transformed into the same standardized schema, then stored separately as:

```text
api_sports_standardized
api_football_standardized
```

The assignment asks for a single standard schema while also keeping API-Sports and API-Football data separate. This design satisfies both requirements.

---

## Technology Choices

Python was chosen because it provides simple and readable ETL code, strong API/JSON handling, and mature DataFrame tooling through pandas.

BigQuery was chosen as the cloud storage layer because it is serverless, supports analytical querying, integrates directly with Looker Studio, and fits the assignment requirement to use GCP tooling.

CSV output is kept as the default local export so the results can be reviewed without requiring cloud credentials.

Cloud Run Jobs were used to demonstrate how the same pipeline can run as a cloud-native batch workload.

---

## Pipeline Flow

1. **Extract** — Fetch `standings` and `teams` endpoints from both APIs concurrently using `ThreadPoolExecutor`. Since API calls spend most of their time waiting for the network, running them in parallel threads reduces total extraction time.

2. **Transform** — Normalize each source's raw JSON into the standard schema. Merge standings with team metadata. Add `run_id`, `schema_version`, `season`, and `last_updated` metadata columns. Enforce final column order and log errors.

3. **Validate** — Before loading, check that required columns (`team_id`, `team_name`, `rank`, `points`) are present and non-null. Skip invalid or empty DataFrames and log errors.

4. **Load** — Write each source to its own table or CSV file. BigQuery uses `WRITE_TRUNCATE` for the standardized latest-state tables so each run refreshes the current view of the data.

5. **Monitor** — Write pipeline execution metrics to `pipeline_run_log`, which is used by the Looker Studio monitoring dashboard.

---

## Standard Schema

The standardized business schema contains 15 core fields. Two additional metadata fields, `run_id` and `schema_version`, are included for lightweight versioning and traceability.

| Column           | Type      | Notes                                        |
| ---------------- | --------- | -------------------------------------------- |
| `run_id`         | STRING    | Unique pipeline execution identifier         |
| `schema_version` | STRING    | Version of the standard schema/mapping logic |
| `source`         | STRING    | `api-sports` or `api-football`               |
| `season`         | INT64     | e.g. `2023`                                  |
| `team_id`        | STRING    | Source-specific team identifier              |
| `team_name`      | STRING    | Team name as returned by the API             |
| `country`        | STRING    | Country of the team                          |
| `venue_name`     | STRING    | Home stadium                                 |
| `rank`           | INT64     | League position                              |
| `played`         | INT64     | Games played                                 |
| `won`            | INT64     | Wins                                         |
| `drawn`          | INT64     | Draws                                        |
| `lost`           | INT64     | Losses                                       |
| `goals_for`      | INT64     | Goals scored                                 |
| `goals_against`  | INT64     | Goals conceded                               |
| `points`         | INT64     | Total points                                 |
| `last_updated`   | TIMESTAMP | UTC timestamp of the pipeline run            |

---

## Schema Decisions

* `goal_diff` is excluded because it is directly derivable from `goals_for - goals_against`.
* `league_name` is excluded because the pipeline is scoped to the Premier League and the value would be constant across all rows.
* `run_id` and `schema_version` are included as lightweight metadata fields for traceability and schema-versioning support.
* Both sources are kept in separate tables to preserve source lineage and allow independent querying.
* Team IDs are treated as source-specific identifiers.

---

## Source Field Mapping and Mismatches

The two APIs expose similar football concepts using different field names and response shapes.

Examples:

| Standard field  | API-Sports source field | API-Football source field |
| --------------- | ----------------------- | ------------------------- |
| `won`           | `all.win`               | `overall_league_W`        |
| `drawn`         | `all.draw`              | `overall_league_D`        |
| `lost`          | `all.lose`              | `overall_league_L`        |
| `goals_for`     | `all.goals.for`         | `overall_league_GF`       |
| `goals_against` | `all.goals.against`     | `overall_league_GA`       |
| `points`        | `points`                | `overall_league_PTS`      |

Each source has its own transformer that maps source-specific fields into the same standardized schema.

Team IDs differ between providers. For example, the same football club can have different IDs in API-Sports and API-Football. Team names are also not used for cross-source joins because provider naming conventions can differ. Therefore, the two standardized output tables are not joined together.

---

## Error Handling

* **API failures** — each source is fetched independently; if one endpoint fails, the rest of the pipeline can continue where possible. Failures are logged and tracked in `PipelineStats.api_failures`.
* **JSON / network errors** — caught at the HTTP layer in `api_extractor.py`, with retry and exponential backoff.
* **Validation failures** — missing required columns, null required values, or empty DataFrames cause that table to be skipped at load time. Errors are logged and tracked in `PipelineStats.errors`.
* **Partial failures** — empty or incomplete source outputs degrade gracefully instead of crashing the full pipeline.
* **BigQuery errors** — raised and logged with the full table ID for traceability.

---

## Logging

The pipeline uses a shared logging setup configured in `etl/utils/logger.py` and initialized from `main.py`.

Logs are written to stdout with timestamps, module names, log levels, and messages.

Main logged events include:

* Pipeline phase transitions: extraction, transformation, validation, and loading
* API request attempts, retries, and failures
* Source-level extraction status for `sports.standings`, `sports.teams`, `football.standings`, and `football.teams`
* Validation failures, including missing columns, null values, and empty DataFrames
* Output mode selection: CSV or BigQuery
* Row counts written to CSV or BigQuery
* Monitoring records written to `pipeline_run_log`

This keeps runtime behavior traceable without adding external logging infrastructure.

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

### 3. Run the pipeline in CSV mode

```bash
python main.py
```

Output is written to:

```text
exports/api_sports_standardized.csv
exports/api_football_standardized.csv
```

The final CSV exports are included in the repository so the output can be reviewed without requiring cloud credentials.

---

## BigQuery Setup

BigQuery loading is optional for local review. The project can be reviewed using the committed CSV outputs without cloud setup.

To enable BigQuery loading locally:

1. Create a GCP project and a BigQuery dataset named `sports_etl`.
2. Create a service account with the following roles:

   * BigQuery Data Editor
   * BigQuery Job User
3. Download the JSON key and save it locally as `config/gcp_key.json`. This file should never be committed.
4. Update `.env`:

```env
USE_BIGQUERY=true
GCP_PROJECT_ID=your-project-id
BIGQUERY_DATASET=sports_etl
GOOGLE_APPLICATION_CREDENTIALS=config/gcp_key.json
```

5. Run:

```bash
python main.py
```

Tables are created automatically on first run. The DDL is available in `schema.sql`.

---

## Cloud Run Job Deployment

In addition to local execution, the pipeline was containerized and successfully executed as a Google Cloud Run Job.

This is a good fit because the ETL is a batch workload: it starts, fetches API data, transforms and validates it, loads the results to BigQuery, writes monitoring metrics, and then exits.

```text
Cloud Run Job → Python ETL (`main.py`) → BigQuery tables → pipeline_run_log
```

Tested Cloud Run Job details:

| Field            | Value                                                                                |
| ---------------- | ------------------------------------------------------------------------------------ |
| Job name         | `premier-league-etl`                                                                 |
| Region           | `europe-west1`                                                                       |
| Container image  | `europe-west1-docker.pkg.dev/sports-etl-494718/sports-etl/premier-league-etl:latest` |
| Output mode      | BigQuery                                                                             |
| Execution result | Succeeded                                                                            |

### Containerization

The project includes a `Dockerfile` that packages the ETL code into a container image.

The image was built and pushed to Artifact Registry using Cloud Build:

```bash
gcloud builds submit --tag europe-west1-docker.pkg.dev/sports-etl-494718/sports-etl/premier-league-etl:latest
```

### Cloud Run Environment Variables

The Cloud Run Job uses environment variables instead of local `.env` files or service-account JSON keys.

Required variables:

```env
USE_BIGQUERY=true
GCP_PROJECT_ID=sports-etl-494718
BIGQUERY_DATASET=sports_etl
API_SPORTS_KEY=<api-sports-key>
API_FOOTBALL_KEY=<api-football-key>
```

`GOOGLE_APPLICATION_CREDENTIALS` is not used in Cloud Run. The job runs using its attached Google Cloud service account, which requires:

* BigQuery Data Editor
* BigQuery Job User

---

## Monitoring

Every pipeline run appends one row per loaded table to `exports/pipeline_run_log.csv` and, when `USE_BIGQUERY=true`, to the `pipeline_run_log` BigQuery table.

Tracked fields:

```text
run_id
source
table_name
rows_loaded
status
run_status
started_at
finished_at
duration_seconds
error_count
api_failure_count
output_mode
```

### Monitoring Dashboard

A Looker Studio dashboard was created and connected to the BigQuery `pipeline_run_log` table. This satisfies the optional monitoring bonus requirement.

[Looker Studio Dashboard](https://datastudio.google.com/reporting/69327344-bcf0-4a0e-af62-09136ab595a2)

The dashboard tracks:

* Pipeline success rate
* Load success rate per source
* API health
* Total rows processed
* Average pipeline duration
* Total errors
* Per-run table details

---

## Schema Versioning

For lightweight versioning, each standardized row includes `run_id` and `schema_version`.

This allows records to be traced back to a specific pipeline execution and schema mapping version. Since BigQuery is used as the storage layer, native table history/time travel can also be used for short-term recovery of previous table states.

For longer-term audit requirements, this could be extended to append-only snapshot tables.

---

## Scheduling

The pipeline is deployable as a Cloud Run Job and can be scheduled using Cloud Scheduler.

In this submission, the Cloud Run Job was executed manually to verify cloud execution and BigQuery loading. A production setup could trigger the same Cloud Run Job on a cron schedule, for example daily during the season or weekly for periodic refreshes.

---

## Assumptions and Limitations

* **Season** is hardcoded to `2023` in `config/settings.py`. Changing it requires a config update.
* **API-Football free tier** returns a mid-season snapshot rather than the full 38-game season and does not filter reliably by `season_id`. This is a known free-tier limitation, and the assignment explicitly allows imperfect seasonal completeness when the pipeline design is sound.
* **Free-tier API limits** may cause partial or misaligned seasonal data. The pipeline prioritizes structural integrity, standardized transformation, validation, and traceable loading.
* **Team ID mismatch** — each provider uses its own team identifiers. Therefore, `team_id` is treated as source-specific, and the two output tables are not joined directly across APIs.

