# Project Context for Claude

This repository is for a data engineering assignment.

## Assignment Goal
Build an ETL pipeline that:
- Extracts sports/event data from the required source
- Loads raw data into BigQuery
- Transforms/cleans the data
- Produces analytical tables/views
- Includes clear documentation and reproducible setup

## Important Assignment Requirements
Claude should always follow the assignment file located at:

`assignment/Data engineer home task - Premier league standings.pdf`

Before suggesting major design or code changes, check whether the idea fits the assignment requirements.

## Current Design Direction
- Python ETL project
- BigQuery as the cloud data warehouse
- PostgreSQL/local DB only if needed for development/testing
- Clean separation between extraction, loading, transformation, config, and orchestration
- Avoid over-engineering unless it helps satisfy the assignment

## Coding Rules
- Keep code simple and readable
- Prefer small modules and clear function names
- Add useful logging
- Add `.env.example`
- Do not commit real credentials
- Make setup easy for the reviewer

## What Claude Should Help With
- Review architecture against the assignment
- Suggest missing milestones
- Generate or refactor code
- Improve README and submission instructions
- Check that the project matches the assignment requirements