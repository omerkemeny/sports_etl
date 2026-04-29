-- BigQuery DDL — Premier League standings schema
-- Dataset: sports_etl
-- Project: sports-etl-494718

CREATE TABLE IF NOT EXISTS `sports-etl-494718.sports_etl.api_sports_standardized`
(
    source        STRING    NOT NULL,
    season        INT64     NOT NULL,
    team_id       STRING    NOT NULL,
    team_name     STRING    NOT NULL,
    country       STRING,
    venue_name    STRING,
    rank          INT64     NOT NULL,
    played        INT64,
    won           INT64,
    drawn         INT64,
    lost          INT64,
    goals_for     INT64,
    goals_against INT64,
    points        INT64     NOT NULL,
    last_updated  TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `sports-etl-494718.sports_etl.api_football_standardized`
(
    source        STRING    NOT NULL,
    season        INT64     NOT NULL,
    team_id       STRING    NOT NULL,
    team_name     STRING    NOT NULL,
    country       STRING,
    venue_name    STRING,
    rank          INT64     NOT NULL,
    played        INT64,
    won           INT64,
    drawn         INT64,
    lost          INT64,
    goals_for     INT64,
    goals_against INT64,
    points        INT64     NOT NULL,
    last_updated  TIMESTAMP NOT NULL
);
