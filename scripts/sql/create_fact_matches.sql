-- DEFINITIVE FIX: Load real matches from extracted CSV data
-- Real matches extracted from S3 JSON files and saved as CSV
-- Note: External table real_matches_csv must exist before running this

CREATE TABLE ucl_analytics_db.fact_matches
WITH (
    format = 'PARQUET',
    external_location = 's3://ucl-lake-2025/processed/fact_matches/'
) AS
SELECT 
    CAST(match_id AS VARCHAR) as match_id,
    CAST(TRY(from_iso8601_timestamp(match_datetime)) AS TIMESTAMP) as match_datetime,
    CAST(match_date AS VARCHAR) as match_date,
    CAST(CASE WHEN completed = 'True' THEN true ELSE false END AS BOOLEAN) as completed,
    CAST(match_status AS VARCHAR) as match_status,
    CAST(match_status AS VARCHAR) as match_status_detail,
    CAST(home_team_id AS VARCHAR) as home_team_id,
    CAST(TRY(CAST(home_score AS INTEGER)) AS INTEGER) as home_score,
    CAST(away_team_id AS VARCHAR) as away_team_id,
    CAST(TRY(CAST(away_score AS INTEGER)) AS INTEGER) as away_score,
    CAST(match_name AS VARCHAR) as match_name,
    CAST(match_short_name AS VARCHAR) as match_short_name,
    CAST(venue AS VARCHAR) as venue,
    CAST(TRY(CAST(season_year AS INTEGER)) AS INTEGER) as season_year
FROM ucl_analytics_db.real_matches_csv
WHERE match_id IS NOT NULL
  AND match_id != ''
  AND home_team_id IS NOT NULL
  AND away_team_id IS NOT NULL
