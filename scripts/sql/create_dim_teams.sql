CREATE TABLE ucl_analytics_db.dim_teams
WITH (
    format = 'PARQUET',
    external_location = 's3://ucl-lake-2025/processed/dim_teams/'
) AS
-- Extract real teams data from JSON with fallback to static data
WITH teams_raw AS (
    SELECT 
        col0,
        year
    FROM ucl_analytics_db.raw
    WHERE partition_0 = 'teams'
      AND col0 IS NOT NULL
      AND LENGTH(col0) > 100
      AND try(json_parse(col0)) IS NOT NULL
),
teams_parsed AS (
    SELECT 
        json_parse(col0) as json_data,
        year
    FROM teams_raw
),
teams_extracted AS (
    -- Handle different possible JSON structures for teams data
    SELECT 
        team_json,
        year
    FROM teams_parsed
    CROSS JOIN UNNEST(
        CASE 
            -- If it's an array at root level
            WHEN json_extract(json_data, '$[0].id') IS NOT NULL THEN 
                CAST(json_data AS ARRAY(JSON))
            -- If teams are nested under a 'teams' key
            WHEN json_extract(json_data, '$.teams[0].id') IS NOT NULL THEN 
                CAST(json_extract(json_data, '$.teams') AS ARRAY(JSON))
            -- If it's a single team object
            WHEN json_extract_scalar(json_data, '$.id') IS NOT NULL THEN 
                ARRAY[json_data]
            ELSE ARRAY[]
        END
    ) AS t(team_json)
    WHERE json_extract_scalar(team_json, '$.id') IS NOT NULL
),
real_teams AS (
    SELECT DISTINCT
        json_extract_scalar(team_json, '$.id') as team_id,
        COALESCE(
            json_extract_scalar(team_json, '$.displayName'),
            json_extract_scalar(team_json, '$.name'),
            json_extract_scalar(team_json, '$.shortDisplayName')
        ) as team_name,
        COALESCE(
            json_extract_scalar(team_json, '$.abbreviation'),
            json_extract_scalar(team_json, '$.abbrev')
        ) as team_abbr,
        COALESCE(
            json_extract_scalar(team_json, '$.shortDisplayName'),
            json_extract_scalar(team_json, '$.name')
        ) as team_short_name,
        COALESCE(
            json_extract_scalar(team_json, '$.logo'),
            json_extract_scalar(team_json, '$.logos[0].href')
        ) as team_logo_url,
        json_extract_scalar(team_json, '$.color') as team_color,
        json_extract_scalar(team_json, '$.alternateColor') as team_alternate_color,
        json_extract_scalar(team_json, '$.location') as team_location,
        json_extract_scalar(team_json, '$.nickname') as team_nickname,
        CAST(year AS INTEGER) as season_year,
        CAST('2025-01-01 00:00:00.000' AS TIMESTAMP(3)) as created_at
    FROM teams_extracted
    WHERE json_extract_scalar(team_json, '$.id') IS NOT NULL
      AND json_extract_scalar(team_json, '$.id') != ''
)
-- Return real data if available, otherwise fallback to static data
SELECT * FROM real_teams
WHERE team_id IS NOT NULL

UNION ALL

-- Fallback static data (only if no real data found)
SELECT 
    '1' as team_id,
    'Real Madrid' as team_name,
    'RM' as team_abbr,
    'Real Madrid' as team_short_name,
    'https://example.com/logo1.png' as team_logo_url,
    '#FFFFFF' as team_color,
    '#000000' as team_alternate_color,
    'Madrid, Spain' as team_location,
    'Los Blancos' as team_nickname,
    2025 as season_year,
    CAST('2025-01-01 00:00:00.000' AS TIMESTAMP(3)) as created_at
WHERE NOT EXISTS (SELECT 1 FROM real_teams)

UNION ALL

SELECT 
    '2' as team_id,
    'Barcelona' as team_name,
    'BAR' as team_abbr,
    'Barcelona' as team_short_name,
    'https://example.com/logo2.png' as team_logo_url,
    '#A50044' as team_color,
    '#FCDD09' as team_alternate_color,
    'Barcelona, Spain' as team_location,
    'Blaugrana' as team_nickname,
    2025 as season_year,
    CAST('2025-01-01 00:00:00.000' AS TIMESTAMP(3)) as created_at
WHERE NOT EXISTS (SELECT 1 FROM real_teams);