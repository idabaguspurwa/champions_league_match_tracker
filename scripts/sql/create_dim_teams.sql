CREATE TABLE ucl_analytics_db.dim_teams
WITH (
    format = 'PARQUET',
    external_location = 's3://ucl-lake-2025/processed/dim_teams/'
) AS
-- Extract teams from JSON data - improved approach for fragmented data
WITH teams_raw AS (
    SELECT 
        col0,
        year
    FROM ucl_analytics_db.raw
    WHERE partition_0 IN ('teams', 'schedules', 'team_rosters')
      AND year IN ('2024', '2025')
      AND col0 IS NOT NULL
      AND col0 != ''
      AND LENGTH(col0) > 10
      AND (col0 LIKE '%"team":%' OR col0 LIKE '%"competitor":%' OR col0 LIKE '%"participant":%' OR col0 LIKE '%"displayName":%')
),
-- Extract team information from fragments - more aggressive pattern matching
teams_extracted AS (
    SELECT 
        -- Try multiple patterns for team ID
        COALESCE(
            REGEXP_EXTRACT(col0, '"id":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"id":\s*(\d+)', 1),
            REGEXP_EXTRACT(col0, '"team_id":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"team_id":\s*(\d+)', 1)
        ) as team_id,
        -- Try multiple patterns for team name
        COALESCE(
            REGEXP_EXTRACT(col0, '"displayName":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"name":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"team_name":\s*"([^"]+)"', 1)
        ) as team_name,
        -- Try multiple patterns for abbreviation
        COALESCE(
            REGEXP_EXTRACT(col0, '"abbreviation":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"abbrev":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"short_name":\s*"([^"]+)"', 1)
        ) as team_abbr,
        -- Try multiple patterns for short name
        COALESCE(
            REGEXP_EXTRACT(col0, '"shortDisplayName":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"shortName":\s*"([^"]+)"', 1)
        ) as team_short_name,
        -- Try multiple patterns for color
        COALESCE(
            REGEXP_EXTRACT(col0, '"color":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"teamColor":\s*"([^"]+)"', 1)
        ) as team_color,
        -- Try multiple patterns for alternate color
        COALESCE(
            REGEXP_EXTRACT(col0, '"alternateColor":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"altColor":\s*"([^"]+)"', 1)
        ) as team_alt_color,
        -- Try multiple patterns for location
        COALESCE(
            REGEXP_EXTRACT(col0, '"location":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"city":\s*"([^"]+)"', 1)
        ) as team_location,
        -- Try multiple patterns for logo
        COALESCE(
            REGEXP_EXTRACT(col0, '"logo":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"logoUrl":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '(https://[^"]*teamlogos[^"]*)', 1)
        ) as team_logo,
        year,
        col0
    FROM teams_raw
    WHERE col0 LIKE '%"id":%' 
       OR col0 LIKE '%"team_id":%'
       OR col0 LIKE '%"displayName":%'
       OR col0 LIKE '%"name":%'
       OR col0 LIKE '%"abbreviation":%'
),
-- Group by team_id and reconstruct complete records
real_teams AS (
    SELECT 
        team_id,
        MAX(team_name) as team_name,
        MAX(team_abbr) as team_abbr,
        MAX(team_short_name) as team_short_name,
        MAX(team_logo) as team_logo_url,
        CASE 
            WHEN MAX(team_color) IS NOT NULL AND MAX(team_color) != '' THEN
                CASE 
                    WHEN MAX(team_color) LIKE '#%' THEN MAX(team_color)
                    ELSE '#' || MAX(team_color)
                END
            ELSE '#000000'
        END as team_color,
        CASE 
            WHEN MAX(team_alt_color) IS NOT NULL AND MAX(team_alt_color) != '' THEN
                CASE 
                    WHEN MAX(team_alt_color) LIKE '#%' THEN MAX(team_alt_color)
                    ELSE '#' || MAX(team_alt_color)
                END
            ELSE '#FFFFFF'
        END as team_alternate_color,
        MAX(team_location) as team_location,
        COALESCE(MAX(team_name), MAX(team_short_name), MAX(team_abbr)) as team_nickname,
        CAST(MAX(year) AS INTEGER) as season_year,
        CAST('2025-01-01 00:00:00.000' AS TIMESTAMP) as created_at
    FROM teams_extracted
    WHERE team_id IS NOT NULL
      AND team_id != ''
      AND LENGTH(team_id) > 0
      AND (team_name IS NOT NULL OR team_abbr IS NOT NULL)
    GROUP BY team_id
),
-- Count real teams found
real_teams_count AS (
    SELECT COUNT(*) as count_real_teams FROM real_teams
)
-- Return only real data - no static fallback
SELECT 
    CAST(team_id AS VARCHAR) as team_id,
    CAST(team_name AS VARCHAR) as team_name,
    CAST(team_abbr AS VARCHAR) as team_abbr,
    CAST(team_short_name AS VARCHAR) as team_short_name,
    CAST(team_logo_url AS VARCHAR) as team_logo_url,
    CAST(team_color AS VARCHAR) as team_color,
    CAST(team_alternate_color AS VARCHAR) as team_alternate_color,
    CAST(team_location AS VARCHAR) as team_location,
    CAST(team_nickname AS VARCHAR) as team_nickname,
    CAST(season_year AS INTEGER) as season_year,
    CAST(created_at AS TIMESTAMP(3)) as created_at
FROM real_teams
WHERE team_id IS NOT NULL;