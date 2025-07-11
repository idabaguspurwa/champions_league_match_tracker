CREATE TABLE ucl_analytics_db.dim_players
WITH (
    format = 'PARQUET',
    external_location = 's3://ucl-lake-2025/processed/dim_players/'
) AS
-- Extract real players data from team rosters - improved fragment approach
WITH rosters_raw AS (
    SELECT 
        col0,
        year
    FROM ucl_analytics_db.raw
    WHERE partition_0 = 'team_rosters'
      AND year IN ('2024', '2025')
      AND col0 IS NOT NULL
      AND LENGTH(col0) > 100  -- Reduced threshold for fragment detection
      AND (col0 LIKE '%"athletes":%' OR col0 LIKE '%"id":%' OR col0 LIKE '%"displayName":%')
),
-- Extract players from fragments - more aggressive pattern matching
players_extracted AS (
    SELECT 
        -- Try multiple patterns for player ID
        COALESCE(
            REGEXP_EXTRACT(col0, '"id":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"id":\s*(\d+)', 1),
            REGEXP_EXTRACT(col0, '"playerId":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"playerId":\s*(\d+)', 1)
        ) as player_id,
        -- Try multiple patterns for player name
        COALESCE(
            REGEXP_EXTRACT(col0, '"displayName":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"fullName":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"name":\s*"([^"]+)"', 1)
        ) as player_name,
        -- Try multiple patterns for first name
        COALESCE(
            REGEXP_EXTRACT(col0, '"firstName":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"first_name":\s*"([^"]+)"', 1)
        ) as first_name,
        -- Try multiple patterns for last name
        COALESCE(
            REGEXP_EXTRACT(col0, '"lastName":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"last_name":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"surname":\s*"([^"]+)"', 1)
        ) as last_name,
        -- Try multiple patterns for jersey number
        COALESCE(
            TRY(CAST(REGEXP_EXTRACT(col0, '"jersey":\s*"([^"]+)"', 1) AS VARCHAR)),
            TRY(CAST(REGEXP_EXTRACT(col0, '"jerseyNumber":\s*"([^"]+)"', 1) AS VARCHAR)),
            TRY(CAST(REGEXP_EXTRACT(col0, '"jersey":\s*(\d+)', 1) AS VARCHAR)),
            TRY(CAST(REGEXP_EXTRACT(col0, '"jerseyNumber":\s*(\d+)', 1) AS VARCHAR))
        ) as jersey_number,
        -- Try multiple patterns for position
        COALESCE(
            REGEXP_EXTRACT(col0, '"position":\s*\{[^}]*"displayName":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"position":\s*\{[^}]*"name":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"position":\s*"([^"]+)"', 1)
        ) as position,
        -- Try multiple patterns for position abbreviation
        COALESCE(
            REGEXP_EXTRACT(col0, '"position":\s*\{[^}]*"abbreviation":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"positionAbbr":\s*"([^"]+)"', 1)
        ) as position_abbr,
        -- Try multiple patterns for age
        COALESCE(
            TRY(CAST(REGEXP_EXTRACT(col0, '"age":\s*(\d+)', 1) AS INTEGER)),
            TRY(CAST(REGEXP_EXTRACT(col0, '"age":\s*"(\d+)"', 1) AS INTEGER))
        ) as age,
        -- Try multiple patterns for birth date
        COALESCE(
            REGEXP_EXTRACT(col0, '"dateOfBirth":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"birthDate":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"dob":\s*"([^"]+)"', 1)
        ) as birth_date,
        -- Try multiple patterns for birth place
        COALESCE(
            REGEXP_EXTRACT(col0, '"birthPlace":\s*\{[^}]*"displayText":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"birthPlace":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"placeOfBirth":\s*"([^"]+)"', 1)
        ) as birth_place,
        -- Try multiple patterns for nationality
        COALESCE(
            REGEXP_EXTRACT(col0, '"citizenship":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"nationality":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"country":\s*"([^"]+)"', 1)
        ) as nationality,
        -- Try multiple patterns for height
        COALESCE(
            REGEXP_EXTRACT(col0, '"displayHeight":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"height":\s*"([^"]+)"', 1)
        ) as height,
        -- Try multiple patterns for weight
        COALESCE(
            REGEXP_EXTRACT(col0, '"displayWeight":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"weight":\s*"([^"]+)"', 1)
        ) as weight,
        -- Try multiple patterns for headshot
        COALESCE(
            REGEXP_EXTRACT(col0, '"headshot":\s*\{[^}]*"href":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"headshot":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"photo":\s*"([^"]+)"', 1),
            REGEXP_EXTRACT(col0, '"image":\s*"([^"]+)"', 1)
        ) as headshot_url,
        year,
        col0
    FROM rosters_raw
    WHERE col0 LIKE '%"id":%' 
       OR col0 LIKE '%"playerId":%'
       OR col0 LIKE '%"displayName":%'
       OR col0 LIKE '%"fullName":%'
       OR col0 LIKE '%"firstName":%'
),
-- Group by player_id and reconstruct complete records
real_players AS (
    SELECT 
        CAST(player_id AS VARCHAR) as player_id,
        CAST(MAX(player_name) AS VARCHAR) as player_name,
        CAST(MAX(first_name) AS VARCHAR) as first_name,
        CAST(MAX(last_name) AS VARCHAR) as last_name,
        CAST(MAX(jersey_number) AS VARCHAR) as jersey_number,
        CAST(MAX(position) AS VARCHAR) as position,
        CAST(MAX(position_abbr) AS VARCHAR) as position_abbr,
        CAST(MAX(age) AS INTEGER) as age,
        CAST(MAX(birth_date) AS VARCHAR) as birth_date,
        CAST(MAX(birth_place) AS VARCHAR) as birth_place,
        CAST(MAX(nationality) AS VARCHAR) as nationality,
        CAST(MAX(height) AS VARCHAR) as height,
        CAST(MAX(weight) AS VARCHAR) as weight,
        CAST(MAX(headshot_url) AS VARCHAR) as headshot_url,
        CAST(NULL AS VARCHAR) as team_id,  -- Team ID not available from fragmented data
        CAST(MAX(year) AS INTEGER) as season_year,
        CAST('2025-01-01 00:00:00.000' AS TIMESTAMP(3)) as created_at
    FROM players_extracted
    WHERE player_id IS NOT NULL
      AND player_id != ''
      AND LENGTH(player_id) > 0
      AND (player_name IS NOT NULL OR first_name IS NOT NULL OR last_name IS NOT NULL)
    GROUP BY player_id
),
-- Count real players found
real_players_count AS (
    SELECT COUNT(*) as count_real_players FROM real_players
)
-- Return real data if available, otherwise fallback to static data
SELECT 
    CAST(player_id AS VARCHAR) as player_id,
    CAST(player_name AS VARCHAR) as player_name,
    CAST(first_name AS VARCHAR) as first_name,
    CAST(last_name AS VARCHAR) as last_name,
    CAST(jersey_number AS VARCHAR) as jersey_number,
    CAST(position AS VARCHAR) as position,
    CAST(position_abbr AS VARCHAR) as position_abbr,
    CAST(age AS INTEGER) as age,
    CAST(birth_date AS VARCHAR) as birth_date,
    CAST(birth_place AS VARCHAR) as birth_place,
    CAST(nationality AS VARCHAR) as nationality,
    CAST(height AS VARCHAR) as height,
    CAST(weight AS VARCHAR) as weight,
    CAST(headshot_url AS VARCHAR) as headshot_url,
    CAST(team_id AS VARCHAR) as team_id,
    CAST(season_year AS INTEGER) as season_year,
    CAST(created_at AS TIMESTAMP(3)) as created_at
FROM real_players
WHERE player_id IS NOT NULL
  AND player_name IS NOT NULL;