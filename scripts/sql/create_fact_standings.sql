-- DEFINITIVE FIX: Calculate real standings from real matches data
-- Note: External table real_matches_for_standings must exist before running this

CREATE TABLE ucl_analytics_db.fact_standings
WITH (
    format = 'PARQUET',
    external_location = 's3://ucl-lake-2025/processed/fact_standings/'
) AS
WITH completed_matches AS (
    SELECT 
        match_id,
        match_date,
        CASE WHEN completed = 'True' THEN true ELSE false END as completed,
        home_team_id,
        TRY(CAST(home_score AS INTEGER)) as home_score,
        away_team_id,
        TRY(CAST(away_score AS INTEGER)) as away_score,
        TRY(CAST(season_year AS INTEGER)) as season_year
    FROM ucl_analytics_db.real_matches_for_standings
    WHERE completed = 'True'
      AND home_score IS NOT NULL
      AND away_score IS NOT NULL
      AND TRY(CAST(home_score AS INTEGER)) IS NOT NULL
      AND TRY(CAST(away_score AS INTEGER)) IS NOT NULL
),
-- Get all teams from matches
all_teams AS (
    SELECT DISTINCT home_team_id as team_id, season_year FROM completed_matches
    UNION
    SELECT DISTINCT away_team_id as team_id, season_year FROM completed_matches
),
-- Calculate team statistics
team_stats AS (
    SELECT 
        at.season_year,
        at.team_id,
        -- Games played
        COUNT(cm.match_id) as games_played,
        -- Wins, draws, losses
        SUM(CASE 
            WHEN (cm.home_team_id = at.team_id AND cm.home_score > cm.away_score) OR
                 (cm.away_team_id = at.team_id AND cm.away_score > cm.home_score) 
            THEN 1 ELSE 0 END) as wins,
        SUM(CASE 
            WHEN cm.home_score = cm.away_score THEN 1 ELSE 0 
        END) as draws,
        SUM(CASE 
            WHEN (cm.home_team_id = at.team_id AND cm.home_score < cm.away_score) OR
                 (cm.away_team_id = at.team_id AND cm.away_score < cm.home_score) 
            THEN 1 ELSE 0 END) as losses,
        -- Goals for and against
        SUM(CASE 
            WHEN cm.home_team_id = at.team_id THEN cm.home_score
            WHEN cm.away_team_id = at.team_id THEN cm.away_score
            ELSE 0 END) as goals_for,
        SUM(CASE 
            WHEN cm.home_team_id = at.team_id THEN cm.away_score
            WHEN cm.away_team_id = at.team_id THEN cm.home_score
            ELSE 0 END) as goals_against,
        -- Points (3 for win, 1 for draw)
        SUM(CASE 
            WHEN (cm.home_team_id = at.team_id AND cm.home_score > cm.away_score) OR
                 (cm.away_team_id = at.team_id AND cm.away_score > cm.home_score) 
            THEN 3
            WHEN cm.home_score = cm.away_score THEN 1
            ELSE 0 END) as points
    FROM all_teams at
    LEFT JOIN completed_matches cm ON 
        (cm.home_team_id = at.team_id OR cm.away_team_id = at.team_id)
        AND cm.season_year = at.season_year
    GROUP BY at.season_year, at.team_id
),
-- Add team names and calculate final standings
final_standings AS (
    SELECT 
        ts.team_id,
        COALESCE(dt.team_name, 'Team ' || ts.team_id) as team_name,
        COALESCE(dt.team_abbr, 'T' || ts.team_id) as team_abbrev,
        'Champions League' as group_name,
        ROW_NUMBER() OVER (
            PARTITION BY ts.season_year 
            ORDER BY ts.points DESC, (ts.goals_for - ts.goals_against) DESC, ts.goals_for DESC
        ) as position,
        ts.points,
        ts.games_played,
        ts.wins,
        ts.draws,
        ts.losses,
        ts.goals_for,
        ts.goals_against,
        (ts.goals_for - ts.goals_against) as goal_difference,
        ts.season_year
    FROM team_stats ts
    LEFT JOIN ucl_analytics_db.dim_teams dt ON ts.team_id = dt.team_id AND ts.season_year = dt.season_year
    WHERE ts.games_played > 0
)
SELECT 
    CAST(team_id AS VARCHAR) as team_id,
    CAST(team_name AS VARCHAR) as team_name,
    CAST(team_abbrev AS VARCHAR) as team_abbrev,
    CAST(group_name AS VARCHAR) as group_name,
    CAST(position AS INTEGER) as position,
    CAST(points AS INTEGER) as points,
    CAST(games_played AS INTEGER) as games_played,
    CAST(wins AS INTEGER) as wins,
    CAST(draws AS INTEGER) as draws,
    CAST(losses AS INTEGER) as losses,
    CAST(goals_for AS INTEGER) as goals_for,
    CAST(goals_against AS INTEGER) as goals_against,
    CAST(goal_difference AS INTEGER) as goal_difference,
    CAST(season_year AS INTEGER) as season_year
FROM final_standings
WHERE team_id IS NOT NULL
  AND team_id != '';
