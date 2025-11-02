-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "3360992c-6f78-4136-83fb-402828ede5d7",
-- META       "default_lakehouse_name": "NBA_Data",
-- META       "default_lakehouse_workspace_id": "66503be7-cb73-4bf0-b14c-dfcdd90f8f13",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "3360992c-6f78-4136-83fb-402828ede5d7"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- # Create materialized lake views 
-- 1. Use this notebook to create materialized lake views. 
-- 2. Select **Run all** to run the notebook. 
-- 3. When the notebook run is completed, return to your lakehouse and refresh your materialized lake views graph. 


-- CELL ********************

CREATE MATERIALIZED LAKE VIEW injury.nba_injured_players_latest
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT
    PlayerName,
    Team,
    CurrentStatus,
    Reason
FROM (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY PlayerName ORDER BY EventEnqueuedUtcTime DESC) AS rn
    FROM injury.nba_injury_reports_raw
    WHERE EventEnqueuedUtcTime > current_timestamp() - INTERVAL '7' DAY
) latest
WHERE rn = 1
    AND CurrentStatus != 'Available'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW semantic.dim_team
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT
    id AS `ID`,
    full_name AS `Name`,
    'https://cdn.nba.com/logos/nba/' || id || '/global/L/logo.svg' AS `Team Logo URL`,
    abbreviation AS `Abbreviation`,
    nickname AS `Nickname`,
    city AS `City`,
    state AS `State`,
    year_founded AS `Year Founded`
FROM dbo.dim_nba_teams_static

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW semantic.dim_player
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
WITH injury_status AS (
    SELECT
        (TRIM(SPLIT_PART(PlayerName, ',', 2)) || ' ' || TRIM(SPLIT_PART(PlayerName, ',', 1))) AS FullName,
        CurrentStatus,
        Reason
    FROM injury.nba_injured_players_latest
)
SELECT
    pla.full_name as Name,
    pla.id as ID,
    'https://cdn.nba.com/headshots/nba/latest/260x190/' || pla.id || '.png' AS `Player Headshot URL`,
    pla.first_name AS `First Name`,
    pla.last_name AS `Last Name`,
    inj.CurrentStatus AS `Current Injury Status`,
    inj.Reason AS `Current Injury`
FROM dbo.dim_nba_players_static AS pla
LEFT OUTER JOIN injury_status AS inj
    ON pla.full_name = inj.FullName
WHERE pla.is_active = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW semantic.fct_gamelogs
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT
    PLAYER_ID AS `Player ID`,
    TEAM_ID AS `Team ID`,
    GAME_ID AS `Game ID`,
    CAST(GAME_DATE AS DATE) AS `Game Date`,
    MATCHUP AS `Matchup`,
    WL,
    MIN,
    FGM,
    FGA,
    FG_PCT,
    FG3M,
    FG3A,
    FG3_PCT,
    FTM,
    FTA,
    FT_PCT,
    OREB,
    DREB,
    REB,
    AST,
    TOV,
    STL,
    BLK,
    BLKA,
    PF,
    PFD,
    PTS,
    PLUS_MINUS,
    DD2,
    TD3
FROM nba_api.playergamelogs

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW semantic.dim_game_date
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT DISTINCT
    SEASON_YEAR AS `Season Year`,
    CAST(GAME_DATE AS DATE) AS `Game Date`
FROM nba_api.playergamelogs

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
