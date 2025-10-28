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
