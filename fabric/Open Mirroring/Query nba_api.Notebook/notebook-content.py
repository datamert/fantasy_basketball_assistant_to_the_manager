# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3360992c-6f78-4136-83fb-402828ede5d7",
# META       "default_lakehouse_name": "NBA_Data",
# META       "default_lakehouse_workspace_id": "66503be7-cb73-4bf0-b14c-dfcdd90f8f13",
# META       "known_lakehouses": [
# META         {
# META           "id": "3360992c-6f78-4136-83fb-402828ede5d7"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "617845d4-22c4-868e-4ee6-683810ba2331",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Import libraries
import pandas as pd
from nba_api.stats.static import players
from nba_api.stats.static import teams
from nba_api.stats.endpoints import playergamelogs
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Create a dimension tables for players and teams using static stats from nba_api

# CELL ********************

# Fetch player list
player_list = players.get_players()
df = pd.DataFrame(player_list)

# Convert to Spark DataFrame (schema inferred)
spark = SparkSession.builder.getOrCreate()
spark_df = spark.createDataFrame(df)

# Write to Lakehouse
spark_df.write.mode("overwrite").saveAsTable("dim_nba_players_static")

print("✅ Table 'dim_nba_players_static' written to Lakehouse.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch team list
team_list = teams.get_teams()
df = pd.DataFrame(team_list)

# Convert to Spark DataFrame (schema inferred)
spark = SparkSession.builder.getOrCreate()
spark_df = spark.createDataFrame(df)

# Write to Lakehouse
spark_df.write.mode("overwrite").saveAsTable("dim_nba_teams_static")

print("✅ Table 'dim_nba_teams_static' written to Lakehouse.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Attempt to query an endpoint from nba_api to show the error returned

# CELL ********************

# Attempt to get Lebron James' gamelogs from last season as an example
gamelogs = playergamelogs.PlayerGameLogs(player_id_nullable=2544, season_nullable="2024-25", season_type_nullable="Regular Season")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Any attempt to query the endpoints of nba_api via Fabric notebooks returns the error "ReadTimeout: HTTPSConnectionPool(host='stats.nba.com', port=443): Read timed out. (read timeout=30)". This is known error with nba_api that blocks queries coming from corporate IPs like Fabric in order to limit its service to personal use or paid accounts that can whitelist their IPs. On the other hand, Google IPs are cleared by nba_api. Therefore, I am running Google Jobs to query nba_api and landing its data into the landing zone of the Open Mirroring database.
