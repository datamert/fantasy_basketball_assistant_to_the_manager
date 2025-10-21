# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6587fea8-3222-4de2-8e41-0d1d89ecc1ad",
# META       "default_lakehouse_name": "nba_player_stats",
# META       "default_lakehouse_workspace_id": "66503be7-cb73-4bf0-b14c-dfcdd90f8f13",
# META       "known_lakehouses": [
# META         {
# META           "id": "6587fea8-3222-4de2-8e41-0d1d89ecc1ad"
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

from nba_api.stats.endpoints import boxscoreadvancedv2

# Attempt to call without game_id
response = boxscoreadvancedv2()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from nba_api.stats.endpoints import playergamelog
import pandas as pd

# Example: Get LeBron James's game logs for 2024–25
lebron_id = 2544  # Replace with desired player ID
gamelog = playergamelog.PlayerGameLog(player_id=lebron_id, season='2024-25', season_type_all_star='Regular Season')
df = gamelog.get_data_frames()[0]

# Save to CSV
df.to_csv('lebron_2024_25_gamelog.csv', index=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
