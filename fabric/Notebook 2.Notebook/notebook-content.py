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

pip show nba_api

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from nba_api.stats.endpoints import playercareerstats

# Anthony Davis
career = playercareerstats.PlayerCareerStats(player_id="203076")
career.get_data_frames()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from nba_api.stats.static import players

# get_players returns a list of dictionaries, each representing a player.
nba_players = players.get_players()
print("Number of players fetched: {}".format(len(nba_players)))
nba_players[:5]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from nba_api.stats.static import teams

# get_teams returns a list of 30 dictionaries, each an NBA team.
nba_teams = teams.get_teams()
print("Number of teams fetched: {}".format(len(nba_teams)))
nba_teams[:3]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from nba_api.stats.endpoints import playercareerstats

# Anthony Davis
career = playercareerstats.PlayerCareerStats(player_id="203076")
career.get_data_frames()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

url = "https://stats.nba.com/stats/commonallplayers"
params = {
    "LeagueID": "00",
    "Season": "2022-23",
    "IsOnlyCurrentSeason": "1"
}
headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.nba.com/"
}

response = requests.get(url, params=params, headers=headers, timeout=30)
data = response.json()
print(data['resultSets'][0]['headers'])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
