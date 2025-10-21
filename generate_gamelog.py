from nba_api.stats.endpoints import playergamelog
import pandas as pd

lebron_id = 2544
gamelog = playergamelog.PlayerGameLog(player_id=lebron_id, season='2024-25', season_type_all_star='Regular Season')
df = gamelog.get_data_frames()[0]
df.to_csv('lebron_2024_25_gamelog.csv', index=False)
