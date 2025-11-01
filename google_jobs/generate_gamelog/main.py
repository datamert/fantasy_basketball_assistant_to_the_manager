# main.py — job entrypoint
import os, sys
print("CONTAINER STARTUP pid", os.getpid(), "cwd", os.getcwd(), flush=True)

import time
import tempfile
from datetime import datetime
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from nba_api.stats.endpoints import playergamelogs
from openmirroring_operations import OpenMirroringClient

# Config from env
FABRIC_HOST = os.environ["FABRIC_HOST"]
FABRIC_CLIENT_ID = os.environ["FABRIC_CLIENT_ID"]
FABRIC_TENANT_ID = os.environ["FABRIC_TENANT_ID"]
FABRIC_CLIENT_SECRET = os.environ["FABRIC_CLIENT_SECRET"]
FABRIC_SCHEMA = os.environ.get("FABRIC_SCHEMA", "nba_api")
FABRIC_TABLE = os.environ.get("FABRIC_TABLE", "playergamelogs")
SEASON = os.environ.get("SEASON", "2025-26")
PLAYER_ID = os.environ.get("PLAYER_ID")
CREATE_TABLE = os.environ.get("CREATE_TABLE")

client = OpenMirroringClient(
    client_id=FABRIC_CLIENT_ID,
    client_secret=FABRIC_CLIENT_SECRET,
    client_tenant=FABRIC_TENANT_ID,
    host=FABRIC_HOST
)

def run():
    try:
        # Fetch playergamelogs
        if PLAYER_ID:
            gl = playergamelogs.PlayerGameLogs(player_id_nullable=PLAYER_ID, season_nullable=SEASON, season_type_nullable="Regular Season")
        else:
            gl = playergamelogs.PlayerGameLogs(season_nullable=SEASON, season_type_nullable="Regular Season")
        df = gl.get_data_frames()[0]

        # Clean up season
        df = df[df['SEASON_YEAR'] == SEASON]

        # Ingestion tracker
        df["INGESTED_AT"] = datetime.utcnow().isoformat()
        if df.empty:
            print({"status": "no_data", "message": f"No game logs found for season {SEASON}. Script will exit with failure."})
            return 1 # Indicate failure

        # Create table in Fabric
        if CREATE_TABLE:
            client.create_table(schema_name=FABRIC_SCHEMA, table_name=FABRIC_TABLE, key_cols=["PLAYER_ID", "GAME_ID"])
        
        # Enforce PLAYER_ID as long
        df["PLAYER_ID"] = pd.to_numeric(df["PLAYER_ID"], downcast="integer").astype("Int64")

        # Write parquet to Fabric
        with tempfile.TemporaryDirectory() as td:
            local_file = os.path.join(td, f"{FABRIC_TABLE}.parquet")
            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table, local_file, compression="snappy")
            client.upload_data_file(schema_name=FABRIC_SCHEMA, table_name=FABRIC_TABLE, local_file_path=local_file)
        print({"status": "ok", "season": SEASON, "rows": len(df)})
        return 0

    except Exception as exc:
        print({"status": "error", "error": str(exc)})
        return 1

if __name__ == "__main__":
    raise SystemExit(run())
