# main.py — job entrypoint
import os, sys
print("CONTAINER STARTUP pid", os.getpid(), "cwd", os.getcwd(), flush=True)
from azure.eventhub import EventHubProducerClient, EventData
from datetime import datetime, timezone, timedelta
from nbainjuries import injury
import json

# Config from env
FABRIC_EVENSTREAM_CONN_STR = os.environ["FABRIC_EVENSTREAM_CONN_STR"]
FABRIC_EVENSTREAM_NAME = os.environ["FABRIC_EVENSTREAM_NAME"]

# Create producer client
producer = EventHubProducerClient.from_connection_string(
    conn_str=FABRIC_EVENSTREAM_CONN_STR,
    eventhub_name=FABRIC_EVENSTREAM_NAME
)

def get_gmt5_hour_floor():
    # Get current UTC time
    utc_now = datetime.now(timezone.utc)

    # Convert to GMT-5
    gmt_minus_5 = timezone(timedelta(hours=-5))
    gmt5_time = utc_now.astimezone(gmt_minus_5)

    # Strip timezone info and zero out minute/second/microsecond
    gmt5_hour_floor = gmt5_time.replace(tzinfo=None, minute=0, second=0, microsecond=0)

    return gmt5_hour_floor

def get_parsed_injury_report(datetimestamp):
    # Step 1: Query NBA injury events
    raw = injury.get_reportdata(datetimestamp)

    # Step 2: Unescape the string
    cleaned = raw.encode().decode('unicode_escape')

    # Step 3: Parse as JSON
    parsed = json.loads(cleaned)

    return parsed

def run():
    # Step 1: Query and parse injury report
    injury_report = get_parsed_injury_report(get_gmt5_hour_floor())

    # Step 2: Create batch and add each event
    event_data_batch = producer.create_batch()
    for injury_event in injury_report:
        event_data_batch.add(EventData(str(injury_event)))

    # Step 3: Send batch
    producer.send_batch(event_data_batch)
    print("✅ Injury batch sent to Fabric Eventstream")

if __name__ == "__main__":
    raise SystemExit(run())