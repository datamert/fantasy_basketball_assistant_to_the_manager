# main.py — job entrypoint
import os, sys
print("CONTAINER STARTUP pid", os.getpid(), "cwd", os.getcwd(), flush=True)
from azure.eventhub import EventHubProducerClient, EventData
import requests

# Config from env
FABRIC_EVENSTREAM_CONN_STR = os.environ["FABRIC_EVENSTREAM_CONN_STR_2"]
FABRIC_EVENSTREAM_NAME = os.environ["FABRIC_EVENSTREAM_NAME_2"]

# Create producer client
producer = EventHubProducerClient.from_connection_string(
    conn_str=FABRIC_EVENSTREAM_CONN_STR,
    eventhub_name=FABRIC_EVENSTREAM_NAME
)

def get_new_espn_articles():
    url = "https://nba-stories.onrender.com/articles?source=espn"
    response = requests.get(url)
    response.raise_for_status()
    articles = response.json()
    return articles

def run():
    # Step 1: Query news articles
    articles = get_new_espn_articles()

    # Step 2: Create batch and add each event
    event_data_batch = producer.create_batch()
    for article in articles:
        event_data_batch.add(EventData(str(article)))

    # Step 3: Send batch
    producer.send_batch(event_data_batch)
    print("✅ Article batch sent to Fabric Eventstream")

if __name__ == "__main__":
    raise SystemExit(run())