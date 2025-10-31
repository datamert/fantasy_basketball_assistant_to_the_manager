# main.py — job entrypoint
import os, sys
print("CONTAINER STARTUP pid", os.getpid(), "cwd", os.getcwd(), flush=True)
from azure.eventhub import EventHubProducerClient, EventData
import feedparser

# Config from env
FABRIC_EVENSTREAM_CONN_STR = os.environ["FABRIC_EVENSTREAM_CONN_STR_2"]
FABRIC_EVENSTREAM_NAME = os.environ["FABRIC_EVENSTREAM_NAME_2"]
ARTICLE_LIMIT = int(os.environ.get("ARTICLE_LIMIT", "15"))

# Create producer client
producer = EventHubProducerClient.from_connection_string(
    conn_str=FABRIC_EVENSTREAM_CONN_STR,
    eventhub_name=FABRIC_EVENSTREAM_NAME
)

def fetch_espn_nba_rss(limit):
    feed_url = "https://www.espn.com/espn/rss/nba/news"
    feed = feedparser.parse(feed_url)

    articles = []
    for entry in feed.entries[:limit]:
        articles.append({
            "title": entry.title,
            "url": entry.link,
            "source": "espn"
        })

    return articles

def run():
    # Step 1: Query news articles
    articles = fetch_espn_nba_rss(ARTICLE_LIMIT)

    # Step 2: Create batch and add each event
    event_data_batch = producer.create_batch()
    for article in articles:
        event_data_batch.add(EventData(str(article)))

    # Step 3: Send batch
    producer.send_batch(event_data_batch)
    print("✅ Article batch sent to Fabric Eventstream")

if __name__ == "__main__":
    raise SystemExit(run())
