"""
YouTube view-count producer.

Polls the YouTube Data API for a list of video IDs and publishes
{video_id, title, view_count, timestamp} events to a Kafka topic.
"""

import json
import time
from datetime import datetime, timezone

from googleapiclient.discovery import build
from kafka import KafkaProducer

# ---- Configuration ----------------------------------------------------

YOUTUBE_API_KEY = "AIzaSyANnKa0wQeFct4diBu_tvlm7nS0XDiLIfc"

# Add the video IDs you want to track (the part after "v=" in a YouTube URL)
VIDEO_IDS = [
    "0lapF4DQPKQ", "hmE9f-TEutc", "pBuZEGYXA6E"
]

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"  # host-side listener from the compose file
KAFKA_TOPIC = "yt-view-counts"

POLL_INTERVAL_SECONDS = 60

# ---- Setup --------------------------------------------------------------

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)


def fetch_stats(video_ids):
    """Fetch current statistics for a batch of video IDs in one API call."""
    response = youtube.videos().list(
        part="statistics,snippet",
        id=",".join(video_ids),
    ).execute()

    results = []
    for item in response.get("items", []):
        results.append({
            "video_id": item["id"],
            "title": item["snippet"]["title"],
            "view_count": int(item["statistics"].get("viewCount", 0)),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    return results


def run():
    print(f"Starting producer, polling every {POLL_INTERVAL_SECONDS}s...")
    while True:
        try:
            stats = fetch_stats(VIDEO_IDS)
            for entry in stats:
                producer.send(KAFKA_TOPIC, key=entry["video_id"], value=entry)
                print(f"Sent: {entry['title'][:40]!r} -> {entry['view_count']} views")
            producer.flush()
        except Exception as e:
            print(f"Error during poll: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
