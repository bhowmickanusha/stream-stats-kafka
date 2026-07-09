"""
Kafka consumer for YouTube view-count events.

Reads {video_id, title, view_count, timestamp} events from Kafka
and stores them in a local SQLite database.
"""

import json
import sqlite3

from kafka import KafkaConsumer

# ---- Configuration ----------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "yt-view-counts"
DB_PATH = "yt_stats.db"

# ---- Setup --------------------------------------------------------------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS view_counts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            title TEXT,
            view_count INTEGER NOT NULL,
            timestamp TEXT NOT NULL
        )
    """)
    # Speeds up "give me the latest row per video" queries
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_video_timestamp
        ON view_counts (video_id, timestamp DESC)
    """)
    conn.commit()
    return conn


def run():
    conn = init_db()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",   # only new messages from now on
        enable_auto_commit=True,
        group_id="yt-stats-consumer",
    )

    print("Consumer started, waiting for messages...")
    for message in consumer:
        entry = message.value
        conn.execute(
            "INSERT INTO view_counts (video_id, title, view_count, timestamp) VALUES (?, ?, ?, ?)",
            (entry["video_id"], entry["title"], entry["view_count"], entry["timestamp"]),
        )
        conn.commit()
        print(f"Stored: {entry['title'][:40]!r} -> {entry['view_count']} views")


if __name__ == "__main__":
    run()
