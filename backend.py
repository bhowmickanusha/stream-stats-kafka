"""
FastAPI backend serving YouTube view-count stats from SQLite.

Endpoints:
  GET /stats/latest        -> latest view count for every tracked video
  GET /stats/{video_id}    -> latest view count + recent history for one video
"""

import sqlite3

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

DB_PATH = "yt_stats.db"

app = FastAPI(title="YouTube Live Stats API")

# Allow the frontend (running on a different port/origin) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten this to your actual frontend URL before going public
    allow_methods=["GET"],
    allow_headers=["*"],
)


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/stats/latest")
def latest_stats():
    conn = get_connection()
    rows = conn.execute("""
        SELECT video_id, title, view_count, timestamp
        FROM view_counts
        WHERE id IN (
            SELECT MAX(id) FROM view_counts GROUP BY video_id
        )
        ORDER BY timestamp DESC
    """).fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/stats/{video_id}")
def video_stats(video_id: str, limit: int = 50):
    conn = get_connection()
    rows = conn.execute("""
        SELECT video_id, title, view_count, timestamp
        FROM view_counts
        WHERE video_id = ?
        ORDER BY timestamp DESC
        LIMIT ?
    """, (video_id, limit)).fetchall()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="No data found for this video_id")

    return {
        "video_id": video_id,
        "latest": dict(rows[0]),
        "history": [dict(row) for row in reversed(rows)],
    }
