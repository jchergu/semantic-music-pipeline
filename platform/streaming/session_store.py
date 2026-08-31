"""
Postgres session/event durability for platform stage 10.

The Postgres half of the "Kafka consumer that reads behavioral events and
maintains session state in Redis" from CLAUDE.md's Decision B. Stage 9
gave that consumer its Redis half (streaming/session_state.py); this
module adds durable storage on top of the same consumer
(session_consumer.py), completing the stages-9-10 span Decision B
describes.
"""
import json
from pathlib import Path

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


def apply_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_PATH.read_text())
    conn.commit()


def persist_event(conn, session_id: str, event: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sessions (session_id) VALUES (%s)
            ON CONFLICT (session_id) DO UPDATE SET last_seen_at = now()
            """,
            (session_id,),
        )
        cur.execute(
            "INSERT INTO events (session_id, event_type, payload) VALUES (%s, %s, %s)",
            (session_id, event.get("event_type"), json.dumps(event)),
        )
    conn.commit()


def get_events(conn, session_id: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("SELECT payload FROM events WHERE session_id = %s ORDER BY id", (session_id,))
        return [row[0] for row in cur.fetchall()]
