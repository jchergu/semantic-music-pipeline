"""
8.3: `media-stream`'s first producer (thesis §6.2 commitment). Publishes a
"now playing" event directly onto the topic for each track in a session's
play sequence, at a fixed interval -- deliberately simpler than
platform/simulator/ (behavioral events: complete/skip/like, each needing
position_ms/duration bookkeeping for session_profile.py's weight table).
A "now playing" event only ever means "this track started", so it carries
just {session_id, track_id, event_time}.

Publishes straight onto Kafka via streaming.producer.produce() rather than
through an HTTP ingestion service -- media-stream and behavioral-events are
architecturally separate (CLAUDE.md's L1 Kafka topics bullet), and unlike
behavioral events (Decision A's ingestion service, with its own schema
validation), a media-stream "now playing" beacon has no such contract to
enforce yet.

Usage (from the repo root; needs the stack up and proactive_service.py
running to see anything consume it):
    PYTHONPATH=platform:usecases/8_3_streaming_proactive \
        platform/enrichment/.venv/bin/python \
        usecases/8_3_streaming_proactive/media_stream_producer.py \
        --session-id demo-session-1 --track-ids 14,15,16 --interval 3
"""
import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from streaming.config import TOPIC_MEDIA_STREAM  # noqa: E402
from streaming.producer import produce  # noqa: E402

# Same fixed-epoch reasoning as platform/simulator/cli.py's SIMULATION_EPOCH:
# reproducible event_time across separate invocations, not wall-clock now().
SIMULATION_EPOCH = dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc)


def publish_session(session_id: str, track_ids: list[int], interval_seconds: float) -> list[dict]:
    published = []
    for i, track_id in enumerate(track_ids):
        event_time = (SIMULATION_EPOCH + dt.timedelta(seconds=i * interval_seconds)).isoformat()
        event = {"session_id": session_id, "track_id": str(track_id), "event_time": event_time}
        produce(TOPIC_MEDIA_STREAM, json.dumps(event), key=session_id)
        published.append(event)
        print(f"published now_playing session={session_id} track_id={track_id}")
        if i < len(track_ids) - 1:
            time.sleep(interval_seconds)
    return published


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--track-ids", required=True, help="Comma-separated track ids, in play order")
    parser.add_argument("--interval", type=float, default=3.0, help="Seconds between now-playing events")
    args = parser.parse_args(argv)

    track_ids = [int(t) for t in args.track_ids.split(",")]
    publish_session(args.session_id, track_ids, args.interval)


if __name__ == "__main__":
    main()
