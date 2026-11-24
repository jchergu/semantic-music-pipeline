"""
Seeds the `demo-rock-thread` session the 8.2/8.3 dashboards (demo/uc82.html,
demo/uc83.html) default their session-id field to, so `/sessions/demo-rock-thread`
resolves on session_api instead of 404ing with "unknown session" the first
time the demo stack is started fresh.

That 404 is session_api working correctly (platform/session_api/main.py's
`_missing()`/`get_session()`) -- it just means no behavioral events have ever
been produced for this session_id yet. This script produces some: `play` then
`complete` events, via the real event_ingestion HTTP path (stage 11), for the
same three rock tracks CLAUDE.md's "running example" already uses elsewhere
in this repo (#14 "Gates", #15 "Billy Comes Home", #16 "Give Me Hope" --
see platform/simulator/scripts/coherent_session.yaml).

`event_time` is required, not optional: flink_session_profile_job.py silently
drops any event missing it (not an error -- see CLAUDE.md's 2026-09-25
operational note). This script always sets it, so a run with the Flink job
already submitted also gets a real session profile, not just the cold-start
recommendation fallback.

Needs event_ingestion (port 8020 by default) reachable -- run_demo.sh calls
this after that service is healthy. Safe to re-run: it only appends events: to
demo-rock-thread's Kafka log; the session's 30-minute sliding raw-event TTL
means old runs age out on their own.
"""
import argparse
import datetime

import httpx

SESSION_ID = "demo-rock-thread"
# (track_id, title) -- rock, same running example used elsewhere in this repo.
TRACKS = [
    (14, "Gates"),
    (15, "Billy Comes Home"),
    (16, "Give Me Hope"),
]
TRACK_DURATION_S = 195


def seed(event_ingestion_url: str, session_id: str = SESSION_ID) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    with httpx.Client(timeout=10) as client:
        for i, (track_id, title) in enumerate(TRACKS):
            t0 = now + datetime.timedelta(seconds=i * (TRACK_DURATION_S + 5))
            for event_type, offset_s, position_ms in (
                ("play", 0, 0),
                ("complete", TRACK_DURATION_S, TRACK_DURATION_S * 1000),
            ):
                payload = {
                    "session_id": session_id,
                    "event_type": event_type,
                    "track_id": str(track_id),
                    "event_time": (t0 + datetime.timedelta(seconds=offset_s)).isoformat(),
                    "position_ms": position_ms,
                }
                resp = client.post(f"{event_ingestion_url}/events", json=payload)
                resp.raise_for_status()
                print(f"  seeded {event_type:8s} track={track_id} ({title!r}) -> {resp.status_code}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-ingestion-url", default="http://127.0.0.1:8020")
    parser.add_argument("--session-id", default=SESSION_ID)
    args = parser.parse_args()

    print(f"Seeding session {args.session_id!r} via {args.event_ingestion_url} ...")
    seed(args.event_ingestion_url, args.session_id)
    print("Done. session_api's /sessions/{id} should now resolve once the "
          "session_consumer_daemon and refresh_daemon have caught up (a few seconds).")


if __name__ == "__main__":
    main()
