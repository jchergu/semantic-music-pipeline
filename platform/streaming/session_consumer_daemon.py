"""
Stage 16: persistent daemon for the RAW-state consumer.

Wraps the Decision B / stages 9-10 consumer
(session_consumer.consume_and_cache_many) in a continuous loop, under the
canonical SESSION_CONSUMER_GROUP_ID. It maintains `session:{id}:events` in
Redis and the durable Postgres `events` log — raw state, per Decision C.
It computes nothing derived: the session profile is stage 13's Flink job,
and recommendations are refresh_daemon.py's.

This closes half of the gap stage 12 first flagged ("Still no persistent
Kafka consumer daemon... arguably stage 9-10's own gap") and stage 15C
flagged again. It matters for more than tidiness:
refresh_recommendations() returns `skip_insufficient_data` below two
events in `session:{id}:events`, and nothing but this consumer ever writes
that key — so without this daemon running, refresh_daemon.py can run
forever and never produce a single recommendation. `eval/8_2`'s harness
papers over that by running both loops itself; a real deployment needs
both processes.

Run (from the repo root, alongside refresh_daemon.py):
    PYTHONPATH=platform platform/enrichment/.venv/bin/python \\
        -m streaming.session_consumer_daemon
"""
from __future__ import annotations

import argparse
import logging
import threading

import psycopg2

from streaming import session_consumer
from streaming.config import SESSION_CONSUMER_GROUP_ID, PG_DSN
from streaming.daemon_runtime import configure_logging, install_signal_handlers
from streaming.session_store import apply_schema

log = logging.getLogger("session_consumer_daemon")

# One event per call, so the offset commit below is per fully-processed
# event. Batching would be faster and is pointless here -- this consumer's
# work is two small writes, and at-least-once semantics are easier to
# reason about one message at a time.
BATCH_SIZE = 1
DEFAULT_POLL_TIMEOUT_SECONDS = 5.0


def run(
    group_id: str = SESSION_CONSUMER_GROUP_ID,
    poll_timeout: float = DEFAULT_POLL_TIMEOUT_SECONDS,
    stop: threading.Event | None = None,
    max_events: int | None = None,
    with_postgres: bool = True,
    expected_session_id: str | None = None,
) -> dict:
    """Consumes behavioral events into Redis (and Postgres) until `stop` is
    set or `max_events` have been handled. Returns a small stats dict.

    `stop` and `max_events` exist so a test can drive this exact loop --
    not a reimplementation of it -- from a worker thread with a bounded
    end, without installing signal handlers (which Python only permits on
    the main thread). `expected_session_id` is the same isolation escape
    hatch consume_and_cache_many() already carries and for the same reason:
    `behavioral-events` is never purged, so a fresh consumer group starting
    from "earliest" would otherwise drain every previous run's events
    before reaching a test's own. A real deployment leaves it None.
    """
    stop = stop or threading.Event()
    pg_conn = psycopg2.connect(PG_DSN) if with_postgres else None
    if pg_conn is not None:
        apply_schema(pg_conn)
    consumer = session_consumer.new_consumer(group_id)
    stats = {"events": 0, "sessions": set(), "errors": 0}

    log.info(
        "started group_id=%s postgres=%s poll_timeout=%.1fs",
        group_id, "on" if with_postgres else "off", poll_timeout,
    )
    try:
        while not stop.is_set() and (max_events is None or stats["events"] < max_events):
            try:
                events = session_consumer.consume_and_cache_many(
                    group_id, count=BATCH_SIZE, timeout=poll_timeout,
                    pg_conn=pg_conn, consumer=consumer,
                    expected_session_id=expected_session_id,
                )
            except Exception:  # noqa: BLE001
                # Same containment and same trade as refresh_daemon's: one
                # unprocessable event must not take the daemon down, and
                # committing past it stops a poison message replaying
                # forever on every restart.
                stats["errors"] += 1
                log.exception("failed to consume an event; skipping it")
                if pg_conn is not None:
                    # persist_event may have left the transaction aborted;
                    # every later INSERT on this connection would fail with
                    # "current transaction is aborted" until it is cleared.
                    pg_conn.rollback()
                consumer.commit(asynchronous=False)
                continue
            if not events:
                continue  # idle poll, not an error -- just no traffic
            # Committed only after the events are in Redis (and Postgres):
            # at-least-once. A crash between the write and this commit
            # replays those events on restart; a commit before the write
            # would lose them instead, which is the worse trade for a
            # durable session log. See daemon_runtime's module docstring
            # for why these daemons commit at all when nothing else does.
            consumer.commit(asynchronous=False)
            for event in events:
                stats["events"] += 1
                stats["sessions"].add(event["session_id"])
                log.info("cached session=%s type=%s track=%s",
                         event["session_id"], event.get("event_type"), event.get("track_id"))
    finally:
        consumer.close()
        if pg_conn is not None:
            pg_conn.close()
        log.info("stopped after %d event(s) across %d session(s), %d error(s)",
                 stats["events"], len(stats["sessions"]), stats["errors"])

    return {"events": stats["events"], "sessions": sorted(stats["sessions"]),
            "errors": stats["errors"]}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group-id", default=SESSION_CONSUMER_GROUP_ID,
                        help="Kafka consumer group (default: the canonical one from config.py).")
    parser.add_argument("--poll-timeout", type=float, default=DEFAULT_POLL_TIMEOUT_SECONDS)
    parser.add_argument("--max-events", type=int, default=None,
                        help="Stop after this many events (default: run until signalled).")
    parser.add_argument("--no-postgres", action="store_true",
                        help="Redis only, skipping the stage 10 durable log.")
    args = parser.parse_args(argv)

    configure_logging()
    stop = threading.Event()
    install_signal_handlers(stop, log)
    run(group_id=args.group_id, poll_timeout=args.poll_timeout, stop=stop,
        max_events=args.max_events, with_postgres=not args.no_postgres)


if __name__ == "__main__":
    main()
