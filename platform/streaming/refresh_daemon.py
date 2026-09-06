"""
Stage 16: persistent daemon for the DERIVED recommendation refresh.

Wraps stage 14's recommendation_refresh.process_one_event() in a
continuous loop under the canonical RECS_REFRESH_GROUP_ID, so
`session:{id}:recs` is kept current for every live session rather than
only for as long as some test or harness happens to be driving the
function by hand. Decision E names this module as the thing that runs the
refresh continuously, and platform/session_api/ as the thing that serves
what it writes — this one computes, that one delivers, and neither does
the other's job.

This closes the second half of the persistent-consumer gap stage 12 first
flagged and stage 15C flagged again. `eval/8_2`'s harness owns a
deliberately test-shaped version of this loop; that stays where it is (it
measures things a daemon has no business knowing about), and this is the
real one.

Both daemons are needed for the pipeline to produce anything: this one
reads `session:{id}:events` to decide what to refresh, and only
session_consumer_daemon.py writes that key.

Run (from the repo root; needs the Semantic API up — both refresh paths
call context_builder over HTTP):
    PYTHONPATH=platform platform/enrichment/.venv/bin/python \\
        -m streaming.refresh_daemon --semantic-api-url http://127.0.0.1:8000
"""
from __future__ import annotations

import argparse
import logging
import threading

import httpx
import redis

from streaming import recommendation_refresh
from streaming.config import RECS_REFRESH_GROUP_ID, REDIS_HOST, REDIS_PORT
from streaming.daemon_runtime import configure_logging, install_signal_handlers

log = logging.getLogger("refresh_daemon")

DEFAULT_SEMANTIC_API_URL = "http://127.0.0.1:8000"
DEFAULT_POLL_TIMEOUT_SECONDS = 5.0
DEFAULT_HTTP_TIMEOUT_SECONDS = 30.0


def run(
    semantic_api_url: str = DEFAULT_SEMANTIC_API_URL,
    group_id: str = RECS_REFRESH_GROUP_ID,
    poll_timeout: float = DEFAULT_POLL_TIMEOUT_SECONDS,
    stop: threading.Event | None = None,
    max_events: int | None = None,
    http_timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    expected_session_id: str | None = None,
) -> dict:
    """Applies the debounced refresh decision to every behavioral event
    until `stop` is set or `max_events` have been processed. Returns a
    small stats dict counting what the loop actually did.

    `stop` and `max_events` exist so a test can drive this exact loop from
    a worker thread with a bounded end, without installing signal handlers
    (only permitted on the main thread). `expected_session_id` is the same
    isolation escape hatch process_one_event() already carries and for the
    same reason: `behavioral-events` is never purged, so a fresh consumer
    group starting from "earliest" would otherwise drain years of other
    runs' events before reaching a test's own. A real deployment leaves it
    None -- serving every live session is the whole job.
    """
    stop = stop or threading.Event()
    redis_client = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)
    pg_conn = recommendation_refresh.connect_postgres()
    milvus_collection = recommendation_refresh.connect_milvus()
    # One Consumer for the daemon's whole life, reused across every
    # process_one_event() call. A fresh one per call would rescan from
    # "earliest" and hand back the same first message forever -- the bug
    # stage 14 found and fixed in its own tests, and the one stage 12 fixed
    # for the raw-state consumer before that.
    consumer = recommendation_refresh.new_consumer(group_id)
    # `refreshed`, `debounced` and `skipped` partition `processed`; the
    # per-action counters below them (cold_start / warm / skip_*) break
    # those down and are added as the actions actually occur, so a new
    # action in recommendation_refresh.py can't silently land in the wrong
    # bucket the way skip_unseedable_cold_start would have.
    stats = {"processed": 0, "refreshed": 0, "debounced": 0, "skipped": 0, "errors": 0,
             "cold_start": 0, "warm": 0, "skip_insufficient_data": 0}

    log.info("started group_id=%s semantic_api=%s poll_timeout=%.1fs",
             group_id, semantic_api_url, poll_timeout)
    try:
        with httpx.Client(base_url=semantic_api_url, timeout=http_timeout) as http_client:
            while not stop.is_set() and (max_events is None or stats["processed"] < max_events):
                try:
                    result = recommendation_refresh.process_one_event(
                        group_id, redis_client, pg_conn, milvus_collection, http_client,
                        timeout=poll_timeout, consumer=consumer,
                        expected_session_id=expected_session_id,
                    )
                except Exception:  # noqa: BLE001
                    # One unprocessable event must not take the daemon down.
                    # The offset is committed anyway: the consumer has
                    # already moved past the message in memory, and leaving
                    # it uncommitted would make a poison message replay
                    # forever on every restart, blocking the partition. The
                    # trade is that a genuinely transient failure loses that
                    # one refresh -- acceptable, because the next event for
                    # the same session recomputes it from scratch (recs are
                    # overwritten, never accumulated).
                    stats["errors"] += 1
                    log.exception("failed to process an event; skipping it")
                    consumer.commit(asynchronous=False)
                    continue
                if result is None:
                    continue  # idle poll, not an error -- just no traffic
                # At-least-once, same trade as session_consumer_daemon: the
                # commit follows the write to session:{id}:recs, so a crash
                # in between reprocesses the event rather than losing it. A
                # reprocessed refresh is harmless -- refresh_recommendations
                # overwrites the key rather than appending to it.
                consumer.commit(asynchronous=False)
                stats["processed"] += 1
                action = result.get("action")
                if result.get("refreshed"):
                    stats["refreshed"] += 1
                    stats[action] = stats.get(action, 0) + 1
                    log.info("refreshed session=%s action=%s count=%s in %.3fs",
                             result["session_id"], action, result.get("count"),
                             result.get("refresh_compute_seconds", float("nan")))
                elif action is not None and action.startswith("skip_"):
                    # Not errors: a session's first event legitimately has
                    # nothing to recommend from yet, and an opening track
                    # that isn't a catalog id can't seed a cold start.
                    # Persistent skip_insufficient_data across many sessions
                    # would instead mean session_consumer_daemon.py is not
                    # running.
                    stats["skipped"] += 1
                    stats[action] = stats.get(action, 0) + 1
                    log.debug("session=%s skipped action=%s", result["session_id"], action)
                else:
                    # No action key at all: the debounce declined before
                    # refresh_recommendations() was ever called.
                    stats["debounced"] += 1
                    log.debug("session=%s debounced (events_since_refresh=%s)",
                              result["session_id"], result.get("events_since_refresh"))
    finally:
        consumer.close()
        pg_conn.close()
        recommendation_refresh.disconnect_milvus()
        log.info("stopped after %d event(s), %d refresh(es)", stats["processed"], stats["refreshed"])

    return stats


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--semantic-api-url", default=DEFAULT_SEMANTIC_API_URL,
                        help="Both refresh paths call context_builder against this.")
    parser.add_argument("--group-id", default=RECS_REFRESH_GROUP_ID,
                        help="Kafka consumer group (default: the canonical one from config.py).")
    parser.add_argument("--poll-timeout", type=float, default=DEFAULT_POLL_TIMEOUT_SECONDS)
    parser.add_argument("--max-events", type=int, default=None,
                        help="Stop after this many events (default: run until signalled).")
    args = parser.parse_args(argv)

    configure_logging()
    stop = threading.Event()
    install_signal_handlers(stop, log)
    run(semantic_api_url=args.semantic_api_url, group_id=args.group_id,
        poll_timeout=args.poll_timeout, stop=stop, max_events=args.max_events)


if __name__ == "__main__":
    main()
