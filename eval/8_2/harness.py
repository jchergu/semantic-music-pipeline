"""
The active half of eval/8_2: drives one scenario against the live system
and records what happens while it happens.

eval/8_1 could be passive -- it queried a frozen table that already
existed. There is no frozen table here, only two independently-scheduled
consumers (stage 13's Flink windows and stage 14's refresh loop) racing on
one Kafka topic, so every number in this pack has to be captured in flight.

Per scenario, four things run at once:

  main thread        posts the session's events, paced by the simulated gap
                     divided by --speed, timing each POST (latency hop H1)
  poster-side check  immediately before the pivot's first event, asserts
                     session:{id}:profile exists (metric 1's warm-path
                     precondition)
  raw consumer       platform/streaming/session_consumer.consume_and_cache_many
                     -- the real stages 9-10 consumer, not a stand-in;
                     without it session:{id}:events stays empty and every
                     refresh returns skip_insufficient_data
  refresh driver     repeated process_one_event() over ONE reused Consumer
                     (see that function's docstring: offsets are never
                     committed, so a fresh Consumer per call re-reads the
                     same first message forever), snapshotting
                     session:{id}:recs after every real refresh
  profile poller     samples session:{id}:profile_meta so H2 (profile
                     compute lag) has a distribution rather than the two or
                     three points a refresh-time-only read would give

This loop is harness-owned, test-shaped code. It is deliberately NOT a step
toward a persistent refresh daemon in platform/streaming/ -- that gap is
real and stays open (stage 12 flagged the same thing for the raw-state
consumer).
"""
from __future__ import annotations

import datetime as dt
import json
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from simulator import events as sim_events  # noqa: E402
from streaming import session_consumer  # noqa: E402
from streaming.recommendation_refresh import new_consumer, process_one_event  # noqa: E402
from streaming.session_state import profile_key, profile_meta_key, recs_key  # noqa: E402

from . import scenario_gen, store_access  # noqa: E402

# Consumer groups join and scan the topic from "earliest" (the platform's
# own setting), and the behavioral-events topic is never purged. Both
# consumers therefore need a head start before the first event is posted,
# or the group-join and backlog scan would show up inside this scenario's
# measured arrival-to-refresh delays as if the system were slow.
CONSUMER_WARMUP_SECONDS = 6.0
PROFILE_POLL_INTERVAL_SECONDS = 0.5
SESSION_KEY_SUFFIXES = ("events", "profile", "profile_meta", "recs", "refresh_meta")


@dataclass
class ScenarioSpec:
    name: str
    session_id: str
    genre_track_counts: list[tuple[str, int]]
    seed: int
    speed: float
    pivot_track_index: int | None = None
    jitter: float = 0.0


@dataclass
class _Recording:
    """Everything the threads write, under one lock."""
    lock: threading.Lock = field(default_factory=threading.Lock)
    posts: list[dict] = field(default_factory=list)          # H1 + post wall-clocks
    refresh_results: list[dict] = field(default_factory=list)  # every process_one_event return
    snapshots: list[dict] = field(default_factory=list)      # recs after each real refresh
    profile_samples: list[dict] = field(default_factory=list)  # H2
    errors: list[str] = field(default_factory=list)


def reset_session(redis_client, session_id: str) -> None:
    """Deletes this session's own keys so a re-invocation can't inherit
    state from the last one. Only ever touches keys under session ids this
    harness generates."""
    redis_client.delete(*[f"session:{session_id}:{suffix}" for suffix in SESSION_KEY_SUFFIXES])


def _read_recs(redis_client, session_id: str) -> list[str]:
    raw = redis_client.get(recs_key(session_id))
    if raw is None:
        return []
    return [str(r["track_id"]) for r in json.loads(raw)]


def _poster(spec: ScenarioSpec, planned: list[dict], pivot_index: int | None,
            ingestion_url: str, redis_client, rec: _Recording, precondition: dict) -> None:
    """Runs on the main thread. Paces posts on the simulated clock, times
    each one, and checks the warm-path precondition at the pivot."""
    with httpx.Client(base_url=ingestion_url, timeout=10.0) as client:
        for i, item in enumerate(planned):
            gap = item["wall_clock_gap_seconds"]
            if gap > 0:
                time.sleep(gap / spec.speed)

            if pivot_index is not None and i == pivot_index:
                # METRICS.md section 2: a K measured while the refresh loop
                # is still on the cold-start fallback is measuring "did the
                # single-seed fallback change", not "did the centroid
                # adapt". Checked here, not assumed, and the run is excluded
                # rather than silently reported if it fails.
                has_profile = redis_client.exists(profile_key(spec.session_id)) == 1
                precondition["profile_exists_at_pivot"] = has_profile
                precondition["checked_at"] = time.time()
                print(f"  [{spec.name}] pivot reached; warm-path precondition: "
                      f"{'satisfied' if has_profile else 'NOT satisfied (run will be excluded)'}")

            started = time.time()
            resp = client.post("/events", json=item["event"])
            elapsed = time.time() - started
            resp.raise_for_status()
            with rec.lock:
                rec.posts.append({
                    "index": i,
                    "wall_clock": started,
                    "h1_post_seconds": elapsed,
                    "track_id": item["event"]["track_id"],
                    "event_type": item["event"]["event_type"],
                    "post_pivot": pivot_index is not None and i >= pivot_index,
                })


def _raw_consumer_thread(spec: ScenarioSpec, event_count: int, budget: float, rec: _Recording) -> None:
    """The real Decision B / stages 9-10 platform consumer, in a thread."""
    pg_conn = store_access.connect_postgres()  # own connection: psycopg2 connections aren't thread-safe
    try:
        session_consumer.consume_and_cache_many(
            group_id=f"eval82-raw-{uuid.uuid4()}",
            count=event_count,
            timeout=budget,
            expected_session_id=spec.session_id,
            pg_conn=pg_conn,
        )
    except Exception as exc:  # noqa: BLE001 - recorded, not swallowed
        with rec.lock:
            rec.errors.append(f"raw consumer: {exc!r}")
    finally:
        pg_conn.close()


def _refresh_driver_thread(spec: ScenarioSpec, event_count: int, deadline: float,
                           semantic_api_url: str, redis_client, milvus_collection,
                           rec: _Recording, stop: threading.Event) -> None:
    group_id = f"eval82-recs-{uuid.uuid4()}"
    consumer = new_consumer(group_id)
    pg_conn = store_access.connect_postgres()
    processed = 0
    refresh_index = 0
    try:
        with httpx.Client(base_url=semantic_api_url, timeout=30.0) as http_client:
            while processed < event_count and not stop.is_set() and time.monotonic() < deadline:
                result = process_one_event(
                    group_id, redis_client, pg_conn, milvus_collection, http_client,
                    timeout=2.0, expected_session_id=spec.session_id, consumer=consumer,
                )
                if result is None:
                    continue
                processed += 1
                now = time.time()
                # Events are consumed in produce order from a single-partition
                # topic, so the n-th result belongs to the n-th posted event.
                with rec.lock:
                    post = rec.posts[processed - 1] if processed - 1 < len(rec.posts) else None
                    entry = dict(result)
                    entry.update({
                        "event_index": processed - 1,
                        "wall_clock": now,
                        "post_wall_clock": post["wall_clock"] if post else None,
                        "arrival_to_refresh_seconds": (now - post["wall_clock"]) if post else None,
                        "post_pivot": post["post_pivot"] if post else None,
                    })
                    if result.get("refreshed"):
                        refresh_index += 1
                        entry["refresh_index"] = refresh_index
                    rec.refresh_results.append(entry)

                if result.get("refreshed"):
                    track_ids = _read_recs(redis_client, spec.session_id)
                    with rec.lock:
                        rec.snapshots.append({
                            "refresh_index": refresh_index,
                            "event_index": processed - 1,
                            "action": result["action"],
                            "wall_clock": now,
                            "post_pivot": entry["post_pivot"],
                            "track_ids": track_ids,
                        })
    except Exception as exc:  # noqa: BLE001
        with rec.lock:
            rec.errors.append(f"refresh driver: {exc!r}")
    finally:
        consumer.close()
        pg_conn.close()


def _profile_poller_thread(spec: ScenarioSpec, redis_client, deadline: float,
                           rec: _Recording, stop: threading.Event) -> None:
    """Samples session:{id}:profile_meta. Flink overwrites the key on every
    window fire, so polling is the only way to see more than the last
    value; each distinct computed_at is one H2 sample."""
    seen: set[str] = set()
    while not stop.is_set() and time.monotonic() < deadline:
        meta = redis_client.hgetall(profile_meta_key(spec.session_id))
        computed_at = meta.get("computed_at")
        if computed_at and computed_at not in seen:
            seen.add(computed_at)
            computed_at_f = float(computed_at)
            with rec.lock:
                # H2 is measured against the most recent event posted at or
                # before the centroid was computed -- that event is the last
                # information the window could possibly have contained.
                prior = [p for p in rec.posts if p["wall_clock"] <= computed_at_f]
                rec.profile_samples.append({
                    "computed_at": computed_at_f,
                    "n_events": int(meta.get("n_events", 0)),
                    "observed_at": time.time(),
                    "triggering_event_index": prior[-1]["index"] if prior else None,
                    "h2_profile_lag_seconds": (computed_at_f - prior[-1]["wall_clock"]) if prior else None,
                })
        time.sleep(PROFILE_POLL_INTERVAL_SECONDS)


def run_scenario(spec: ScenarioSpec, anchor: dt.datetime, ids_by_genre: dict[str, list[int]],
                 durations: dict[int, int], ingestion_url: str, semantic_api_url: str,
                 redis_client, milvus_collection) -> dict:
    """Drives one scenario end to end and returns everything observed."""
    script = scenario_gen.build_genre_session_script(spec.genre_track_counts, spec.seed, ids_by_genre)
    planned = sim_events.build_session_events(script, spec.session_id, durations, anchor)
    if spec.jitter > 0:
        import random
        planned = sim_events.apply_jitter(planned, spec.jitter, random.Random(spec.seed))

    pivot_index = (
        scenario_gen.pivot_event_index(script, spec.pivot_track_index)
        if spec.pivot_track_index is not None else None
    )
    simulated_span = sum(p["wall_clock_gap_seconds"] for p in planned)
    budget = simulated_span / spec.speed + 180.0

    reset_session(redis_client, spec.session_id)
    rec = _Recording()
    precondition: dict = {"profile_exists_at_pivot": None}
    stop = threading.Event()
    deadline = time.monotonic() + budget

    print(f"  [{spec.name}] {len(planned)} events, {simulated_span/60:.1f} min simulated, "
          f"~{simulated_span/spec.speed:.0f}s wall at speed {spec.speed}")

    threads = [
        threading.Thread(target=_raw_consumer_thread, args=(spec, len(planned), budget, rec), daemon=True),
        threading.Thread(target=_refresh_driver_thread,
                         args=(spec, len(planned), deadline, semantic_api_url, redis_client,
                               milvus_collection, rec, stop), daemon=True),
        threading.Thread(target=_profile_poller_thread,
                         args=(spec, redis_client, deadline, rec, stop), daemon=True),
    ]
    for t in threads:
        t.start()
    time.sleep(CONSUMER_WARMUP_SECONDS)

    session_start = time.time()
    _poster(spec, planned, pivot_index, ingestion_url, redis_client, rec, precondition)

    # Let the consumers drain what is still in flight: the debounce means a
    # refresh can legitimately land seconds after the last event was posted.
    drain_deadline = time.monotonic() + 45
    while time.monotonic() < min(drain_deadline, deadline):
        with rec.lock:
            done = len(rec.refresh_results) >= len(planned)
        if done:
            break
        time.sleep(0.5)
    stop.set()
    for t in threads:
        t.join(timeout=20)

    with rec.lock:
        if len(rec.refresh_results) != len(rec.posts):
            # The n-th refresh result is matched to the n-th posted event
            # (single-partition topic, so consume order == produce order).
            # A count mismatch means that assumption broke -- e.g. the
            # consumer picked up events this run never posted -- and every
            # pre/post-pivot split downstream would be quietly wrong.
            rec.errors.append(
                f"event/result count mismatch: {len(rec.posts)} posted, "
                f"{len(rec.refresh_results)} processed"
            )
        return {
            "name": spec.name,
            "session_id": spec.session_id,
            "seed": spec.seed,
            "speed": spec.speed,
            "jitter": spec.jitter,
            "genre_track_counts": [list(g) for g in spec.genre_track_counts],
            "anchor_event_time": anchor.isoformat(),
            "session_start_wall": session_start,
            "event_count": len(planned),
            "simulated_span_seconds": simulated_span,
            "pivot_event_index": pivot_index,
            "track_ids_in_order": [t["track_id"] for t in script["tracks"]],
            "warm_path_precondition": precondition,
            "posts": list(rec.posts),
            "refresh_results": list(rec.refresh_results),
            "snapshots": list(rec.snapshots),
            "profile_samples": list(rec.profile_samples),
            "errors": list(rec.errors),
        }
