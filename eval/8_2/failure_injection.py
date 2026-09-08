"""
Stage 15D: failure injection against the REAL stage 16 deployment shape.

METRICS.md section 9. This module is deliberately NOT built on
harness.run_scenario(): that harness owns test-shaped threads (its own
raw-state consumer loop, its own driver loop over process_one_event), and
section 0 is explicit that those are not a deployment. A failure-injection
result measured against harness threads would be a claim about the harness
rather than about the system, and the client-visible half of this metric --
what `GET /sessions/{id}` says while a store is down -- would have no client
in it at all.

So this drives the real thing: five processes (Semantic API, event
ingestion, session_api, and the two stage 16 daemons under the CANONICAL
consumer group ids from streaming/config.py), plus the stage 13 Flink job.
Committed offsets and restart behaviour are part of what is measured, and a
throwaway group id would replay the never-purged topic from earliest and
hide exactly that.

What is reused rather than rebuilt: orchestration.RestartableService and
daemon_process for the processes, orchestration.flink_session_profile_job
for the job, scenario_gen + platform/simulator for the event stream,
metrics.anchor_schedule for event-time anchors, store_access for the
stores. What is new is only the injection and the observation.
"""
from __future__ import annotations

import datetime as dt
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from simulator import events as sim_events  # noqa: E402
from streaming.session_state import (  # noqa: E402
    events_key, profile_key, recs_key,
)

from . import metrics, orchestration, scenario_gen, store_access  # noqa: E402

REDIS_CONTAINER = "8-1-redis"

# The scenario every arm shares: METRICS.md section 9.2. K=8 chillout ->
# 8 rock, speed 60 -- the same scenario metrics 6 and 7 use, already
# confirmed to satisfy the warm-path precondition, so an arm that produces
# no warm refresh is a finding rather than a mis-chosen scenario.
ARM_GENRES = [("chillout", 8), ("rock", 8)]
ARM_PIVOT_TRACK_INDEX = 8
ARM_SPEED = 60.0
ARM_SEED = 42

# Section 9.2: the injection window is indexed by EVENT, not wall-clock.
# Each track contributes a play + a complete, so 32 events; the pivot's
# first event is index 16. Injecting at 22 leaves the pivot and at least one
# post-pivot refresh healthy inside every arm, and reverting at 28 leaves
# four events for recovery to show up in.
INJECT_AT_EVENT = 22
REVERT_AT_EVENT = 28

# Both daemons start from their committed offset under the canonical group
# ids, but group-join is not instant and the refresh daemon opens Milvus and
# Postgres first. Longer than the metric 1-7 harness's 6s because these are
# processes rather than threads.
DAEMON_WARMUP_SECONDS = 12.0
# After the last event, how long to let the debounce and the refresh loop
# settle before the final client sample.
DRAIN_SECONDS = 20.0


@dataclass
class Arm:
    """One row of METRICS.md section 9.3."""
    name: str
    kind: str  # none | redis_outage | semantic_api_outage | ttl_expiry
    replicate: int
    session_id: str


@dataclass
class _ArmState:
    """Everything one arm records. Written to the raw pack before any
    verdict is computed, for the reason section 0 gives for every other
    metric here: the run is the measurement, the verdict is derivable."""
    posts: list[dict] = field(default_factory=list)
    client_samples: list[dict] = field(default_factory=list)
    injection: dict = field(default_factory=dict)
    flink: dict = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


def _docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True, capture_output=True, text=True)


def sample_client(session_api_url: str, session_id: str, phase: str) -> dict:
    """METRICS.md section 9.4, measure 3: the whole client view at one phase.

    Every request is wrapped: during a Redis outage session_api raises
    rather than answering, and "the service returned nothing at all" is a
    legitimate observation about what a client sees -- not an error that
    should abort the arm and destroy the run that produced it.
    """
    sample = {"phase": phase, "observed_at": time.time()}
    for label, path in (
        ("session", f"/sessions/{session_id}"),
        ("recommendations", f"/sessions/{session_id}/recommendations"),
        ("profile", f"/sessions/{session_id}/profile"),
    ):
        try:
            resp = httpx.get(f"{session_api_url}{path}", timeout=10.0)
            sample[f"{label}_status"] = resp.status_code
            try:
                sample[f"{label}_body"] = resp.json()
            except ValueError:
                sample[f"{label}_body"] = resp.text[:500]
        except Exception as exc:  # noqa: BLE001
            sample[f"{label}_status"] = None
            sample[f"{label}_body"] = f"{type(exc).__name__}: {exc}"
    return sample


def _session_event_count(pg_conn, session_id: str) -> int:
    """Postgres is the ground truth for measure 1 (section 9.4): it is the
    system of record, it has no TTL, and the raw-state daemon writes it and
    Redis on the same poll -- so a post that reached Kafka and is absent
    here was consumed and discarded."""
    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM events WHERE session_id = %s", (session_id,))
        return cur.fetchone()[0]


def _last_refresh_ts(redis_client, session_id: str) -> float | None:
    raw = redis_client.hget(f"session:{session_id}:refresh_meta", "last_refresh_ts")
    return float(raw) if raw is not None else None


def apply_injection(arm: Arm, redis_client, semantic_api: orchestration.RestartableService,
                    state: _ArmState) -> None:
    if arm.kind == "redis_outage":
        _docker("stop", REDIS_CONTAINER)
    elif arm.kind == "semantic_api_outage":
        semantic_api.stop()
    elif arm.kind == "ttl_expiry":
        # Section 9.3: reproduces the stage 16 deferred finding
        # deterministically instead of waiting 30 minutes for the real
        # sliding TTL. Same idiom tests/test_stage16_session_api.py already
        # uses to construct this state.
        redis_client.expire(events_key(arm.session_id), 0)
    state.injection["applied_at"] = time.time()


def revert_injection(arm: Arm, semantic_api: orchestration.RestartableService,
                     state: _ArmState) -> None:
    if arm.kind == "redis_outage":
        _docker("start", REDIS_CONTAINER)
        _wait_for_redis()
    elif arm.kind == "semantic_api_outage":
        semantic_api.start()
    elif arm.kind == "ttl_expiry":
        # Nothing to revert: expiry is not an outage. Recorded explicitly so
        # a reader does not read the missing revert as an omission.
        state.notes.append("ttl_expiry is not reverted -- expiry is a state loss, not an outage")
        return
    state.injection["reverted_at"] = time.time()


def _wait_for_redis(timeout: float = 60.0) -> None:
    import redis as redis_lib
    deadline = time.monotonic() + timeout
    client = store_access.connect_redis()
    while time.monotonic() < deadline:
        try:
            client.ping()
            return
        except redis_lib.RedisError:
            time.sleep(0.5)
    raise RuntimeError(f"redis did not come back within {timeout}s")


def run_arm(arm: Arm, anchor: dt.datetime, ids_by_genre: dict, durations: dict,
            ingestion_url: str, session_api_url: str,
            semantic_api: orchestration.RestartableService,
            redis_client) -> dict:
    """Posts one scenario, applying and reverting the arm's injection at the
    event indices section 9.2 fixes, sampling the client view at four
    phases."""
    state = _ArmState()
    script = scenario_gen.build_genre_session_script(ARM_GENRES, ARM_SEED, ids_by_genre)
    planned = sim_events.build_session_events(script, arm.session_id, durations, anchor)
    pivot_event_index = scenario_gen.pivot_event_index(script, ARM_PIVOT_TRACK_INDEX)

    # The arm's own keys only -- never a wildcard delete.
    redis_client.delete(*[f"session:{arm.session_id}:{s}"
                          for s in ("events", "profile", "profile_meta", "recs", "refresh_meta")])

    pg_conn = store_access.connect_postgres()
    pg_conn.autocommit = True
    try:
        state.injection.update({
            "kind": arm.kind,
            "inject_at_event_index": INJECT_AT_EVENT if arm.kind != "none" else None,
            "revert_at_event_index": REVERT_AT_EVENT if arm.kind not in ("none", "ttl_expiry") else None,
        })

        print(f"  [{arm.name}] posting {len(planned)} events (pivot at {pivot_event_index})")
        with httpx.Client(base_url=ingestion_url, timeout=10.0) as client:
            for i, item in enumerate(planned):
                gap = item["wall_clock_gap_seconds"]
                if gap > 0:
                    time.sleep(gap / ARM_SPEED)

                # The control arm samples at the SAME event indices as the
                # injected arms, and simply injects nothing there. Section
                # 9.5 compares the phase/status sequences across arms, so
                # sampling the control at a different point in the session
                # would make every arm look different from it for reasons
                # that have nothing to do with any failure.
                if i == INJECT_AT_EVENT:
                    state.client_samples.append(
                        sample_client(session_api_url, arm.session_id, "before"))
                    # Both baselines are read while everything is still up,
                    # including for redis_outage -- this happens before the
                    # container is stopped.
                    state.injection["events_in_postgres_at_injection"] = _session_event_count(
                        pg_conn, arm.session_id)
                    state.injection["last_refresh_ts_at_injection"] = _last_refresh_ts(
                        redis_client, arm.session_id)
                    if arm.kind != "none":
                        print(f"  [{arm.name}] INJECT {arm.kind} at event {i}")
                        apply_injection(arm, redis_client, semantic_api, state)
                    state.client_samples.append(
                        sample_client(session_api_url, arm.session_id, "during"))

                if i == REVERT_AT_EVENT:
                    if arm.kind not in ("none", "ttl_expiry"):
                        print(f"  [{arm.name}] REVERT {arm.kind} at event {i}")
                        revert_injection(arm, semantic_api, state)
                    state.client_samples.append(
                        sample_client(session_api_url, arm.session_id, "after"))

                started = time.time()
                # The wall-clock the pacing above INTENDED to consume before
                # this post. Compared against what actually elapsed, below --
                # see wall_clock_stall().
                expected_sleep = gap / ARM_SPEED
                try:
                    resp = client.post("/events", json=item["event"])
                    accepted = resp.status_code < 300
                    status = resp.status_code
                except Exception as exc:  # noqa: BLE001
                    accepted, status = False, None
                    state.notes.append(f"post {i} failed: {type(exc).__name__}: {exc}")
                state.posts.append({
                    "index": i,
                    "wall_clock": started,
                    "expected_sleep_seconds": expected_sleep,
                    "status": status,
                    "accepted": accepted,
                    "track_id": item["event"]["track_id"],
                    "event_type": item["event"]["event_type"],
                    "event_time": item["event"]["event_time"],
                    "in_injection_window": (
                        arm.kind != "none" and INJECT_AT_EVENT <= i < (
                            REVERT_AT_EVENT if arm.kind != "ttl_expiry" else len(planned))
                    ),
                })

        print(f"  [{arm.name}] draining {DRAIN_SECONDS}s")
        time.sleep(DRAIN_SECONDS)
        state.client_samples.append(sample_client(session_api_url, arm.session_id, "end"))

        accepted_posts = [p for p in state.posts if p["accepted"]]
        in_postgres = _session_event_count(pg_conn, arm.session_id)
        last_refresh_after = _last_refresh_ts(redis_client, arm.session_id)
        ts_at_injection = state.injection.get("last_refresh_ts_at_injection")
        events_at_injection = state.injection.get("events_in_postgres_at_injection")

        # Section 9.4 measure 2: did the pipeline resume with nothing
        # restarted? Read from state, not from daemon logs -- the raw log
        # grew, or the refresh timestamp advanced, or it did not.
        if arm.kind == "none":
            recovers = None
        else:
            raw_resumed = (events_at_injection is not None and in_postgres > events_at_injection)
            refresh_resumed = (
                last_refresh_after is not None
                and (ts_at_injection is None or last_refresh_after > ts_at_injection)
            )
            recovers = bool(raw_resumed and refresh_resumed)

        record = {
            "arm": arm.kind,
            "arm_name": arm.name,
            "replicate": arm.replicate,
            "session_id": arm.session_id,
            "anchor_event_time": anchor.isoformat(),
            "speed": ARM_SPEED,
            "seed": ARM_SEED,
            "pivot_event_index": pivot_event_index,
            "posts": state.posts,
            "posts_accepted": len(accepted_posts),
            "events_in_postgres": in_postgres,
            "events_lost": len(accepted_posts) - in_postgres,
            "recovers_without_restart": recovers,
            "raw_events_resumed": None if arm.kind == "none" else raw_resumed,
            "refresh_resumed": None if arm.kind == "none" else refresh_resumed,
            "last_refresh_ts_at_end": last_refresh_after,
            "recs_present_at_end": redis_client.exists(recs_key(arm.session_id)) == 1,
            "profile_present_at_end": redis_client.exists(profile_key(arm.session_id)) == 1,
            "injection": state.injection,
            "client_samples": state.client_samples,
            "notes": state.notes,
        }
        record.update(staleness(record))
        record.update(wall_clock_stall(state.posts))
        if record["wall_clock_stall_detected"]:
            state.notes.append(
                f"WALL-CLOCK STALL: {record['max_post_gap_overrun_seconds']:.0f}s beyond intended "
                "pacing between two posts -- this arm's run conditions were not the specified "
                "ones and it must be re-run, not reported"
            )
            print(f"  [{arm.name}] *** wall-clock stall detected; arm is invalid ***")
        return record
    finally:
        pg_conn.close()


# A gap this far beyond the intended pacing is not scheduling jitter. The
# threshold is generous on purpose: a real stall (see wall_clock_stall) is
# hours, and flagging a healthy arm would be worse than missing nothing.
STALL_THRESHOLD_SECONDS = 60.0


def wall_clock_stall(posts: list[dict]) -> dict:
    """Detects an arm whose wall-clock was interrupted mid-run.

    Added after a real incident: the machine running the first pre-fix pack
    suspended overnight, freezing one arm for 20 hours between two of its
    posts. Nothing in the data said so -- the arm completed, every event
    landed, and its measures looked ordinary -- but its run conditions were
    not the specified ones. The refresh debounce is wall-clock while --speed
    compresses only session time, and session:{id}:events carries a 30-minute
    TTL, so a suspended arm silently becomes a TTL-expiry arm no matter which
    injection it was supposed to be testing.

    An arm flagged here is not adjusted or reweighted; it is excluded and
    re-run. A measurement taken under conditions the spec did not describe is
    not a weaker observation, it is a different experiment.
    """
    overruns = []
    for prev, post in zip(posts, posts[1:]):
        actual = post["wall_clock"] - prev["wall_clock"]
        overruns.append(actual - post.get("expected_sleep_seconds", 0.0))
    worst = max(overruns) if overruns else 0.0
    return {
        "max_post_gap_overrun_seconds": worst,
        "wall_clock_stall_detected": worst > STALL_THRESHOLD_SECONDS,
        "stall_threshold_seconds": STALL_THRESHOLD_SECONDS,
    }


def staleness(record: dict) -> dict:
    """METRICS.md section 9.4, measure 4: could a client tell, FROM THE API
    RESPONSES ALONE, that what it is being served is stale or incomplete?

    Deliberately strict. A 5xx or a dead connection counts -- the client
    learns something is wrong. `raw_state_expired: true` counts, and so does
    a `notes` entry, because both are fields session_api emits for exactly
    this purpose. What does NOT count is a 200 carrying recommendations that
    are simply older than the client thinks: session:{id}:recs has no
    timestamp of any kind (unlike :profile, whose sibling :profile_meta
    carries computed_at), so there is no field a client could read to notice.
    """
    evidence = []
    for sample in record["client_samples"]:
        if sample["phase"] not in ("during", "after"):
            continue
        for label in ("session", "recommendations", "profile"):
            status = sample.get(f"{label}_status")
            if status is None:
                evidence.append(f"{sample['phase']}: {label} unreachable")
            elif status >= 500:
                evidence.append(f"{sample['phase']}: {label} HTTP {status}")
        body = sample.get("session_body")
        if isinstance(body, dict):
            if body.get("raw_state_expired"):
                evidence.append(f"{sample['phase']}: raw_state_expired=true")
            for note in body.get("notes", []) or []:
                evidence.append(f"{sample['phase']}: note: {note[:80]}")
    return {"staleness_detectable": bool(evidence), "staleness_evidence": evidence}


def build_arms(run_id: str, replicates: int = 2) -> list[Arm]:
    """Section 9.3's four arms, each run `replicates` times. Session ids are
    scoped per invocation for the reason run.py:build_specs documents: the
    topic is never purged and a reused id replays the last run's events."""
    kinds = [("control", "none"), ("redis_outage", "redis_outage"),
             ("semantic_api_outage", "semantic_api_outage"), ("ttl_expiry", "ttl_expiry")]
    return [
        Arm(name=f"{name}_rep{r}", kind=kind, replicate=r,
            session_id=f"eval82-fail-{name}-rep{r}-{run_id}")
        for name, kind in kinds
        for r in range(1, replicates + 1)
    ]


def arm_spans(arms: list[Arm], ids_by_genre: dict, durations: dict) -> list[float]:
    """Every arm runs the same scenario, so every span is the same -- but
    metrics.anchor_schedule() still needs one per arm, because the anchors
    must advance monotonically through event time (the Flink watermark is
    stream-wide, not per key: METRICS.md section 1)."""
    script = scenario_gen.build_genre_session_script(ARM_GENRES, ARM_SEED, ids_by_genre)
    planned = sim_events.build_session_events(script, "span-probe", durations, metrics.EVAL_EPOCH)
    span = sum(p["wall_clock_gap_seconds"] for p in planned)
    return [span for _ in arms]
