"""
Stage 15D entrypoint: `python -m eval.8_2.failure_run`.

Runs METRICS.md section 9's four arms (control, redis_outage,
semantic_api_outage, ttl_expiry), twice each, against the real stage 16
deployment shape, and writes eval/8_2/failure_injection.json.

Separate from run.py on purpose. run.py drives harness-owned threads and
produces metrics 1-7; this drives five real processes and produces metric 8,
and METRICS.md section 9.7 keeps the two output packs apart for the same
reason -- they are measured against different process topologies, so folding
them together would blur what results.json's determinism claim covers.

Needs the stack up and track embeddings preloaded into Redis. It starts and
stops everything else itself:

    PYTHONPATH=platform platform/enrichment/.venv/bin/python \\
        platform/streaming/preload_embeddings_to_redis.py
    platform/enrichment/.venv/bin/python -m eval.8_2.failure_run

The redis_outage arms stop and start the 8-1-redis container, so this
must not be run against a stack anyone else is using.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from streaming.config import RECS_REFRESH_GROUP_ID, SESSION_CONSUMER_GROUP_ID  # noqa: E402

from . import failure_injection as fi  # noqa: E402
from . import metrics, orchestration, scenario_gen, store_access  # noqa: E402

OUT_DIR = Path(__file__).resolve().parent
RAW_NAME = "raw_failure_records.json"


def _flink_state() -> str:
    """The Flink job's state, tolerating a JobManager that is itself
    unreachable. Recorded per arm because stopping Redis can take the stage
    13 job down with it -- its window function writes to Redis -- and an arm
    whose profile stopped updating for that reason must be readable as such
    rather than looking like a recommender finding."""
    try:
        jobs = orchestration.list_flink_jobs()
    except Exception as exc:  # noqa: BLE001
        return f"unavailable: {type(exc).__name__}"
    running = [j for j in jobs if j.get("status") == "RUNNING"]
    return running[0]["status"] if running else (jobs[-1].get("status") if jobs else "none")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=None,
                        help="Scopes session ids to this invocation (default: a UTC timestamp).")
    parser.add_argument("--replicates", type=int, default=2,
                        help="Runs per arm. METRICS.md section 9.5 fixes this at 2 and says why.")
    parser.add_argument("--arms", nargs="*", default=None,
                        help="Run only these arm kinds (default: all four).")
    parser.add_argument("--label", default=None,
                        help="Suffix for the output files, e.g. 'before' for the pre-fix pack.")
    parser.add_argument("--from-records", default=None,
                        help="Recompute the verdicts from a saved raw pack, with no live run.")
    parser.add_argument("--no-manage-flink", action="store_true")
    args = parser.parse_args(argv)

    if args.from_records:
        records = json.loads(Path(args.from_records).read_text())
        write_outputs(records, args.label, recomputed_from=args.from_records)
        return

    run_id = args.run_id or dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S")
    arms = fi.build_arms(run_id, replicates=args.replicates)
    if args.arms:
        arms = [a for a in arms if a.kind in args.arms]

    pg_conn = store_access.connect_postgres()
    try:
        ids_by_genre = {g: scenario_gen.fetch_genre_track_ids(pg_conn, g)
                        for g, _ in fi.ARM_GENRES}
        with pg_conn.cursor() as cur:
            cur.execute("SELECT id, duration_sec FROM tracks")
            durations = {row[0]: row[1] for row in cur.fetchall()}
    finally:
        pg_conn.close()

    anchors = metrics.anchor_schedule(fi.arm_spans(arms, ids_by_genre, durations))
    redis_client = store_access.connect_redis()
    log_dir = OUT_DIR / ".daemon_logs"
    log_dir.mkdir(exist_ok=True)

    records: list[dict] = []
    semantic_api = orchestration.RestartableService("semantic_api.main:app", "semantic-api")
    semantic_api.start()
    try:
        with orchestration.uvicorn_service("event_ingestion.main:app", "event-ingestion") as ingestion_url, \
             orchestration.uvicorn_service("session_api.main:app", "session-api") as session_api_url, \
             orchestration.flink_session_profile_job(manage=not args.no_manage_flink), \
             orchestration.daemon_process(
                 "streaming.session_consumer_daemon", ["--group-id", SESSION_CONSUMER_GROUP_ID],
                 "raw-daemon", log_dir / "session_consumer_daemon.log"), \
             orchestration.daemon_process(
                 "streaming.refresh_daemon",
                 ["--group-id", RECS_REFRESH_GROUP_ID, "--semantic-api-url", semantic_api.base_url],
                 "refresh-daemon", log_dir / "refresh_daemon.log"):

            print(f"  daemons warming up ({fi.DAEMON_WARMUP_SECONDS}s)")
            time.sleep(fi.DAEMON_WARMUP_SECONDS)

            for arm, anchor in zip(arms, anchors):
                print(f"[{arm.name}] anchor={anchor.isoformat()}")
                flink_before = _flink_state()
                record = fi.run_arm(arm, anchor, ids_by_genre, durations,
                                    ingestion_url, session_api_url, semantic_api, redis_client)
                flink_after = _flink_state()
                record["flink"] = {"state_before": flink_before, "state_after": flink_after}
                # The stage 13 job writes to Redis from its window function
                # and the compose cluster runs it with no checkpointing, so
                # Flink's restart strategy is "none": stopping Redis takes
                # the job down permanently. Without resubmitting here, every
                # arm after the first redis_outage would run with no session
                # profile at all, never reach the warm path, and differ from
                # the control for a reason that has nothing to do with its
                # own injection.
                if flink_after != "RUNNING" and not args.no_manage_flink:
                    print(f"  [{arm.name}] flink is {flink_after}; resubmitting before the next arm")
                    record["flink"]["resubmitted_after_arm"] = True
                    record["notes"].append(f"flink was {flink_after} after this arm; resubmitted")
                    orchestration.cancel_running_flink_jobs()
                    orchestration.submit_flink_job()
                # An outage arm can leave the Semantic API down if the revert
                # failed; the next arm would then silently measure that
                # instead of its own injection.
                if not semantic_api.running:
                    record["notes"].append("semantic api was not running after this arm; restarted")
                    semantic_api.start()
                records.append(record)
                _write_raw(records, args.label)
    finally:
        semantic_api.stop()

    write_outputs(records, args.label)


def _write_raw(records: list[dict], label: str | None) -> None:
    """Written after EVERY arm, not once at the end. An arm takes minutes and
    stops a database container; a crash in arm 7 must not destroy arms 1-6.
    Same principle run.py applies by writing raw artifacts before metrics."""
    (OUT_DIR / _named(RAW_NAME, label)).write_text(
        json.dumps(records, indent=2, sort_keys=True) + "\n")


def _named(name: str, label: str | None) -> str:
    if not label:
        return name
    stem, _, ext = name.rpartition(".")
    return f"{stem}_{label}.{ext}"


def write_outputs(records: list[dict], label: str | None,
                  recomputed_from: str | None = None) -> None:
    if recomputed_from is None:
        _write_raw(records, label)

    by_kind: dict[str, list[dict]] = {}
    for record in records:
        by_kind.setdefault(record["arm"], []).append(record)

    control = by_kind.get("none")
    if not control:
        raise SystemExit("no control arm in the records -- section 9.5's rule needs one")
    # The control arm is a baseline, not a treatment: its own replicates are
    # collapsed by taking the first, and any disagreement between them is
    # reported rather than averaged away.
    control_record = control[0]
    control_disagreement = [
        m for m in metrics.CATEGORICAL_MEASURES + metrics.COUNT_MEASURES
        if len({repr(metrics._measure_value(r, m)) for r in control}) > 1
    ]

    verdicts = {
        kind: metrics.failure_injection_verdict(control_record, arms)
        for kind, arms in by_kind.items() if kind != "none"
    }

    out = {
        "metric": "8 -- failure injection (METRICS.md section 9)",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "recomputed_from": recomputed_from,
        "deployment_shape": (
            "real: semantic_api + event_ingestion + session_api + "
            "session_consumer_daemon + refresh_daemon + flink job, "
            "daemons under the canonical group ids"
        ),
        "scenario": {
            "genres": fi.ARM_GENRES, "seed": fi.ARM_SEED, "speed": fi.ARM_SPEED,
            "inject_at_event_index": fi.INJECT_AT_EVENT,
            "revert_at_event_index": fi.REVERT_AT_EVENT,
        },
        "control": {
            "replicates": len(control),
            "events_lost": control_record["events_lost"],
            "staleness_detectable": control_record["staleness_detectable"],
            "replicate_disagreement": control_disagreement,
        },
        "verdicts": verdicts,
        "arms": [
            {k: v for k, v in r.items() if k != "posts"} for r in records
        ],
    }
    path = OUT_DIR / _named("failure_injection.json", label)
    path.write_text(json.dumps(out, indent=2, sort_keys=True, default=str) + "\n")
    print(f"\nwrote {path}")
    for kind, verdict in sorted(verdicts.items()):
        effects = ", ".join(verdict["effects_beyond_control"]) or "none"
        unstable = ", ".join(verdict["unstable_measures"])
        print(f"  {kind:24s} pass={verdict['pass']!s:5s} effects: {effects}"
              + (f"  UNSTABLE: {unstable}" if unstable else ""))


if __name__ == "__main__":
    main()
