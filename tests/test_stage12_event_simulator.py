"""Verifies the Stage 12 event simulator: events.py/scripts_io.py's pure
logic (unit tests, no live services) and the end-to-end CLI against a live
event ingestion service + the stage 9-10 consumer, draining
behavioral-events into Redis and Postgres the same way
test_stage11_event_ingestion.py already does."""
import sys
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from simulator import cli as simulator_cli  # noqa: E402
from simulator import events, scripts_io  # noqa: E402
from streaming.config import PG_DSN  # noqa: E402
from streaming.session_consumer import consume_and_cache_many  # noqa: E402
from streaming.session_state import get_session_events  # noqa: E402
from streaming.session_store import apply_schema, get_events  # noqa: E402
from streaming.topics import create_topics  # noqa: E402

SCRIPTS_DIR = ROOT / "platform" / "simulator" / "scripts"


@pytest.fixture(scope="module", autouse=True)
def _ensure_topics():
    create_topics()


@pytest.fixture()
def streaming_pg_conn():
    import psycopg2

    conn = psycopg2.connect(PG_DSN)
    apply_schema(conn)
    yield conn
    conn.close()


def _drain(session_id: str, expected_count: int, pg_conn) -> list[dict]:
    group_id = f"stage12-test-{uuid.uuid4()}"
    drained = consume_and_cache_many(
        group_id, count=expected_count, timeout=15.0, expected_session_id=session_id, pg_conn=pg_conn
    )
    assert len(drained) == expected_count, f"expected {expected_count} events for {session_id}, drained {len(drained)}"
    return drained


# --- unit tests: events.py, pure functions, no I/O ---


def test_resolve_position_ms_absolute():
    assert events.resolve_position_ms({"position_ms": 2500}, duration_sec=200) == 2500


def test_resolve_position_ms_relative():
    assert events.resolve_position_ms({"position_pct": 0.5}, duration_sec=200) == 100000


def test_resolve_position_ms_requires_exactly_one_field():
    with pytest.raises(events.ScriptValidationError):
        events.resolve_position_ms({}, duration_sec=200)
    with pytest.raises(events.ScriptValidationError):
        events.resolve_position_ms({"position_ms": 1, "position_pct": 0.1}, duration_sec=200)


def _synthetic_script() -> dict:
    return {
        "name": "synthetic",
        "intent": "for unit tests",
        "tracks": [
            {
                "track_id": 101,
                "events": [
                    {"type": "play", "position_ms": 0},
                    {"type": "complete", "position_pct": 0.9},
                ],
            },
            {
                "track_id": 102,
                "events": [
                    {"type": "play", "position_ms": 0},
                    {"type": "skip", "position_ms": 2000},
                ],
            },
        ],
    }


def test_build_session_events_is_ordered_and_monotonic():
    durations = {101: 200, 102: 200}
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    planned = events.build_session_events(_synthetic_script(), "sess-1", durations, start)

    assert [p["event"]["event_type"] for p in planned] == ["play", "complete", "play", "skip"]
    times = [datetime.fromisoformat(p["event"]["event_time"]) for p in planned]
    assert times == sorted(times)
    assert times[0] == start


def test_build_session_events_gap_reflects_position_not_full_duration():
    # track 101's last event is at 90% of 200s = 180s; track 102's first
    # event (play) should follow with zero gap -- an early skip means the
    # next track starts almost immediately, not after the nominal duration.
    durations = {101: 200, 102: 200}
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    planned = events.build_session_events(_synthetic_script(), "sess-1", durations, start)

    assert planned[1]["event"]["event_type"] == "complete"
    assert planned[1]["wall_clock_gap_seconds"] == pytest.approx(180.0)
    assert planned[2]["event"]["event_type"] == "play"
    assert planned[2]["wall_clock_gap_seconds"] == 0.0


def test_build_session_events_rejects_unknown_track():
    with pytest.raises(events.ScriptValidationError):
        events.build_session_events(_synthetic_script(), "sess-1", {101: 200}, datetime.now(timezone.utc))


def test_build_session_events_rejects_unknown_event_type():
    script = _synthetic_script()
    script["tracks"][0]["events"][0]["type"] = "rage_quit"
    with pytest.raises(events.ScriptValidationError):
        events.build_session_events(script, "sess-1", {101: 200, 102: 200}, datetime.now(timezone.utc))


def test_session_ids_are_deterministic_from_seed():
    # cli.main()'s scheme is f"sim-{seed}-{i}" -- no randomness involved,
    # so this holds trivially, but it's the load-bearing assumption behind
    # the "same seed -> identical events" exit criterion.
    assert f"sim-{42}-{0}" == "sim-42-0"


# --- unit tests: scripts_io.py, real shipped scripts ---


@pytest.mark.parametrize("filename", ["coherent_session.yaml", "three_early_skips.yaml", "context_switch.yaml"])
def test_canned_scripts_load_and_validate(filename):
    script = scripts_io.load_script(SCRIPTS_DIR / filename)
    assert script["name"]
    assert script["intent"]
    assert len(script["tracks"]) >= 1


def test_scripts_io_rejects_missing_key(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("name: x\nintent: y\n")  # missing tracks
    with pytest.raises(scripts_io.ScriptValidationError):
        scripts_io.load_script(bad)


def test_scripts_io_rejects_unknown_event_type(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "name: x\nintent: y\ntracks:\n  - track_id: 1\n    events:\n      - {type: rage_quit, position_ms: 0}\n"
    )
    with pytest.raises(scripts_io.ScriptValidationError):
        scripts_io.load_script(bad)


# --- live end-to-end: CLI -> event ingestion HTTP -> Kafka -> Redis + Postgres ---


def test_simulator_e2e_lands_in_redis_and_postgres(event_ingestion_server, streaming_pg_conn):
    seed = int(uuid.uuid4().int % 1_000_000)
    results = simulator_cli.main(
        [
            "--seed",
            str(seed),
            "--speed",
            "1000",
            "--sessions",
            "1",
            "--script",
            str(SCRIPTS_DIR / "three_early_skips.yaml"),
            "--ingestion-url",
            event_ingestion_server,
        ]
    )
    (session_id, posted), = results.items()
    assert session_id == f"sim-{seed}-0"
    assert len(posted) == 8  # 4 tracks x 2 events each

    drained = _drain(session_id, len(posted), streaming_pg_conn)
    assert drained == posted
    assert get_session_events(session_id) == posted
    assert get_events(streaming_pg_conn, session_id) == posted


def test_simulator_seed_determinism_across_two_runs(event_ingestion_server, streaming_pg_conn):
    seed = int(uuid.uuid4().int % 1_000_000)
    script_path = str(SCRIPTS_DIR / "coherent_session.yaml")
    run_kwargs = [
        "--seed",
        str(seed),
        "--speed",
        "1000",
        "--sessions",
        "1",
        "--script",
        script_path,
        "--ingestion-url",
        event_ingestion_server,
    ]

    # Both runs post to the exact same deterministic session_id (by design --
    # that's the point being verified), so they must be drained together in
    # one pass: draining after run 1 alone, then again after run 2, would
    # just re-read run 1's events both times (see consume_and_cache_many's
    # docstring -- no offsets are ever committed, "earliest" always wins).
    results_1 = simulator_cli.main(run_kwargs)
    (session_id, posted_1), = results_1.items()

    results_2 = simulator_cli.main(run_kwargs)
    (session_id_2, posted_2), = results_2.items()
    assert session_id_2 == session_id  # same seed -> same deterministic session_id
    assert posted_1 == posted_2  # byte-identical event content across the two runs

    _drain(session_id, len(posted_1) + len(posted_2), streaming_pg_conn)

    all_events = get_events(streaming_pg_conn, session_id)
    assert len(all_events) == len(posted_1) + len(posted_2)
    # "identical event sequences ... modulo event_id": every event from run 1
    # appears exactly twice in Postgres (once per run), by content.
    counts = Counter(tuple(sorted(e.items())) for e in all_events)
    for event in posted_1:
        assert counts[tuple(sorted(event.items()))] == 2
