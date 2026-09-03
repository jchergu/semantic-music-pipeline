"""Verifies Stage 15B's simulator capability audit and late-event support:
events.apply_jitter()'s pure reordering logic (unit tests, no live
services), and a live test proving the simulator's out-of-order delivery
actually survives to Kafka/Redis/Postgres, diverging from event_time order
by more than the Flink job's 5s watermark bound (Duration.of_seconds(5) in
platform/streaming/flink_session_profile_job.py). Whether Flink itself
drops such a late event is confirmed manually, not by an automated test --
see docs/platform/stage15b-simulator-jitter-and-late-event-audit.md."""
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "platform"))

from simulator import cli as simulator_cli  # noqa: E402
from simulator import events  # noqa: E402
from streaming.config import PG_DSN  # noqa: E402
from streaming.session_consumer import consume_and_cache_many  # noqa: E402
from streaming.session_store import apply_schema, get_events  # noqa: E402
from streaming.topics import create_topics  # noqa: E402

SCRIPTS_DIR = ROOT / "platform" / "simulator" / "scripts"
FLINK_WATERMARK_BOUND_SECONDS = 5.0  # Duration.of_seconds(5) in flink_session_profile_job.py


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
    group_id = f"stage15b-test-{uuid.uuid4()}"
    drained = consume_and_cache_many(
        group_id, count=expected_count, timeout=15.0, expected_session_id=session_id, pg_conn=pg_conn
    )
    assert len(drained) == expected_count, f"expected {expected_count} events for {session_id}, drained {len(drained)}"
    return drained


def _synthetic_script(num_tracks: int = 6) -> dict:
    return {
        "name": "synthetic-jitter",
        "intent": "for unit tests",
        "tracks": [
            {
                "track_id": 100 + i,
                "events": [
                    {"type": "play", "position_ms": 0},
                    {"type": "complete", "position_pct": 0.9},
                ],
            }
            for i in range(num_tracks)
        ],
    }


def _synthetic_planned(num_tracks: int = 6) -> list[dict]:
    durations = {100 + i: 200 for i in range(num_tracks)}
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return events.build_session_events(_synthetic_script(num_tracks), "sess-jitter", durations, start)


# --- unit tests: events.apply_jitter, pure, no I/O ---


def test_apply_jitter_noop_when_jitter_zero():
    planned = _synthetic_planned()
    rng = random.Random(42)
    state_before = rng.getstate()

    result = events.apply_jitter(planned, 0.0, rng)

    assert result is planned
    assert rng.getstate() == state_before  # zero draws


def test_apply_jitter_is_deterministic_given_same_seed():
    planned = _synthetic_planned()

    result_1 = events.apply_jitter(list(planned), 0.6, random.Random(42))
    result_2 = events.apply_jitter(list(planned), 0.6, random.Random(42))

    seq_1 = [(p["event"]["event_type"], p["event"]["track_id"]) for p in result_1]
    seq_2 = [(p["event"]["event_type"], p["event"]["track_id"]) for p in result_2]
    assert seq_1 == seq_2


def test_apply_jitter_is_a_permutation():
    planned = _synthetic_planned()
    rng = random.Random(7)

    result = events.apply_jitter(list(planned), 0.7, rng)

    def _key(p):
        return (p["event"]["event_type"], p["event"]["track_id"], p["event"]["event_time"])

    assert sorted(_key(p) for p in result) == sorted(_key(p) for p in planned)
    assert len(result) == len(planned)


def test_apply_jitter_produces_genuine_reordering():
    planned = _synthetic_planned(num_tracks=8)
    rng = random.Random(1)

    result = events.apply_jitter(list(planned), 1.0, rng)

    times = [datetime.fromisoformat(p["event"]["event_time"]) for p in result]
    assert times != sorted(times)


def test_apply_jitter_bounds_displacement():
    planned = _synthetic_planned(num_tracks=8)
    max_delay = 2
    rng = random.Random(3)

    result = events.apply_jitter(list(planned), 1.0, rng, max_delay=max_delay)

    original_index = {id(p): i for i, p in enumerate(planned)}
    for new_index, item in enumerate(result):
        orig = original_index[id(item)]
        assert abs(new_index - orig) <= max_delay


# --- live end-to-end: --jitter survives CLI -> event ingestion HTTP -> Kafka -> Postgres ---


def test_simulator_jitter_delivers_events_out_of_event_time_order_beyond_watermark_bound(
    event_ingestion_server, streaming_pg_conn
):
    script_path = str(SCRIPTS_DIR / "coherent_session.yaml")

    # A small, fixed, deterministic seed list -- not randomly chosen at test
    # time -- so the test itself stays fully reproducible even though which
    # seed happens to produce a >5s inversion depends on the interaction
    # between the script's actual track-boundary gaps and this seed's rng
    # draws inside apply_jitter.
    candidate_seeds = [9000, 9001, 9002, 9003, 9004]
    qualifying = None

    for seed in candidate_seeds:
        results = simulator_cli.main(
            [
                "--seed",
                str(seed),
                "--speed",
                "1000",
                "--sessions",
                "1",
                "--script",
                script_path,
                "--jitter",
                "1.0",
                "--ingestion-url",
                event_ingestion_server,
            ]
        )
        (session_id, posted), = results.items()

        drained = _drain(session_id, len(posted), streaming_pg_conn)
        assert drained == posted  # jitter reorders delivery, it never drops/duplicates events

        event_times = [datetime.fromisoformat(e["event_time"]) for e in posted]
        for i in range(len(event_times) - 1):
            gap = (event_times[i] - event_times[i + 1]).total_seconds()
            if gap > FLINK_WATERMARK_BOUND_SECONDS:
                qualifying = (seed, i, gap)
                break
        if qualifying:
            break

    assert qualifying is not None, (
        f"none of the seeds {candidate_seeds} produced a delivery-order inversion "
        f"exceeding the {FLINK_WATERMARK_BOUND_SECONDS}s watermark bound"
    )
