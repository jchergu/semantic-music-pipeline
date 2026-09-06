"""Pure tests for eval/8_2's genre-based scenario generator -- no Postgres,
no live services (the genre->ids mapping is injected)."""
import datetime as dt
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT / "platform"))
sys.path.insert(0, str(ROOT))

from importlib import import_module  # noqa: E402

from simulator import events as sim_events  # noqa: E402

# Imported by dotted package path rather than eval/8_1's flat `import
# metrics` convention: "8_2" isn't a valid identifier, and more to the
# point a flat import would collide with eval/8_1's same-named modules in
# a single pytest session (one sys.modules["metrics"] for both packages).
scenario_gen = import_module("eval.8_2.scenario_gen")

IDS = {
    "chillout": [10, 20, 30, 40, 50],
    "rock": [1, 2, 3, 4, 5, 6, 7, 8],
}


def test_same_seed_gives_the_same_tracks():
    a = scenario_gen.build_genre_session_script([("chillout", 3), ("rock", 4)], 42, IDS)
    b = scenario_gen.build_genre_session_script([("chillout", 3), ("rock", 4)], 42, IDS)
    assert [t["track_id"] for t in a["tracks"]] == [t["track_id"] for t in b["tracks"]]


def test_different_seed_gives_different_tracks():
    a = scenario_gen.build_genre_session_script([("chillout", 3)], 1, IDS)
    b = scenario_gen.build_genre_session_script([("chillout", 3)], 2, IDS)
    assert [t["track_id"] for t in a["tracks"]] != [t["track_id"] for t in b["tracks"]]


def test_tracks_come_from_the_requested_genres_in_play_order():
    script = scenario_gen.build_genre_session_script([("chillout", 2), ("rock", 3)], 7, IDS)
    genres = [t["genre"] for t in script["tracks"]]
    assert genres == ["chillout", "chillout", "rock", "rock", "rock"]
    assert all(t["track_id"] in IDS[t["genre"]] for t in script["tracks"])


def test_no_track_repeats_within_a_session():
    script = scenario_gen.build_genre_session_script([("chillout", 5), ("rock", 8)], 3, IDS)
    ids = [t["track_id"] for t in script["tracks"]]
    assert len(ids) == len(set(ids))


def test_raises_loudly_when_a_genre_cannot_supply_enough_tracks():
    with pytest.raises(scenario_gen.ScenarioGenerationError, match="chillout"):
        scenario_gen.build_genre_session_script([("chillout", 99)], 1, IDS)


def test_generated_script_feeds_build_session_events_unchanged():
    """The whole point of matching scripts_io.load_script()'s shape."""
    script = scenario_gen.build_genre_session_script([("chillout", 2), ("rock", 2)], 5, IDS)
    durations = {tid: 200 for ids in IDS.values() for tid in ids}
    planned = sim_events.build_session_events(script, "s1", durations, dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc))
    assert len(planned) == 8  # 4 tracks x (play + complete)
    assert planned[0]["event"]["event_type"] == "play"
    assert planned[1]["event"]["event_type"] == "complete"


def test_pivot_event_index_points_at_the_first_post_pivot_event():
    script = scenario_gen.build_genre_session_script([("chillout", 3), ("rock", 2)], 5, IDS)
    durations = {tid: 200 for ids in IDS.values() for tid in ids}
    planned = sim_events.build_session_events(script, "s1", durations, dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc))
    idx = scenario_gen.pivot_event_index(script, 3)
    assert idx == 6
    assert planned[idx]["event"]["track_id"] == str(script["tracks"][3]["track_id"])
