"""
Stage 12: YAML loading + structural validation for session scripts. The
only I/O is reading the file itself -- no network, no database.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from simulator.events import KNOWN_EVENT_TYPES, ScriptValidationError


def load_script(path: str | Path) -> dict:
    with open(path) as f:
        script = yaml.safe_load(f)
    _validate(script, source=str(path))
    return script


def _validate(script, source: str) -> None:
    if not isinstance(script, dict):
        raise ScriptValidationError(f"{source}: top level must be a mapping")
    for key in ("name", "intent", "tracks"):
        if key not in script:
            raise ScriptValidationError(f"{source}: missing required key {key!r}")
    if not isinstance(script["tracks"], list) or not script["tracks"]:
        raise ScriptValidationError(f"{source}: 'tracks' must be a non-empty list")
    for i, track in enumerate(script["tracks"]):
        if "track_id" not in track:
            raise ScriptValidationError(f"{source}: tracks[{i}] missing 'track_id'")
        if not isinstance(track.get("events"), list) or not track["events"]:
            raise ScriptValidationError(f"{source}: tracks[{i}] must have a non-empty 'events' list")
        for j, event in enumerate(track["events"]):
            if event.get("type") not in KNOWN_EVENT_TYPES:
                raise ScriptValidationError(
                    f"{source}: tracks[{i}].events[{j}] has unknown type {event.get('type')!r}, "
                    f"expected one of {sorted(KNOWN_EVENT_TYPES)}"
                )
            has_ms = "position_ms" in event
            has_pct = "position_pct" in event
            if has_ms == has_pct:
                raise ScriptValidationError(
                    f"{source}: tracks[{i}].events[{j}] must specify exactly one of "
                    "position_ms/position_pct"
                )
