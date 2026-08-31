# Stage 14 — Recommendation Refresh Loop

Session F of the roadmap. Closes the loop stage 13 opened: nothing read
`session:{id}:profile` before this. On each behavioral event, decides
(debounced) whether to refresh the session's recommendations, and if so,
writes `session:{id}:recs` — 8.2's actual recommendation output, for the
first time.

## Decision D: extracting `ranking.py` to `platform/`

`ranking.score_recommendations()` (the pure scoring function this stage
must reuse, not reimplement) lived inside
`usecases/8_1_batch_reactive/recommender/`, a use-case-owned directory.
CLAUDE.md's independence rule is *between* use cases, not between a use
case and the platform — Decision B already established that a future
session shouldn't refuse to extract shared components on independence
grounds. Moved verbatim to `platform/scoring/ranking.py` (`git mv`,
history preserved). Only two import sites needed fixing
(`usecases/8_1_batch_reactive/recommender/recommend.py`,
`usecases/8_1_batch_reactive/tests/test_uc81_recommender.py` — confirmed
by grep before moving); `context_builder.py`/`trigger_handler.py` stay
8.1-owned. Named `scoring`, not `recommender`, specifically to avoid a
top-level package name collision: `platform/`'s convention is each
subdirectory becomes its own top-level importable name once `platform/`
is on `sys.path` (`streaming`, `semantic_api`, `simulator`, ...) —
`platform/recommender/` would shadow
`usecases/8_1_batch_reactive/recommender/` (order-dependent, whichever is
added to `sys.path` first) since stage 14's cold-start path needs both
directories in the same process. All 13 of 8.1's own tests pass
unchanged after the move.

## Pipeline

`platform/streaming/recommendation_refresh.py` — an independent Kafka
consumer group (`RECS_REFRESH_GROUP_ID = "platform-recs-refresh"`,
`config.py`) on `behavioral-events`, alongside the stages-9-10 raw-state
consumer and stage 13's Flink job (Kafka's normal fan-out: every group
gets its own full copy of the stream). Never writes
`session:{id}:events` — reads it only, via
`session_state.get_session_events()`.

**Debounce**: at most once per 5 seconds OR once per 3 events, whichever
comes first — a `session:{id}:refresh_meta` Redis hash
(`last_refresh_ts`, `events_since_refresh`). The pure decision function
is `should_refresh()`. A refresh attempt that turns out to be a cold-start
no-op (fewer than 2 raw events) does **not** consume debounce budget —
only Milvus/HTTP calls need protecting from being hammered, and the
no-op guard makes neither.

**Cold start** (fewer than 2 raw events, or no `session:{id}:profile`
key yet — stage 13's 5-min/30s-slide windows need more than a couple of
events to fire meaningfully): falls back to exactly 8.1's own batch path
— `context_builder.build_context()` + `scoring.ranking.score_recommendations()`
— seeded by the session's first track. Logged distinctly
(`fallback=cold_start`).

**Warm path**: direct Milvus search using the profile vector (own alias
`recs-refresh`, per CLAUDE.md's Platform contracts rule; the Semantic API
has no "search by raw vector" endpoint — `/tracks/{id}/similar` only
takes a track id), `expr` excluding every track already played in the
session. `context_builder.build_context()` is reused for its
genre-sibling/same-artist HTTP calls only, seeded by the session's most
recent event's track (the "active context" anchor) — its own `similar`
result is discarded, replaced by the profile-vector search. Both
candidate lists are filtered against the played-tracks set again before
scoring (`context_builder`'s Neo4j lookups don't know about session play
history, so a played track could otherwise re-enter via the genre/artist
boost path even though the Milvus search already excluded it).
`scoring.ranking.score_recommendations()` merges and scores exactly as
8.1 does.

**Catalog exhaustion**: 411 tracks is a small catalog: a long session can
exhaust novel candidates. Logged explicitly (`recs_short=true`, actual
count) when fewer than 10 survive exclusion — not padded, not hidden.

**Race with the raw-state consumer**: no ordering guarantee that it has
already cached the *current* event by the time this consumer processes
the same Kafka message. `merge_current_event()` appends the in-hand
current event locally if `get_session_events()` doesn't already reflect
it, rather than waiting.

**Profile staleness**: inherent, not a bug. Stage 13's profile lags up to
~30s+ (window slide) behind the newest events; this stage's 5s/3-event
debounce runs on its own, independent cadence.

## A real bug found while testing, not assumed away

`process_one_event()`'s first version created a fresh `Consumer` on every
call. Since offsets are never committed (`enable.auto.commit: False`,
same as `session_consumer.py`), a fresh Consumer always rescans from
"earliest" and returns the same first matching message again — **the
same bug class stage 12/Decision C's `consume_and_cache_many()` already
fixed for the raw-state consumer**, but this time it wasn't just a test
artifact: a real driver calling `process_one_event()` in a loop (its
whole intended usage — "on each behavioral event") would hit it too,
permanently stuck reprocessing the first event for a given session. Fixed
by adding `new_consumer()` and an optional `consumer` parameter: a real
driver creates one Consumer and reuses it across calls (same fix shape as
`consume_and_cache_many` — one Consumer for a whole sequence of reads,
not one per read); omitting it still works for a single isolated call.

A second, milder bug (test-only): the initial debounce test assumed all
five simulated events would process within the 5-second interval
threshold, so behavior would be governed entirely by the event-count
branch. Real sequential Milvus/Postgres/HTTP/Kafka-consumer-creation
calls take enough real wall-clock time that the interval branch fired
before the count branch did in the original (pre-Consumer-reuse) version
of the test. Fixing the Consumer-reuse bug incidentally resolved this too
(fewer Consumer creations per test run), but it's worth noting as a
reminder that "these calls are fast" is an assumption to verify, not
assume, when timing thresholds are involved.

## Verification

`tests/test_stage14_recommendation_refresh.py`: 13 tests — 11 pure
(`should_refresh`, `determine_context`, `merge_current_event`, no live
services) plus 2 live against the real stack:

- **Exit criterion**
  (`test_recs_visibly_change_after_a_skip_and_stay_stable_on_a_debounced_noop`):
  replays the stage 12 simulator's `three_early_skips.yaml` script
  event-by-event. `session:{id}:recs` is absent after event 1
  (insufficient data), appears after event 2 (a skip — the first real
  refresh), stays byte-identical after event 3 (a debounced no-op), and
  the debounce mechanism correctly re-arms and runs again after event 5
  crosses the 3-event count threshold (content happens to be identical to
  event 2's, since cold-start's seed — the session's first track — hasn't
  changed; this is documented as expected, not asserted away).
- **Warm path** (`test_warm_path_excludes_already_played_tracks`): seeds
  a fake `session:{id}:profile` directly (a real track's own embedding,
  standing in for stage 13's output — which needs real wall-clock window
  time to materialize and isn't worth blocking a fast test on) and
  confirms the returned recommendations never include an already-played
  track.

109/109 tests pass repo-wide (13 new).
