# Stage 16 — Refresh Daemons and the Session API

Decision E's two halves, built together because neither is useful alone:
a delivery service with nothing keeping its data current serves stale
recommendations, and a refresh loop nobody can read from produces them
into the void.

| Component | Owns | Kafka group |
|---|---|---|
| `platform/streaming/session_consumer_daemon.py` | RAW state — `session:{id}:events`, the Postgres `events` log | `platform-session-consumer` |
| `platform/streaming/refresh_daemon.py` | DERIVED recommendations — `session:{id}:recs` | `platform-recs-refresh` |
| `platform/session_api/` | nothing — read-only delivery over Redis | — |

## Why two daemons when Decision E names one

Decision E names `refresh_daemon.py`. Running only that produces nothing
at all: `refresh_recommendations()` reads `session:{id}:events` and returns
`skip_insufficient_data` below two events, and the *only* writer of that
key is the stages 9-10 raw-state consumer — which also had no daemon
(stage 12 flagged it as "arguably stage 9-10's own gap", stage 15C flagged
it again, and `eval/8_2`'s harness papers over it by running both loops
itself).

So a real deployment needs both processes, and this stage ships both, as
separate modules in separate consumer groups per Decision C's ownership
split. Only the genuinely identical parts — signal handling and logging
setup — are shared, in `daemon_runtime.py`.

## These daemons commit offsets; nothing before them did

Every consumer written before stage 16 runs `enable.auto.commit: False`
and never commits. That is right for them: each is a bounded, one-shot or
test-scoped read under a throwaway group id, where "start from earliest"
is exactly what is wanted.

A daemon is the opposite case. It runs under the canonical group id from
`config.py`, it restarts, and `behavioral-events` is never purged — so
without commits every restart would replay the entire topic, pushing
duplicate events into `session:{id}:events` (`record_event` rpushes) and
duplicate rows into the Postgres log. Both daemons therefore commit
manually and synchronously, **after** the write, giving at-least-once
delivery.

No existing consumer's configuration changed. Commits are per group, and
every pre-stage-16 caller uses a throwaway group id, so none of them can
observe a daemon's committed offsets.

The corollary, measured below: a daemon's *first* start under a canonical
group id has no committed offset and does replay the whole retained topic
once. That is a one-time cost, not a per-restart one.

## The Session API computes nothing

`platform/session_api/main.py` serves what the refresh daemon has already
written and nothing else. Producing a recommendation on the read path
would put a Milvus search and two HTTP calls behind a GET and duplicate
the debounce the daemon exists to enforce.

**Redis is its only dependency** — no Postgres, Milvus or Neo4j. That
falls out of the data rather than being engineered: the rows
`recommendation_refresh` stores are already self-contained (`track_id`,
`title`, `artist_name`, `similarity`, `genre_sibling`, `same_artist`,
`score`), so nothing needs enriching at read time. It is also one of
Decision E's three grounds for the service existing separately at all.

| Endpoint | Returns |
|---|---|
| `GET /health` | liveness |
| `GET /sessions/{id}` | which state exists across all three keys, and why recommendations are missing if they are |
| `GET /sessions/{id}/recommendations` | the ranked list, verbatim |
| `GET /sessions/{id}/profile` | the centroid vector, its dimension, and `computed_at`/`n_events` |

**Request/response, not WebSocket**, per Decision E and deliberately
against what `thesis/06-streaming-use-cases.md` §6.1 still says — that
line is to be rewritten in a thesis session, not honoured in code. 8.2 vs
8.3 is *explicit query vs system-inferred suggestion*, not pull vs push.

### 404 distinguishes two very different situations

A wrong session id and a session whose first events have not yet cleared
the refresh debounce are not the same thing to a client, and collapsing
both into one 404 would make the second look like a bug. So the handlers
check `session:{id}:events` before answering: *"unknown session 'x'"*
versus *"session 'x' exists but has no recommendations yet"*.
`GET /sessions/{id}` goes further and says which of the three keys are
present, with a note explaining each absence.

## Two real bugs found by running a daemon for the first time

### `int()` on a free-form track id

A behavioral event's `track_id` is a string on an `extra="allow"` schema —
the ingestion service never validates it against the catalog — but
`refresh_recommendations()` indexed Postgres integer ids with a bare
`int()`. Every consumer before this stage was scoped to one session
(`expected_session_id`) or to one test's own payloads, so none had ever
read an arbitrary event off the topic. `refresh_daemon.py` reads **every**
session by design, and the first event it met with a non-numeric track id
— `"xyz789"`, left on the never-purged topic by stage 11's own test —
raised `ValueError` straight out of `refresh_recommendations()` and killed
the loop.

Fixed with `recommendation_refresh.as_track_id()`, which returns `None`
instead of raising, applied at all three call sites (played ids, the
re-rank anchor, the cold-start seed). A session whose *opening* track is
unparseable has no cold-start context at all, so that returns a new
`skip_unseedable_cold_start` action rather than guessing at a different
event.

That new action exposed a second-order problem: `process_one_event()`
decided whether real work happened with
`result["action"] != "skip_insufficient_data"`, so any *new* skip would
have counted as a refresh and spent the debounce budget. Now checked by
prefix — `not result["action"].startswith("skip_")`.

### A daemon must not die on one bad message

Even with the above fixed, a daemon that exits on a single unprocessable
event is not a daemon. Both loops now contain per-event failures: log,
count in `stats["errors"]`, commit past the message, continue.

Committing past a failed message is deliberate. The consumer has already
moved past it in memory, and leaving it uncommitted would make a poison
message replay forever on every restart and block the partition. The trade
is that a genuinely transient failure loses that one refresh — acceptable,
because the next event for the same session recomputes it from scratch
(recs are overwritten, never accumulated). The raw daemon additionally
rolls its Postgres connection back before continuing, or every later
`INSERT` would fail with "current transaction is aborted".

### A stats miscount, caught by an assertion rather than by reading

`refresh_daemon`'s counters originally had a specific
`skipped_insufficient_data` bucket and an `else` that meant "debounced" —
so `skip_unseedable_cold_start` would have been silently counted as a
debounce. The buckets now partition `processed` explicitly
(`refreshed` + `debounced` + `skipped`), with per-action counters beneath
them, and a test asserts the partition holds.

## The stage 16 finding deferred to 15D: derived state outlives raw state

`session:{id}:events` carries a 30-minute sliding TTL (stage 9). Nothing
ever expires `session:{id}:profile`, `:profile_meta`, `:recs` or
`:refresh_meta` — stage 13's `redis.set` and stage 14's `_write_recs` set
no TTL. So this service can serve recommendations for a session whose raw
state expired hours ago, and the derived keyspace grows without bound.

Surfaced rather than patched, by explicit decision: fixing it changes
stages 13 and 14, and **stage 15D's failure injection is the right place
to decide what a client should see when session state disappears
underneath it** — which is exactly the question 15D was sequenced after
this stage to be able to ask. `GET /sessions/{id}` reports it as
`raw_state_expired` with an explanatory note, so it is visible in the API
rather than only in a document.

## Not populating `contracts/`

`contracts/README.md` says the shared recommendation response shape gets
added "once 8.2 is a second independent consumer of them", and closes with
"Do not add anything else here speculatively." Stage 16 is the first time
that shape is actually *served*, but 8.3 — the second consumer Decision E
anticipates — does not exist yet. Freezing a `session-api-v1.json` now
would be speculative by that rule, so `contracts/` is untouched. Worth
revisiting when 8.3 starts.

## Results

Verified 2026-09-06 against the live stack, running the real deployment
shape: the Semantic API, the event ingestion service and the Session API
as three uvicorn processes, both daemons as two more, under the
**canonical group ids from `config.py`** (not throwaway test ids), with
stage 13's Flink job submitted for the second half.

### First start replays the retained topic once, then never again

`behavioral-events` held **1,820 messages** when the daemons first joined,
with no committed offset for either group.

| | Backlog handled | Time |
|---|---|---|
| `session_consumer_daemon` | 1,780 of 1,820 cached to Redis + Postgres | **7.5 s** |
| `refresh_daemon` | 571 refreshes | **~11 s** (16 ms per cold-start refresh) |

The 40-message shortfall is correct, not loss: the topic also carries
stage 7's plain-string test payloads and other messages with no
`session_id`, which the consumer skips by design.

**Zero errors and zero tracebacks** across both daemons over all 1,820
messages — the containment added this stage was never actually needed on
this data once `as_track_id()` was fixed, which is the intended outcome.

Both groups then sat at `current=1838 end=1838 lag=0`, and a real restart
of the raw daemon against the caught-up topic **replayed 0 events in 10
seconds** — the commit design doing its job. Without it, every restart
would have re-pushed all 1,780 events into Redis and Postgres.

### The delivery path, end to end

Simulated session driven through the real chain (simulator → ingestion →
Kafka → both daemons → Redis → Session API), first without the Flink job
and then with it:

Without the Flink job, `GET /sessions/{id}` returned 17 raw events
(`ttl_seconds: 1233`, the sliding TTL visibly counting down), 10
recommendations, no profile — and the note explaining exactly that: *"No
session profile yet: stage 13's Flink job has not fired a window for this
session, so any recommendations above came from the cold-start fallback
path."* The two 404s came back distinct, as designed: `unknown session
'does-not-exist'` versus `session 'sim-777-0' exists but has no profile
yet`.

With the job running, the same endpoint returned all three states present
and **an empty `notes` list** — 9 raw events, a **512-dimension** profile
with `n_events: 2` and its `computed_at`, and 10 recommendations.
`GET /profile` served the real centroid the Flink job had just written.

Recommendations came back ranked and self-contained, exactly as stored —
top row `track_id 12, score 0.6585, genre_sibling=true, same_artist=true`
— with no Postgres, Milvus or Neo4j touched on the read path.

## Verification

- **17 new tests**, all against the live stack: 8 in
  `tests/test_stage16_refresh_daemon.py`, 9 in
  `tests/test_stage16_session_api.py`. **197 pass repo-wide** (up from
  180).
- The daemon tests drive the real `run()` loops from worker threads with a
  bounded `max_events`, rather than reimplementing them — which is what
  `stop`/`max_events` exist for.
- The end-to-end test posts real events and asserts a populated ranked
  list comes back out of the API, with none of the played tracks in it.
- Regression tests cover both bugs: `as_track_id()` on unparseable ids,
  and a daemon surviving a session whose events carry non-catalog track
  ids.
- The manual run above exercised what tests cannot: the canonical group
  ids, offset commits across a genuine restart, and the backlog replay.



## Known gaps, deliberately left open

- **No process supervision.** Both daemons are plain foreground processes
  with cooperative shutdown; nothing restarts them. They are not in
  `docker-compose.yml`, matching the Semantic API and the event ingestion
  service, which have always run on the host.
- **The TTL asymmetry above**, deferred to 15D.
- **`eval/8_2`'s harness keeps its own refresh loop.** It is test-shaped
  on purpose — it records timings and snapshots a daemon has no business
  knowing about — so it was not switched over to `refresh_daemon.run()`.
