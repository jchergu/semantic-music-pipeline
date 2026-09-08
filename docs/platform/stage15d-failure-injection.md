# Stage 15D — Failure injection, and the TTL fix stage 16 deferred

Verified 2026-09-08. Two halves, sequenced deliberately: **measure first,
then fix**, so the fix's effect is observed rather than asserted.

Stage 15D was placed *after* stage 16 on purpose. Stages 13-15C.2 could only
ask what the pipeline computes; once `platform/session_api/` existed there was
finally a client, so this stage could ask the question the roadmap actually
wanted: **what does a client see when a store dies underneath a live
session?**

It also inherits stage 16's deferred finding — derived session state outliving
raw session state — and closes it.

## What this is not

Not built on `harness.run_scenario()`. That harness owns test-shaped threads
(its own raw-state consumer loop, its own driver loop over
`process_one_event()`), and `METRICS.md` §0 is explicit that those are not a
deployment. A failure-injection result measured against harness threads would
be a claim about the harness, and the client-visible half would have no client
in it at all.

`eval/8_2/failure_run.py` therefore drives the real stage 16 deployment: five
processes (Semantic API, event ingestion, `session_api`, and both daemons
**under the canonical consumer group ids from `streaming/config.py`**) plus
the stage 13 Flink job. Committed offsets and restart behaviour are part of
what is measured; a throwaway group id would replay the never-purged topic
from earliest and hide exactly that.

The `allowedLateness`/side-output decision that stages 15B and 15C.2 left open
to "15C/15D" **stays open**. Metric 7 exists to quantify the consequence of
the job having no lateness handling; adding it here would have destroyed the
effect that metric measures. It is future work, and the thesis says so.

## The spec came first, and is in git before the data

`METRICS.md` §9 and `metrics.failure_injection_verdict()` were committed in
`bbbde62`, **before any arm ran**, so the pre-registration is verifiable
rather than asserted. Four arms — a no-injection `control` plus `redis_outage`,
`semantic_api_outage` and `ttl_expiry` — two replicates each, on one fixed
scenario (the K=8 chillout→rock pivot metrics 6 and 7 already use, speed 60).

Two positions that section takes, both deliberate:

- **`pass` means the failure was measured soundly, not that the system behaved
  well.** An arm that loses events and exposes no staleness signal passes as a
  measurement while being a bad result for the system. Keeping the two apart
  is the point: the verdict says the measurement is sound, the finding says
  what it found.
- **Two replicates, where this project's noise floors use n≥5.** Those pool
  repeated runs because they estimate a ceiling on a *continuous* quantity. An
  event is in the Postgres log or it is not; a daemon resumes or it does not.
  Repetition here buys reproducibility, not a percentile. §9.5 states the
  deviation rather than leaving it to be noticed.

The injection window is indexed **by event, not wall-clock** (applied after
event 22, reverted at 28): a wall-clock window would cover a different number
of events each run, since posting is paced against a live stack, and
`events_lost` would not be comparable across arms.

## Two things the running of it found

### A suspended machine produced a perfect-looking, invalid arm

The first pre-fix pack was invalidated by something no measure in §9.4 would
have caught. The machine suspended overnight and froze one arm for twenty
hours between two of its posts. The arm **completed**: every event landed,
`events_lost` was 0, its four measures looked entirely ordinary, and it agreed
with its own replicate.

It was nonetheless not the specified experiment. The refresh debounce is
wall-clock while `--speed` compresses only session time, and
`session:{id}:events` carries a 30-minute TTL, so an arm frozen for hours
silently becomes a TTL-expiry arm regardless of which injection it was meant
to be testing.

`failure_injection.wall_clock_stall()` now compares, per post, the wall-clock
actually elapsed against the pacing the scenario intended, and flags any
overrun beyond 60s. **A flagged arm is excluded and re-run, never adjusted or
reweighted** — a measurement taken under conditions the spec did not describe
is a different experiment, not a weaker observation of this one. That pack was
discarded and the whole thing re-run; the published pack contains no stalled
arm (`wall_clock_stall_detected` is `false` in all eight records).

### A measure the control cannot have is not control-differenceable

`recovers_without_restart` is undefined for the control — with no injection
there is nothing to recover from, so it records `None`. The pre-registered
rule compared each arm's `True` against that `None` and reported "the system
recovered" as an **effect of the failure**, which is the opposite of what it
means. Corrected: such measures are listed under
`measures_not_comparable_to_control` and excluded from
`effects_beyond_control`, while still being reported and still required to be
stable across replicates.

This is a post-hoc correction to pre-registered code, so its direction matters
and §9.5 records it: it **removes a vacuous effect rather than creating one**,
makes no arm's result stronger, and both the pre- and post-correction outputs
are derivable from the same raw records via `--from-records`.

## Results (pre-fix, `--run-id 15dpre2`)

Both replicates agreed on every measure in every arm; no arm stalled.

| Arm | Events lost | Recovers w/o restart | Client can tell | Effects beyond control |
|---|---|---|---|---|
| `control` ×2 | 0 | — | no | — |
| `redis_outage` ×2 | **6** | yes | yes (500s, *during* only) | `events_lost`, `client_status_sequence`, `staleness_detectable` |
| `semantic_api_outage` ×2 | 0 | yes | **no** | **none** |
| `ttl_expiry` ×2 | 0 | yes | yes (`raw_state_expired`) | `staleness_detectable` |

### Redis outage: recovers cleanly, loses data permanently

Six events, posted and accepted by the ingestion service (so genuinely on
Kafka), never reached the Postgres log. Both daemons caught
`redis.ConnectionError` in their blanket per-event handler, counted an error,
**committed the offset past the message**, and continued.

That handler is right for a poison message — a daemon that dies on one bad
payload is not a daemon — but a dependency outage is not a poison message, and
the same policy makes every event arriving during the outage unrecoverable.
There is no retry and no dead-letter path, and because the offset advanced,
restarting the daemon does not bring them back.

The recovery half is good news: redis-py is pool-backed and reconnected on its
own, and both raw state and refreshes resumed with nothing restarted.

The outage also **took the Flink job down permanently**. Its window function
writes to Redis, and the compose cluster runs it with no checkpointing, so
Flink's restart strategy is "none". The harness resubmits between arms;
without that, every arm after the first `redis_outage` would have run with no
session profile at all and differed from the control for a reason having
nothing to do with its own injection.

### Semantic API outage: no measure distinguishes it from the control

This is the most consequential result in the pack, and it reads as a null.

Killing the Semantic API mid-session loses **no events** — the raw-state
daemon does not depend on it — and every client request returns 200 throughout:
`before`, `during`, `after`, `end`. `staleness_detectable` is `false`, the same
as the control.

What actually happens is that every refresh attempted during the outage fails
(`context_builder` is pure HTTP against that API), is counted as an error, and
has its offset committed. `session:{id}:recs` simply stops moving, and
`session_api` keeps serving the last value **as current**.

A client cannot tell, and the reason is structural: `:recs` carries no
timestamp of any kind. `:profile` has a sibling `:profile_meta` with
`computed_at` precisely so its freshness is inspectable; the recommendations
have no equivalent. So the system degrades invisibly — the failure is real,
the output is stale, and nothing in the API surface says so.

This is the §9.5 case the spec anticipated: the arm **passes** as a
measurement while being a bad result for the system.

### TTL expiry: the one failure the API already explains

Expiring `session:{id}:events` under a live session is visible: `GET
/sessions/{id}` reports `raw_state_expired: true` with an explanatory note,
which stage 16 added for exactly this. Refreshes resumed once the session
accumulated two fresh events again.

### The post-fix pack is identical, and that is expected

Re-running all eight arms against the fixed code (`--run-id 15dpost`) produced
**the same values on all four measures in all four arms**. That is not a
disappointment and it is not evidence the fix did nothing — it is a scope
limit of the metric, and worth stating so nobody reads the post-fix pack as
validation it cannot supply.

An arm runs about 90 seconds. `DERIVED_TTL_SECONDS` is 1800. No derived key
can expire inside one arm, so metric 8's four measures are structurally blind
to this fix. The fix is verified directly instead (see below and
`eval/8_2/tables.md`), and by six live unit tests in
`tests/test_stage15d_ttl.py`.

What the post-fix pack *does* establish is that the fix broke nothing: the
same failure behaviour, the same recovery, the same client-visible signals.

## The fix: derived state must not outlive raw state

`session:{id}:events` carried a 30-minute sliding TTL since stage 9. Nothing
ever expired `:profile`, `:profile_meta`, `:recs` or `:refresh_meta` — stage
13's `redis.set` and stage 14's `_write_recs` set none. The state was visible
in Redis before this stage ran: the 15C.2 sessions from the previous day still
had all four derived keys at `TTL = -1` with their `:events` long gone.

`streaming/config.py` now defines `DERIVED_TTL_SECONDS`, equal to
`SESSION_TTL_SECONDS`. One constant, not two: derived state is meaningless
without the session it derives from, Redis is a session cache and never a
system-of-record substitute, and the Postgres `events` log stays the durable
record, so nothing irreplaceable expires.

**What this does and does not guarantee.** It is tempting to say the
asymmetry now runs the other way — that derived state expires *before* raw
state — and that is not true. Raw events refresh their TTL on every event;
derived state refreshes its own only when a refresh fires or a Flink window
closes, and a window can close either side of the session's last event.

Measured on the post-fix pack, across 32 derived keys in 8 sessions:
derived-minus-raw TTL lands in **[-21s, +23s]**, with `:recs` and
`:refresh_meta` always at or before raw (they are written on event arrival)
and `:profile`/`:profile_meta` falling either side. So the honest claim is
that derived state now expires *within roughly half a minute* of the session
it belongs to — a bound set by the window and debounce cadence — rather than
never. `session_api` can no longer serve recommendations for a session it has
forgotten by more than that margin, and `GET /sessions/{id}` still reports the
transient via `raw_state_expired`.

Five changes:

- `config.py` — the constant. Also corrected a comment stage 16 had made false
  (it still said a continuously-running deployment of the raw consumer "doesn't
  exist yet").
- `session_state.py` — `refresh_meta_key()` and `expire_derived()`.
  `:refresh_meta` was the one session key with **no canonical definition**
  here; `recommendation_refresh.py` built the string inline. That is precisely
  how it became the one derived key nobody noticed had no TTL.
- `recommendation_refresh.py` — TTL in `_write_recs()`, and on the `hincrby`
  that *creates* `:refresh_meta`. The second placement matters: `hincrby`
  creates the key on a session's first event, so a session that never gets far
  enough to refresh would otherwise leave an immortal key behind — the same
  unbounded growth, in the one case a refresh never reaches.
- `flink_session_profile_job.py` — TTL on the `:profile` `set` and (as a second
  call, since `hset` takes no `ex=`) on `:profile_meta`. The constant is
  duplicated inline for the same reason the key strings already are: the job's
  Python UDF workers have no `platform/` on their path inside the container.
  Cross-checked by test, as the key strings are.
- `session_api/main.py` — `_missing()` now decides "known session" from **any**
  session key. Deciding it from `:events` alone made that function collapse the
  two cases in exactly the situation it exists to separate: once the raw list
  expired, a session with live recommendations or a live profile was reported
  as `unknown session`. The regression test was confirmed to fail against the
  old code and pass against the fix.

`track:{id}:embedding` and `:duration_sec` stay TTL-free deliberately: catalog
state, not session state.

Verified directly in Redis, which is the only place this fix is observable at
the timescale these runs occupy. Sessions written by the pre-fix code carry
`-1` on all four derived keys — Redis for "exists, never expires" — with
`:events` long since expired. Sessions written by the post-fix code carry a
TTL on all five.

## Verification

- **215 tests pass repo-wide** (197 before this stage, plus 12 pure metric-8
  tests and 6 live TTL tests).
- The pre-fix and post-fix packs are the measurement, run against the real
  five-process deployment.
- **Metrics 1-7 are unchanged by the fix.** `eval.8_2.run --from-records`
  reproduced `results.json`, `reactivity.json`, `coherence.json` and
  `coverage.json` **byte-identically** (md5-verified) against the fixed code.
  The live 25-minute harness re-run was deliberately skipped in favour of this:
  the fix touches keys those metrics read, and recomputation from the raw
  records answers exactly that question at zero live cost. Recorded here so
  the substitution is visible rather than silent.

## Known gaps, deliberately left open

- **No retry or dead-letter path.** The daemons' error handling cannot tell a
  poison message from a dependency outage, and treats both by discarding the
  event and committing past it. Fixing it properly means distinguishing the two
  — retry with backoff for a store that is down, commit-past for a payload that
  can never be processed — which is a design change beyond this stage's scope.
  It is now measured rather than suspected.
- **`:recs` has no freshness metadata**, so a stale recommendation set is
  indistinguishable from a current one. A sibling `:recs_meta` mirroring
  `:profile_meta` would close it.
- **Postgres, Milvus and Kafka outages are untested.** §9.6 records what
  reading the code predicts — psycopg2 does not self-heal, so a Postgres outage
  is predicted to break a daemon permanently; a Kafka outage is predicted to be
  invisible, since an empty poll is indistinguishable from idle traffic — and
  marks both explicitly as predictions, not results.
- **The Flink job still has no `allowedLateness`**, per the scope note above.
