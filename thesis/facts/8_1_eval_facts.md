# 8.1 Evaluation Facts — frozen extraction for §5.5

**Produced:** 2026-09-08, Session 1 of the Chapter 5 gap plan. **Read-only session**:
nothing under `eval/`, `reports/` or `thesis/` was modified; this file is the only
artifact written.

**Purpose.** Every number that §5.5 (and any Chapter 5 figure) may cite. Numbers are
copied from artifacts, not recomputed and not re-rounded, except where a row says
otherwise and shows its command. Every row carries a source path. Confidence tags:
`[Certain]` = read out of a live artifact; `[Likely]` = reconstructed from code or
commit history; `[Guessing]` = not written here, escalated instead (see §7).

---

## 0. The provenance fork — read this before using any number

`eval/8_1/` contains **two complete result packs**, and they are not two runs of the
same thing. Getting this wrong is the single easiest way to put a wrong number in the
thesis.

| Pack | File | Generated against | Genre query | Status |
|---|---|---|---|---|
| **Canonical** | `eval/8_1/results.json` | `eval/8_1/regenerated_recommendations.json` (reconstruction, validated bit-exact against the real recommender) | Stage 15A **ordered** `LIMIT 10` | cite this |
| **Legacy** | `eval/8_1/frozen_legacy_results.json` | the frozen Postgres table, `run_id 3a7ffa23-c5ac-48ec-86f8-65412658be8b` | original **unordered** `LIMIT 10` | provenance only |

- Legacy provenance string, verbatim: *"LEGACY -- generated 2026-08-31/2026-09-01
  against the frozen 4110-row recommendations table (run_id 3a7ffa23-...) under the
  ORIGINAL related_by_genre Cypher query: LIMIT 10 with NO ORDER BY (arbitrary
  candidate selection). Superseded 2026-09-01 (Stage 15A.2) by eval/8_1/results.json
  … Retained for provenance only -- do not cite these numbers as current."*
  — `eval/8_1/frozen_legacy_results.json::_provenance` `[Certain]`
- The live Postgres `recommendations` table today holds **only** the frozen run:
  one `run_id`, `3a7ffa23-c5ac-48ec-86f8-65412658be8b`, 4,110 rows. Measured
  2026-09-08 via `select run_id, count(*) from recommendations group by run_id`.
  `[Certain]` — so the canonical pack is *not* readable back out of Postgres; its
  input is the checked-in `regenerated_recommendations.json`.
- Both packs cover 411 seeds × 10 = **4,110 rows**.
  (`results.json::signal_contribution.total_rows`, same key in the legacy file.)
  `[Certain]`
- A third, older pack exists under `reports/` (`reports/uc81/results.json`,
  `reports/results_evaluation/results.json`, both last written 2026-08-28, commit
  `9afca0c`). **It is pre-fix.** Its `genre_overlap_rows = 2200` matches the *legacy*
  eval number exactly (canonical is 2,184), which is the tell. `[Certain]`

---

## 1. Metric inventory and pre-registered rules

Metric numbering as it appears in `eval/8_1/run.py` lines 15–16 and `eval/8_1/README.md`
("Outputs"). `[Certain]`

| # | Metric | Computed by | Output file |
|---|---|---|---|
| 1 | Signal contribution | `metrics.reconstruct_signals` + `metrics.signal_contribution_summary` | `results.json::signal_contribution` |
| 2 | Catalog coverage (+ Gini) | `metrics.catalog_coverage` | `results.json::catalog_coverage` |
| 3 | Intra-list diversity | `metrics.intra_list_diversity` | `results.json::intra_list_diversity` |
| 4 | Latency per stage | `eval/8_1/latency.py` | `latency.json` (separate file, by design) |
| 5 | KG connectivity | `eval/8_1/kg_connectivity.py` | `results.json::kg_connectivity` |
| 6 | Failure / edge cases | `metrics.failure_edge_cases` | `results.json::failure_edge_cases` |

### 1.1 There is no `METRICS.md` for 8.1 — ESCALATION

`find . -iname 'METRICS*'` returns `eval/8_2/METRICS.md` and nothing equivalent under
`eval/8_1/`. A repo-wide grep for `pre-regist`/`preregist`/`pass/fail`/`verdict`
across `eval/8_1`, `docs/`, `usecases/`, `thesis/`, `reports/` finds pre-registration
language **only** in `eval/8_1/diff_runs.py`. `[Certain]`

**Therefore: metrics 1–6 above are marked `NO PRE-REGISTERED RULE`.** They are
descriptive measurements with no threshold and no PASS/FAIL verdict, and none was ever
written down. §5.5 must not state or imply a verdict for any of them. This is a real
asymmetry with Chapter 6 — 8.2 pre-registered all eight of its rules — and §5.5 should
say so plainly rather than let the reader assume parity. See §7, item E1.

### 1.2 The one pre-registered rule in 8.1 scope

Applies to the Stage 15A.2 reconstruction-path validation, **not** to metrics 1–6.
Verbatim from `eval/8_1/diff_runs.py:203-212` `[Certain]`:

> Pre-registered PASS/FAIL rule -- fixed here, before Task 2's numbers exist, so it
> cannot be tuned after seeing them.
>
> PASS iff:
>   - path_validation's membership_changed seed count does not exceed the worst
>     membership_changed count observed in the noise floor, AND
>   - path_validation's max score delta does not exceed the noise floor's max score
>     delta.

**Verdict: PASS** (both checks). Source `eval/8_1/path_validation.json::verdict`:
`pass = True`; `membership_check` observed 0 vs ceiling 0, `pass = True`;
`delta_check` observed 0.0 vs ceiling 0.0, `pass = True`. `[Certain]`

**Caveat on verifiability.** `diff_runs.py` and `path_validation.json` were committed
**together** (`f69f419` / `a56a44c`, both 2026-09-01), so unlike 8.2's metric 8 — whose
rule landed in its own earlier commit `bbbde62` — the pre-registration here rests on
the docstring's assertion, not on commit ordering. `[Likely]` (commit dates are certain;
the inference about what that does to verifiability is mine). If §5.5 claims
pre-registration for this rule, it should claim it at that strength and no higher.

---

## 2. Metric-by-metric results — CANONICAL pack

All rows below: source file `eval/8_1/results.json` unless stated. Rule column is
`NO PRE-REGISTERED RULE` throughout, per §1.1. All `[Certain]`.

### Metric 1 — Signal contribution (`results.json::signal_contribution`)

| Quantity | Value | Key |
|---|---|---|
| Total rows | 4110 | `.total_rows` |
| similarity only | 3055 (74.33%) | `.combination_counts.similarity_only` / `.combination_pct.similarity_only` |
| genre only | 440 (10.71%) | `.combination_counts.genre_only` / `.combination_pct.genre_only` |
| artist only | 454 (11.05%) | `.combination_counts.artist_only` / `.combination_pct.artist_only` |
| genre **and** artist | 161 (3.92%) | `.combination_counts.genre_and_artist` / `.combination_pct.genre_and_artist` |
| rows with genre sibling | 14.62% | `.pct_genre_sibling` |
| rows with same artist | 14.96% | `.pct_same_artist` |
| rows in the similarity pool | 100.0% | `.pct_in_similarity_pool` |
| reconstruction-inconsistent rows | 0 (0.0%) | `.reconstruction_inconsistent_count` / `_pct` |

The four combination counts sum to 4110 exactly (3055+440+454+161) — checked. They are
mutually exclusive and exhaustive, which is what makes them safe to plot as a partition.

**Genre-boost coverage gap** (`.genre_boost_coverage_gap`):

| Quantity | Value | Key |
|---|---|---|
| rows sharing a genre tag with their seed | 2184 | `.genre_tag_overlap_rows` |
| of those, never given `GENRE_BOOST` | 1583 | `.missed_by_cap_count` |
| as a percentage | 72.48% | `.missed_by_cap_pct_of_tag_overlap` |

Note field, verbatim: *"genre_sibling is the exact capped related_by_genre set (LIMIT
10, ordered by shared-genre-count descending as of Stage 15A) the batch run actually
applied GENRE_BOOST from; genre_tag_overlap is the complete tracks.genre_tags overlap
fact. This counts rows where the two disagree -- a real, confirmed pipeline behavior
(large genres still lose most of their true siblings to the cap itself, independent of
ordering -- the Stage 15A fix changed which 10 get selected, not how many can fit),
not a reconstruction error. Unaddressed: raising the cap is a separate, larger change
than this fix and was out of scope for it."*

### Metric 2 — Catalog coverage (`results.json::catalog_coverage`)

| Quantity | Value | Key |
|---|---|---|
| Total tracks | 411 | `.total_tracks` |
| Covered tracks | 400 | `.covered_tracks` |
| Percent covered | 97.32 | `.pct_covered` |
| Gini coefficient | 0.3898 | `.gini_coefficient` |
| Max appearances (one track) | 41 | `.max_appearances` |
| Min appearances | 0 | `.min_appearances` |
| Per-track appearance counts | 411 entries | `.appearance_counts_by_track_id` (dict, track_id → count) |

`appearance_counts_by_track_id` is the data any long-tail / concentration figure plots.
It is present in **both** packs under the same key, which is what makes a before/after
figure possible without recomputation.

### Metric 3 — Intra-list diversity (`results.json::intra_list_diversity`)

Mean pairwise CLAP cosine *distance* within each seed's top-10.

| Quantity | Value | Key |
|---|---|---|
| Mean | 0.2569 | `.mean` |
| Median | 0.2349 | `.median` |
| p10 | 0.1447 | `.p10` |
| p90 | 0.4046 | `.p90` |
| Seeds evaluated | 411 | `.seeds_evaluated` |
| Seeds skipped (<2 recs) | 0 | `.seeds_skipped_lt_2_recs` |
| Per-seed values | 411 entries | `.per_seed` |

### Metric 4 — Latency per stage (`eval/8_1/latency.json`)

**Not deterministic and not part of the reproducibility claim** — the pack keeps it in
its own file precisely so `results.json`'s byte-identical-across-runs claim holds
literally (`eval/8_1/README.md`, "Outputs").

| Stage | p50 (ms) | p95 (ms) | mean (ms) | Key |
|---|---|---|---|---|
| Milvus ANN | 3.7 | 5.773 | 4.188 | `.summary.milvus_ms.*` |
| Neo4j | 4.177 | 7.777 | 5.299 | `.summary.neo4j_ms.*` |
| Ranking | 0.22 | 0.499 | 0.224 | `.summary.ranking_ms.*` |
| Total | 8.125 | 13.552 | 9.712 | `.summary.total_ms.*` |

Seeds measured: 411 (`.seeds_measured`).

Note field, verbatim: *"Measured fresh against the current environment. The original
stage-5 batch run's ~8.8s/411-seed wall-clock (CLAUDE.md) was never broken down by
stage, so this is not a decomposition of that historical figure."*

**Provenance caveat.** These timings were taken during the **legacy/frozen** pass.
`eval/8_1/frozen_legacy_tables.md` carries them under a full "## Latency" table, while
the canonical `eval/8_1/tables.md` says *"Not measured for this pass -- the ranking
pipeline's per-stage latency doesn't depend on which recommendations table is being
evaluated; see the frozen table's tables.md for a fresh reading."* `latency.json` on
disk holds the numbers above. `[Certain]` on all three readings. So §5.5 may cite them
as a per-stage latency measurement of the pipeline, but **not** as "the canonical
pack's latency" — the canonical pack deliberately did not re-measure it.

### Metric 5 — KG connectivity (`results.json::kg_connectivity`)

| Quantity | Value | Key |
|---|---|---|
| Tracks / Artists / Genres | 411 / 201 / 77 | `.total_tracks` / `.total_artists` / `.total_genres` |
| Track→Genre out-degree | mean 1.859, median 2, min 0, max 3 | `.track_genre_out_degree.*` |
| Track→Artist in-degree | mean 1.0, median 1, min 1, max 1 | `.track_artist_in_degree.*` |
| Artist→Track out-degree | mean 2.045, median 1, min 1, max 20 | `.artist_track_out_degree.*` |
| Genre→Track in-degree | mean 9.922, median 3, min 1, max 185 | `.genre_track_in_degree.*` |
| Seeds with zero genre siblings | 59 | `.zero_genre_sibling_seed_count` |
| …their ids | 59 entries | `.zero_genre_sibling_seed_ids` |

The genre in-degree mean 9.922 vs median 3 (max 185) is the skew that makes the
`LIMIT 10` cap bite; it is the same distribution fact the coverage-gap note refers to.

### Metric 6 — Failure / edge cases (`results.json::failure_edge_cases`)

| Quantity | Value | Key |
|---|---|---|
| Seeds with fewer than 10 recommendations | 0 | `.count` |
| Expected top-k | 10 | `.expected_top_k` |

---

## 3. `related_by_genre` — before / after, both packs

Every pair below is the **same key** read out of two files, so the comparison needs no
recomputation. `[Certain]` throughout.

| Quantity | BEFORE (`frozen_legacy_results.json`) | AFTER (`results.json`) | Key |
|---|---|---|---|
| Max appearances | **67** | **41** | `catalog_coverage.max_appearances` |
| Gini coefficient | **0.3942** | **0.3898** | `catalog_coverage.gini_coefficient` |
| Covered tracks | 399 | 400 | `catalog_coverage.covered_tracks` |
| Percent covered | 97.08 | 97.32 | `catalog_coverage.pct_covered` |
| Min appearances | 0 | 0 | `catalog_coverage.min_appearances` |
| similarity only | 3025 (73.60%) | 3055 (74.33%) | `signal_contribution.combination_counts/pct.similarity_only` |
| genre only | 470 (11.44%) | 440 (10.71%) | `…genre_only` |
| artist only | 485 (11.80%) | 454 (11.05%) | `…artist_only` |
| genre and artist | 130 (3.16%) | 161 (3.92%) | `…genre_and_artist` |
| pct genre sibling | 14.60 | 14.62 | `signal_contribution.pct_genre_sibling` |
| pct same artist | 14.96 | 14.96 | `signal_contribution.pct_same_artist` |
| genre-tag-overlap rows | 2200 | 2184 | `signal_contribution.genre_boost_coverage_gap.genre_tag_overlap_rows` |
| missed by the cap | 1600 (72.73%) | 1583 (72.48%) | `…missed_by_cap_count` / `…_pct_of_tag_overlap` |
| Intra-list diversity mean | 0.2562 | 0.2569 | `intra_list_diversity.mean` |
| Reconstruction-inconsistent rows | 0 | 0 | `signal_contribution.reconstruction_inconsistent_count` |
| KG connectivity (all keys) | identical | identical | `kg_connectivity.*` |
| Seeds with <10 recs | 0 | 0 | `failure_edge_cases.count` |

**How to state this honestly.** Max appearances drops 67 → 41, a 38.8% fall in the
worst single-track concentration. Gini moves 0.3942 → 0.3898, a change of 0.0044 —
about 1.1% of the pre-fix value. The two numbers describe the same fix and disagree
about how big it is, because Gini is a whole-distribution statistic and the fix hit the
head hardest. The coverage gap barely moves (72.73% → 72.48%), which is the point:
ordering changed *which* ten siblings are selected, not *how many can fit*, so the cap
remains an open limitation. Do not present the Gini shift as the headline.

**Row-level scope of the change** (`eval/8_1/regeneration_diff.json`) `[Certain]`:

| Quantity | Value | Key |
|---|---|---|
| Total seeds compared | 411 | `.total_seeds` |
| Total rows compared | 4110 | `.total_rows_compared` |
| Differing rows | 1741 | `.differing_rows` |
| Seeds: membership changed | 217 | `.classification_counts.membership_changed` |
| Seeds: order only | 60 | `.classification_counts.order_only` |
| Seeds: identical | 134 | `.classification_counts.identical` |
| "Affected" seeds (membership+order) | 277 | `.affected_seed_ids` (length) |
| Identical seeds that still had row diffs | 11 | `.identical_but_row_diffs_seed_ids` (length) |

Those 11 are explained in `.identical_but_row_diffs_note`, verbatim: *"Seeds with
unchanged track_id order/membership but row_diffs > 0 -- verified (see module
docstring) to be exactly +-GENRE_BOOST (0.05) on one candidate whose genre-sibling
status flipped between the pre- and post-Stage-15A query without changing its rank.
Not float precision noise, not ANN non-determinism -- a real, smaller-magnitude effect
the track_id-based classification above doesn't count as 'affected'."*

---

## 4. Noise floor / determinism — the §6.1.9 forward reference

**The claim §6.1.9 makes is supported by an artifact.** `thesis/06-streaming-use-cases.md:199`
(§6.1.9, Metric 6) says the 8.1 evaluation measured *"this scoring path's repeated-run
noise floor at exactly zero across 41,100 matched pairs."* Source
`eval/8_1/noise_floor.json` `[Certain]`:

| Quantity | Value | Key |
|---|---|---|
| Runs compared | 5 | `.run_ids` (5 uuids) |
| Pairs | 10 | `.pair_count` |
| **Pooled matched score deltas** | **41100** | `.pooled_score_delta_stats.count` |
| Pooled max score delta | **0.0** | `.pooled_score_delta_stats.max` |
| Pooled mean / p50 / p99 delta | 0.0 / 0.0 / 0.0 | `.pooled_score_delta_stats.*` |
| Max membership-changed seeds, any pair | 0 | `.max_membership_changed_seed_count` |
| Max order-only seeds, any pair | 0 | `.max_order_only_seed_count` |
| Max score delta, any pair | 0.0 | `.max_score_delta` |
| Per-pair `identical` seed count | 411 in all 10 pairs | `.pairs.*.classification_counts.identical` |

10 pairs × 4110 rows = 41,100 — the arithmetic checks out.

**The bound the artifact itself puts on the claim**, verbatim from
`.milvus_index_note`: *"IVF_FLAT is an approximate index (a true nearest neighbor could
be missed if it falls in an unprobed cluster -- nprobe=16 of nlist=128, see
platform/semantic_api/main.py), not exhaustive/FLAT search. Cluster assignment is fixed
at index-build time; this measurement never rebuilt the index between the 5 runs, so
the 0.0 noise floor reflects run-to-run repeatability against a STATIC index, not a
claim that Milvus search is unconditionally deterministic. Recall relative to true
exhaustive search was not measured."*

Index facts: `IVF_FLAT`, metric `COSINE`, `nlist = 128`
(`.milvus_index_info.embedding.*`). `[Certain]`

§5.5 must carry that bound. The measured quantity is *run-to-run repeatability against
an unrebuilt index*, not determinism of Milvus, and not recall.

**Path validation** (`eval/8_1/path_validation.json`), the thing the §1.2 rule judges:
anchor run `1c7cbcf7-f4a9-4194-98c2-48c6438c3cb8`; 411 seeds `identical`, 0
`membership_changed`, 0 `order_only`; 4110 score deltas, max/mean/p50/p99 all `0.0`;
`verdict.pass = True`. `[Certain]`

---

## 5. Score distribution — needed for the planned Figure 5.x, WITH A CONFLICT

The plan's Session 2 asks for a score-distribution figure over the 4,110 rows citing
"range 0.181–1.179, mean 0.794". Those numbers exist, but they are **pre-fix**.

| Source | n | min | max | mean | Provenance |
|---|---|---|---|---|---|
| `reports/uc81/results.json::score_distribution` | 4110 | **0.1809** | **1.1789** | **0.794** | frozen/pre-fix table, commit `9afca0c` 2026-08-28 |
| `eval/8_1/regenerated_recommendations.json` | 4110 | **0.1790** | **1.1789** | **0.7941** | canonical/post-fix reconstruction |

`[Certain]` for the first row (read directly out of the JSON).
`[Certain]` for the second: computed 2026-09-08 from the checked-in file with
`min()`/`max()`/`mean` over the 4,110 `score` values —
`python3 -c "import json;s=[r['score'] for r in json.load(open('eval/8_1/regenerated_recommendations.json'))];print(len(s),min(s),max(s),sum(s)/len(s))"`
→ `4110 0.17900459468364716 1.1789000034332275 0.7940608...`. This is a computation over
a frozen artifact, not a re-run of anything.

Supporting data available for the figure:
- Full sorted score list, pre-fix: `reports/uc81/results.json::score_distribution.scores`
  (4,110 values). `[Certain]`
- Pre-binned histogram, pre-fix: `reports/uc81/histogram.json` — `lo = 0.1809`,
  `hi = 1.1789`, `bin_width = 0.0384`, `counts` = 26 bins. `[Certain]`
- Full per-row scores, post-fix: `eval/8_1/regenerated_recommendations.json`, one
  object per row with `seed_track_id` / `recommended_track_id` / `rank` / `score`.
  `[Certain]`
- An existing pre-fix figure already exists: `reports/uc81/score_distribution.png`.

**`thesis/05-implementation-uc81.md:87` currently cites the pre-fix numbers**
("scores ranging from 0.181 to 1.179 (mean 0.794)"). See §7, item E3.

---

## 6. Test counts — measured, not cited

Command, run 2026-09-08 against the live stack (all 11 containers healthy per
`docker compose ps`):

```bash
platform/enrichment/.venv/bin/python -m pytest -q
```

**Result: `215 passed, 19 warnings in 75.53s`.** `[Certain]`

Per-file counts, from
`platform/enrichment/.venv/bin/python -m pytest --collect-only -q` `[Certain]`:

| File | Tests |
|---|---|
| `tests/test_stage2_ingestion.py` | 5 |
| `tests/test_stage3_enrichment.py` | 6 |
| `tests/test_stage4_api.py` | 9 |
| `usecases/8_1_batch_reactive/tests/test_uc81_recommender.py` | 8 |
| `usecases/8_1_batch_reactive/tests/test_uc81_e2e.py` | 5 |
| `eval/8_1/tests/test_metrics.py` | 18 |
| `eval/8_1/tests/test_run_smoke.py` | 2 |
| `tests/test_stage7_kafka.py` | 3 |
| `tests/test_stage8_flink.py` | 2 |
| `tests/test_stage9_redis.py` | 3 |
| `tests/test_stage10_postgres_events.py` | 5 |
| `tests/test_stage11_event_ingestion.py` | 3 |
| `tests/test_stage12_event_simulator.py` | 15 |
| `tests/test_stage13_session_profile.py` | 16 |
| `tests/test_stage14_recommendation_refresh.py` | 13 |
| `tests/test_stage15b_jitter.py` | 6 |
| `tests/test_stage15d_ttl.py` | 6 |
| `tests/test_stage16_refresh_daemon.py` | 8 |
| `tests/test_stage16_session_api.py` | 9 |
| `eval/8_2/tests/test_82_metrics.py` | 18 |
| `eval/8_2/tests/test_82_metrics_15c2.py` | 26 |
| `eval/8_2/tests/test_82_run_smoke.py` | 10 |
| `eval/8_2/tests/test_82_scenario_gen.py` | 7 |
| `eval/8_2/tests/test_82_failure_injection.py` | 12 |
| **Total** | **215** |

### 6.1 The 32 / 162 / 215 reconciliation — it closes exactly

Derived by summing the measured per-file counts above. `[Certain]` on every count;
`[Likely]` on the *labelling* of which files constitute each subset (the labels follow
the build-order structure in CLAUDE.md; no artifact declares them).

| Subset | Files | Tests |
|---|---|---|
| **8.1 build stages 2–6** (what Ch5's "32/32" refers to) | stage2 5, stage3 6, stage4 9, uc81_recommender 8, uc81_e2e 5 | **33** |
| **8.1 evaluation pack** | eval/8_1 18 + 2 | **20** |
| **8.1 total** | the two rows above | **53** |
| **Streaming** (what Ch6's "162" refers to) | stages 7–16 (3+2+3+5+3+15+16+13+6+6+8+9 = 89) + eval/8_2 (73) | **162** |
| **Repo total** | 53 + 162 | **215** |

**So `215 − 162 = 53` is correct and fully accounted for**, and the plan's "≠ 32" is
right for the reason below.

**Why Chapter 5 says 32 and the suite now says 33.** `tests/test_stage4_api.py` holds
9 tests today; it held 8 when stage 6 closed. Stage 15A added
`test_track_graph_related_by_genre_ordered_by_shared_genre_count` — the regression test
for the very `LIMIT 10 … no ORDER BY` fix §5.5 is going to report. `[Certain]` that the
file has 9 tests and that CLAUDE.md's stage-4 entry records 8 at the time; `[Likely]`
that this one added test is the whole of the 32→33 delta (no other stage-2–6 file
changed count, but I did not run the suite at the stage-6 commit to prove it).

The remaining 20 of the 53 are the `eval/8_1` pack's own tests, which did not exist
when the "32/32" sentence was written.

Chapter 5's two claims to reconcile in Session 5:
- `thesis/05-implementation-uc81.md:99` — *"Final verification state: 32 of 32 tests
  passing across all six build stages, over the full 411-track dataset."*
- `thesis/05-implementation-uc81.md:119` — *"32/32 tests pass across the whole suite."*
  This one is the weaker of the two: it was true when written and is now false on its
  face, since the whole suite is 215.

Chapter 6's claim, which needs no change:
- `thesis/06-streaming-use-cases.md:282` — *"Of the 215 automated tests in the
  repository, all of which pass, 162 belong to the streaming stages and their
  evaluation packs."* Both numbers confirmed. `[Certain]`

---

## 7. Escalations — decisions for the owner, not for the next session

**E1 — 8.1 has no pre-registered rules, and §5.5 must not imply it does.**
Metrics 1–6 are descriptive; no thresholds, no verdicts, no `METRICS.md`. The only
pre-registered rule in 8.1 scope is `diff_runs.verdict()`, which judges the
reconstruction path, not the recommender's quality. Chapter 6 leans hard on
pre-registration as a methodological virtue; Chapter 5 cannot claim the same and should
say why (8.1's pack was written after the fact against an already-frozen table, so
there was nothing to pre-register *against*). **Owner decision: is that framing
acceptable, or should §5.5 avoid the comparison entirely?**

**E2 — the pre-registration of `diff_runs.verdict()` is docstring-asserted, not
commit-ordered.** Rule and result landed in the same commits (`f69f419`, `a56a44c`),
unlike 8.2's metric 8 (`bbbde62`, before the run). Truthful phrasing is "the rule is
recorded in the script that applies it, fixed before the compared numbers were
produced" — not "verifiably pre-registered in the commit history". **Owner decision:
does §5.5 mention this rule at all, given the weaker guarantee?**

**E3 — the score range Chapter 5 already prints is pre-fix.**
`thesis/05-implementation-uc81.md:87` says 0.181–1.179, mean 0.794. That is
`reports/uc81/results.json` — the frozen, pre-Stage-15A table. The canonical post-fix
figures are min 0.1790, max 1.1789, mean 0.7941. The max is unchanged; the min shifts
in the third decimal; the mean shifts in the fourth. **Owner decision, three options:**
(a) update §5 to the canonical numbers and build the figure from
`regenerated_recommendations.json`; (b) keep the pre-fix numbers and label them
explicitly as describing the frozen batch run that §5 is actually narrating; (c) print
both. Option (b) is arguably the most honest for a chapter that *describes the original
batch run* — but it must then say so, because §5.5 will be reporting canonical numbers
two pages later, and an unlabelled 0.181 next to an unlabelled 0.1790 reads as an error.
**This is the one item that must be settled before Session 2 draws anything.**

**E4 — latency belongs to the legacy pass.** `latency.json` holds real measurements;
the canonical `tables.md` explicitly declines to re-measure. Citing them is fine;
citing them *as the canonical pack's* is not. No decision needed if §5.5 phrases it as
"measured fresh against the live stack over 411 seeds" and omits the pack attribution.

**E5 — nothing in the 8.1 pack supports a quality or accuracy claim.** No ground truth,
no users, no labels, and the pack says so itself. Any sentence in §5.5 of the form "the
recommendations are good/reasonable/coherent" has no artifact behind it. The adjacent
`reports/results_evaluation/results.json` (same-artist rates, genre-overlap rates,
catalog skew, an 8-seed spot check) is descriptive too — and pre-fix. Not escalated as
a conflict, just fenced off.

---

## 8. Data available to Session 2's figures — all present, no recomputation needed

| Planned figure | Data | Path |
|---|---|---|
| Score distribution | 4,110 sorted scores (pre-fix) **or** 4,110 per-row scores (post-fix) | `reports/uc81/results.json::score_distribution.scores` / `eval/8_1/regenerated_recommendations.json` — **blocked on E3** |
| Popularity concentration, before vs after | two 411-entry appearance maps, plus max/Gini for both | `frozen_legacy_results.json` and `results.json`, `catalog_coverage.appearance_counts_by_track_id` + `.max_appearances` + `.gini_coefficient` |
| Candidate-source contribution | four mutually exclusive counts summing to 4110 | `results.json::signal_contribution.combination_counts` (legacy equivalents available for a before/after variant) |

Chapter 6's figure conventions, for matching: generator
`thesis/figures/make_82_diagrams.py` (matplotlib), outputs `thesis/figures/*.png`;
the evaluation-pack figures live at `eval/8_2/figures/*.png` and are referenced from
the thesis in place as `../eval/8_2/figures/…` at `{width=6.0in}`, which resolves
because `make` runs pandoc from `thesis/`. `[Certain]` — `thesis/Makefile`,
`thesis/06-streaming-use-cases.md:141`. Note that `eval/8_1/figures/` already contains
`coverage_long_tail.png`, `diversity_histogram.png`, `latency_breakdown.png` (canonical)
and a `frozen_legacy/` subdirectory of the same three — Session 2 should check whether
referencing those in place is preferable to generating new ones, exactly as §6.1 does.

---

## 9. Chapter 6's three forward references into 8.1 — all located

| Line | Subsection | What it asserts | Supported? |
|---|---|---|---|
| `06-streaming-use-cases.md:119` | §6.1.7 | "The 8.1 evaluation pack reads a frozen table: the recommendations were generated once, and every metric is computed against those stored rows." | **Partly.** True of the legacy pack. The canonical pack reads `regenerated_recommendations.json`, a validated reconstruction, because the fix postdated the frozen run. §5.5 should describe the frozen-table *method* and note the reconstruction, or §6.1.7's sentence becomes an overstatement. `[Certain]` on the artifact provenance; `[Likely]` that this is worth a wording tweak in Ch6 — **but Session 3 must not touch Chapter 6**, so this goes to Session 5. |
| `06-streaming-use-cases.md:125` | §6.1.7 | "No accuracy metric … This is the same position taken by the 8.1 evaluation pack, for the same reason." | **Yes.** `eval/8_1/README.md`: *"**No accuracy metric.** There is no ground truth and no real users for this dataset, so this pack computes no precision@k, recall@k, or NDCG."* `[Certain]` |
| `06-streaming-use-cases.md:199` | §6.1.9 | "…the 8.1 evaluation having measured this scoring path's repeated-run noise floor at exactly zero across 41,100 matched pairs." | **Yes**, with the static-index bound in §4 above. `[Certain]` |

---

## 10. Session 1 exit state

- Full suite run live: **215 passed**, 0 failed.
- Files modified: this one. `eval/`, `reports/`, `thesis/*.md`, `docs/` untouched —
  confirmed by `git status`.
- Every numeric row above carries a source path.
- No `[Guessing]` content was written; the three judgement calls are escalated in §7.

---

## 11. Session 2 addendum (2026-09-08)

Appended by Session 2 (figures). **Nothing above this line was edited** — §§0–10
are Session 1's output as reviewed. This section records the one escalation the
owner resolved and the one number Session 2 needed that Session 1 had not
extracted.

### 11.1 E3 — RESOLVED: keep pre-fix, label it explicitly

Owner decision, 2026-09-08: **option (a)**. Figure 5.1 plots the pre-fix frozen
batch run (`reports/uc81/`), and `thesis/05-implementation-uc81.md:87` keeps its
numbers (0.181–1.179, mean 0.794) but **gains an explicit label** saying they
describe the original batch run. §5.5 then reports the canonical post-fix pack
(min 0.1790, max 1.1789, mean 0.7941) with the provenance fork explained once.

Consequences for later sessions:
- **Session 3** writes that label into §5 line 87 and explains the fork once in
  §5.5. Without the label the chapter prints 0.181 and 0.1790 two pages apart
  with no account of the difference, which reads as an error.
- **Session 5** should confirm both readings survive the consistency sweep.
- Session 2 wrote **no thesis prose** and made no edit to line 87.

### 11.2 The >1.0 region — new derived count

Figure 5.1 marks the region above 1.0, which required a count Session 1 had not
recorded. Computed 2026-09-08 over the artifact already registered in §5:

```bash
python3 -c "import json;s=json.load(open('reports/uc81/results.json'))['score_distribution']['scores'];print(sum(1 for x in s if x>1.0), len(s))"
```

| Quantity | Value | Source |
|---|---|---|
| Rows scoring above 1.0 | **203** | counted over `reports/uc81/results.json::score_distribution.scores` |
| As a share of 4,110 | **4.94%** | same |

`[Certain]` — a count over a frozen artifact, not a re-run. Pre-fix pack, so it
belongs with §5's pre-fix row and with Figure 5.1, not with §5.5's canonical
numbers. This is the only figure value not already present in §§0–10; it is
recorded here so the "every number in a figure traces to a facts-file row"
invariant holds literally.

### 11.3 Binning detail worth knowing before writing the caption

`reports/uc81/histogram.json` has 26 bins, `lo = 0.1809`, `bin_width = 0.0384`,
counts summing to exactly 4,110 (verified). One bin **spans 1.0** — edges
0.9873 → 1.0257 — so the "above 1.0" region is not a clean bin boundary. The
figure shades the region rather than recolouring bars for exactly this reason.
A caption should say "scores above 1.0", not "the bins above 1.0". `[Certain]`

### 11.4 Session 2 exit state

- Files written: `thesis/figures/make_81_figures.py`,
  `thesis/figures/fig-5-{1,2,3}-*.png`, `thesis/figures/FIGURE_MANIFEST.md`,
  and this addendum.
- No thesis `.md` file was edited; no `![…]` reference exists yet. Session 3
  owns figure references and captions.
- Every plotted value traces to a row in this file. No figure introduced a
  number that is not recorded above.

---

## 12. Session 3 addendum (2026-09-08)

Appended by Session 3 (writing §5.5). **Nothing above this line was edited** — §§0–10
are Session 1's output, §11 is Session 2's. This section records the two escalations
the owner resolved before writing began, and the exit state.

### 12.1 E1 — RESOLVED: state the absence of pre-registration plainly

Owner decision, 2026-09-08: §5.5 says outright that nothing in the 8.1 pack was
pre-registered, and gives the structural reason (the pack was written against an
already-generated table, so the numbers existed before the questions did). The
contrast with Chapter 6 is named rather than left for the reader to infer. Written
as the fourth commitment in §5.5.1 and repeated as the second limitation in §5.5.5.

**Consequence for the prose, applied:** metrics 1–6 are reported with no verdict
language anywhere in §5.5.2 — no "pass", no "meets", no threshold. The plan's
structure item 1 ("state the pre-registration commitment") could not be followed as
written and was inverted, per this decision.

### 12.2 E2 — RESOLVED: include the path-validation rule at the weaker strength

Owner decision, 2026-09-08: §5.5.4 reports the rule and its PASS, phrased as *"the
rule is recorded in the script that applies it, written before the numbers it judges
existed; but rule and result entered version control together, so … the ordering here
rests on the record in the source file rather than on the commit history."* No
sentence in Chapter 5 claims commit-ordered pre-registration.

### 12.3 One claim dropped for lack of an artifact row

An early draft of §5.5.3 stated that re-running the pack after the ordering fix
reproduced the pre-fix `results.json` byte for byte. That claim appears in the
project's working notes but **has no row in §§0–11 of this file and no artifact
behind it here**, so it was removed rather than cited. The sentence now says only
what §0 and §3 support: the pre-fix pack is retained unmodified and marked
superseded, so Table 5.5 compares two recorded states.

Two further numbers that exist in the working notes were likewise **not** used, for
the same reason: the ~39% signal-reconstruction self-inconsistency rate that first
exposed the `related_by_genre` finding, and the `score::float8` round-trip fix from
the path-validation work. Neither has a row above. §5.5.3 states the finding without
the 39%; §5.5.4 states the validation without the float8 detail.

### 12.4 Rows consumed by §5.5

Every number in §5.5 traces to a row above. Mapping, by facts-file section:

| §5.5 subsection | Facts rows used |
|---|---|
| 5.5.1 (commitments) | §0 (provenance fork), §1.1 (no pre-registered rules), §1.2 (the one rule), §2 Metric 1 (`reconstruction_inconsistent_count` = 0), §9 (Ch6's forward references) |
| 5.5.2 Metric 1 | §2 Metric 1 — all four combination counts/pcts, `pct_in_similarity_pool`, the coverage-gap block (2184 / 1583 / 72.48%) |
| 5.5.2 Metric 2 | §2 Metric 2 — 400/411, 97.32, Gini 0.3898, max 41 |
| 5.5.2 Metric 3 | §2 Metric 3 — mean 0.2569, median 0.2349, p10 0.1447, p90 0.4046, 411 seeds, 0 skipped |
| 5.5.2 Metric 4 | §2 Metric 4 — the full nine-value table, 411 seeds; framing per §7 E4 (no pack attribution) |
| 5.5.2 Metric 5 | §2 Metric 5 — 411/201/77, all four degree distributions, 59 zero-genre-sibling seeds |
| 5.5.2 Metric 6 | §2 Metric 6 — 0 seeds under top-k 10 |
| 5.5.3 | §3 — the full before/after table and the row-level block (1741 / 217 / 60 / 134 / 11, ±0.05) |
| 5.5.4 | §4 — 5 runs, 10 pairs, 41100 pairs, 0.0 max delta, 411 identical per pair, the IVF_FLAT/nlist 128/nprobe 16 bound, path validation PASS; §1.2 for the rule text |
| 5.5.5 | §1.1, §2 Metric 1 (gap), §4 (index bound), §7 E5 |
| §5.3.4 line 87 label | §5 (both score readings), §11.1 (E3 decision), §11.2 (203 rows / 4.94%) |

### 12.5 Figures referenced, and where

| Figure | File | Referenced in |
|---|---|---|
| 5.1 Score distribution (pre-fix) | `figures/fig-5-1-score-distribution.png` | §5.3.4, at the batch-run paragraph it describes |
| 5.2 Popularity concentration, before/after | `figures/fig-5-2-concentration.png` | §5.5.3 |
| 5.3 Signal contribution | `figures/fig-5-3-signal-contribution.png` | §5.5.2, Metric 1 |
| 5.4 Intra-list diversity histogram | `../eval/8_1/figures/diversity_histogram.png` | §5.5.2, Metric 3 — the eval pack's own canonical figure, referenced **in place**, exactly as §6.1 references `../eval/8_2/figures/…` |

Figure 5.4 was added this session by owner decision; Session 2 generated no such
figure and none was needed, since the canonical pack already ships one.

### 12.6 Left for Session 5, deliberately untouched

- `thesis/05-implementation-uc81.md` — the two stale test-count claims (32 of 32
  across six build stages; 32/32 across the whole suite). §5.5 states **no** test
  count of its own, so Session 5 owns the reconciliation described in §6.1 of this
  file without any new conflict to resolve.
- Chapter 6 line 119 (§6.1.7's "reads a frozen table"), flagged in §9 as an
  overstatement now that the canonical pack reads a validated reconstruction. §5.5.1
  states the reconstruction explicitly, so the two chapters are consistent in
  substance; the wording tweak in Ch6 is still Session 5's call.
- `thesis/04-system-architecture.md`'s draft note — Session 4.

### 12.7 Session 3 exit state

- Files modified: `thesis/05-implementation-uc81.md` (§5.3.4 label + Figure 5.1,
  captions on the three pre-existing tables as Tables 5.1–5.3, new §5.5) and this
  addendum. Chapter 6 untouched, confirmed by `git status`.
- `make` in `thesis/` builds `build/thesis.docx` clean: 12 images embedded (8 from
  Chapter 6, 4 from Chapter 5), and all five Chapter 5 table captions plus all four
  Chapter 5 figure captions render.
- Full suite: see §12.8 below.

### 12.8 Test suite, this session

```bash
platform/enrichment/.venv/bin/python -m pytest -q
```

**Result: `215 passed, 19 warnings in 69.96s`.** `[Certain]` — unchanged from §6's
Session 1 reading, as expected: this session wrote prose only and touched no code.
