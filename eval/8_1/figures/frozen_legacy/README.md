# Legacy figures — do not cite

`coverage_long_tail.png`, `diversity_histogram.png`, and
`latency_breakdown.png` in this directory were generated 2026-08-31
against the frozen 4110-row `recommendations` table under the *original*
`related_by_genre` Cypher query (`LIMIT 10`, no `ORDER BY` — arbitrary
candidate selection).

Superseded 2026-09-01 (Stage 15A.2) by `eval/8_1/figures/*.png`,
generated against a regenerated table validated bit-identical to a real
`recommend.py` run under the Stage 15A ordered query (see
`eval/8_1/path_validation.json`).

Retained for provenance only — do not cite these as current results.
