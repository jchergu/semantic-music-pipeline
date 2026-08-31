# 8.1 Evaluation Tables

## Signal contribution

| Combination | Rows | % |
|---|---|---|
| similarity_only | 3025 | 73.6 |
| genre_only | 470 | 11.44 |
| artist_only | 485 | 11.8 |
| genre_and_artist | 130 | 3.16 |

In similarity pool: 100.0%.
Reconstruction-inconsistent rows (see note in results.json): 0 / 4110 (0.0%).

Genre-boost coverage gap: of 2200 rows that genuinely share a genre tag with their seed, 1600 (72.73%) never received GENRE_BOOST because the Semantic API's related_by_genre response is capped at 10 candidates with no ordering -- a confirmed pipeline behavior, not a reconstruction artifact.

## Catalog coverage

- Covered: 399 / 411 tracks (97.08%)
- Gini coefficient: 0.3942
- Appearance range: 0–67

## Intra-list diversity

- Seeds evaluated: 411 (skipped 0 with <2 recommendations)
- Mean: 0.2562, median: 0.2342, p10: 0.144, p90: 0.3986

## Latency (measured fresh against the current environment; not the original batch run's historical timing)

| Stage | p50 (ms) | p95 (ms) | mean (ms) |
|---|---|---|---|
| Milvus ANN | 2.299 | 2.847 | 2.532 |
| Neo4j | 1.741 | 3.112 | 1.947 |
| Ranking | 0.134 | 0.248 | 0.131 |
| Total | 4.232 | 6.052 | 4.61 |

## KG connectivity

- Tracks / Artists / Genres: 411 / 201 / 77
- Track genre out-degree: mean 1.859, median 2
- Artist track out-degree: mean 2.045, median 1
- Genre track in-degree: mean 9.922, median 3
- Seeds with zero genre siblings (cold start): 59

## Failure / edge cases

- Seeds with fewer than 10 recommendations: 0
