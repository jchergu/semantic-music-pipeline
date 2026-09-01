# 8.1 Evaluation Tables

## Signal contribution

| Combination | Rows | % |
|---|---|---|
| similarity_only | 3055 | 74.33 |
| genre_only | 440 | 10.71 |
| artist_only | 454 | 11.05 |
| genre_and_artist | 161 | 3.92 |

In similarity pool: 100.0%.
Reconstruction-inconsistent rows (see note in results.json): 0 / 4110 (0.0%).

Genre-boost coverage gap: of 2184 rows that genuinely share a genre tag with their seed, 1583 (72.48%) never received GENRE_BOOST because the Semantic API's related_by_genre response is capped at 10 candidates (ordered by shared-genre-count descending as of Stage 15A) -- a confirmed, still-unaddressed pipeline limitation (the cap itself, not the ordering), not a reconstruction artifact.

## Catalog coverage

- Covered: 400 / 411 tracks (97.32%)
- Gini coefficient: 0.3898
- Appearance range: 0–41

## Intra-list diversity

- Seeds evaluated: 411 (skipped 0 with <2 recommendations)
- Mean: 0.2569, median: 0.2349, p10: 0.1447, p90: 0.4046

## Latency

Not measured for this pass -- the ranking pipeline's per-stage latency doesn't depend on which recommendations table is being evaluated; see the frozen table's tables.md for a fresh reading.

## KG connectivity

- Tracks / Artists / Genres: 411 / 201 / 77
- Track genre out-degree: mean 1.859, median 2
- Artist track out-degree: mean 2.045, median 1
- Genre track in-degree: mean 9.922, median 3
- Seeds with zero genre siblings (cold start): 59

## Failure / edge cases

- Seeds with fewer than 10 recommendations: 0
