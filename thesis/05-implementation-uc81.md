# 5. Implementation: Use Case 8.1 (Batch, Reactive Recommendation)

This chapter documents the implementation and verification of the batch/reactive Recommender Engine, the first of the three confirmed use-case modes to be built. The build follows the confirmed six-stage order: environment provisioning, seed ingestion, semantic enrichment, the Semantic API, the Recommender Engine, and end-to-end verification.

## 5.1 Scope and Deviation from the General Architecture

The general pipeline architecture (Chapter 4) assigns Apache Spark to Layer-1 batch feature extraction and the Kafka-Flink pair to Layer-2 streaming enrichment. The 8.1 prototype implements neither, by deliberate design rather than omission. The seed dataset is capped at 200-500 tracks specifically to keep CLAP inference feasible on local CPU hardware; at this scale, a distributed batch engine has no workload to meaningfully distribute — enrichment processes the dataset as a sequential loop in batches of 8 tracks, completing in approximately 4 minutes wall-clock, CPU-only. Kafka and Zookeeper are provisioned in the Docker Compose environment from the first build stage, anticipating their role in the streaming use cases (8.2/8.3), but no producer or consumer exists yet in the 8.1 codebase. Introducing Flink-based stream processing at this stage would apply a streaming solution to a use case that, at this scale and in this batch/reactive mode, does not require one. Spark and Flink remain part of the confirmed technology stack; their absence here is a scope boundary of this specific use case, not a retraction of the architecture defined in Chapter 4.

## 5.2 Runtime Environment

The environment is defined in a single Docker Compose configuration provisioning: PostgreSQL 16 (metadata, system of record), MinIO (Layer-1 object storage for raw audio), Neo4j 5 Community (Layer-2 knowledge graph), Milvus 2.4.5 standalone — with its own dedicated etcd and MinIO-backed internal metadata/object store, kept distinct from the Layer-1 MinIO instance — for CLAP embedding storage and similarity search, and Kafka with Zookeeper plus Redis, both provisioned ahead of the streaming use cases and unused at this stage.

## 5.3 Pipeline Stages

### 5.3.1 Stage 2 — Seed Dataset Ingestion

The seed dataset was sourced from the Jamendo API rather than the Free Music Archive originally listed as a candidate in the state-of-the-art survey — a disk-budget decision: Jamendo serves tracks individually via a free API key, so download footprint scales with what is actually kept (approximately 2.3 GB for the 411 tracks retained), whereas FMA is distributed as one archive per split, with even the smallest split requiring a roughly 7.2 GB download before any subsetting is possible.

Each track is uploaded to MinIO, fingerprinted via Chromaprint, and linked on a best-effort basis to MusicBrainz via AcoustID. The dataset settled at 411 unique tracks against an initial 500-track target: Jamendo's popularity-based pagination is not perfectly stable across separate HTTP requests, producing roughly 90 duplicate entries on a second fetch pass, which were discarded via a database-level uniqueness constraint on the source identifier. 411 tracks remains within the project's confirmed 200-500 seed range, so no further fetching was pursued.

The AcoustID-to-MusicBrainz match rate reached 49.4% (203 of 411 tracks) — an expected outcome rather than a defect, since a substantial share of Jamendo's catalog consists of independent, self-released music that was never submitted to MusicBrainz. The pipeline treats a missing MusicBrainz identifier as a normal case rather than an error condition; this is itself a legitimate empirical observation about the partial nature of fingerprint-based linkage for non-mainstream corpora, worth reporting as a finding rather than only as an implementation detail.

  -------------------------------------------------------------------------
  **Metric**                            **Value**
  ------------------------------------- -----------------------------------
  Tracks ingested                       411 (target 500)

  Total audio size in object storage    2.31 GB

  Duration range / mean                 76s - 953s / 231s

  Distinct genre tags                   77

  AcoustID -> MusicBrainz match rate   49.4% (203 / 411)
  -------------------------------------------------------------------------

Verified by five automated tests: dataset size within the confirmed seed range, absence of duplicate source identifiers, presence of a fingerprint for every row, referential parity between the relational store and object storage, and duration plausibility bounds.

### 5.3.2 Stage 3 — Semantic Enrichment

A 512-dimensional CLAP audio embedding is computed for every track (LAION-CLAP 1.1.6, non-fusion configuration, the pretrained 630k-AudioSet checkpoint, CPU-only) and indexed in Milvus using an IVF_FLAT index under the cosine similarity metric. Independently, a (:Artist)-[:PERFORMED]->(:Track)-[:HAS_GENRE]->(:Genre) subgraph is materialized in Neo4j directly from the relational metadata already present after Stage 2 — this is a structural projection of existing data into a graph representation, not a step that infers new information. The enrichment job is idempotent, guarded by a per-track enriched timestamp, so partial or interrupted runs can be resumed safely rather than reprocessed from scratch.

All 411 tracks were enriched with zero failures; wall-clock time was approximately 4 minutes at a batch size of 8, i.e. roughly 0.6 seconds per track — concrete evidence that the 200-500-track scale constraint achieves its intended purpose of CPU feasibility without a GPU. The internal track identifier is deliberately reused as the join key across PostgreSQL, Milvus, and Neo4j, directly implementing the architecture's principle of a common key linking every storage component.

  ----------------------------------------------------------------------------
  **Metric**                               **Value**
  ---------------------------------------- -----------------------------------
  Tracks enriched                          411 / 411 (100%)

  Milvus vectors (dim)                     411 (512)

  Neo4j :Track / :Artist / :Genre nodes    411 / 201 / 77

  :PERFORMED / :HAS_GENRE relationships    411 / 764

  Wall-clock (411-track batch, CPU-only)   ~4 min (~0.6 s/track)
  ----------------------------------------------------------------------------

Verified by six automated tests confirming row-count and identifier parity between the relational store, the vector index, and the graph, in both directions.

### 5.3.3 Stage 4 — Semantic API

A single, read-only FastAPI service exposes six endpoints over the three Layer-1/Layer-2 stores: health, single-track metadata, similarity search, a per-track graph view (artist, genres, genre-sibling tracks), artist-scoped track listing, and genre-scoped track listing. A shared connection lifecycle — a pooled PostgreSQL connection pool, one persistent Milvus collection handle, and one Neo4j driver — is opened once at process startup and closed at shutdown, exposed to route handlers via dependency injection rather than being managed ad hoc per request.

The API deliberately exposes no catalog-enumeration endpoint (no "list all tracks"): its fixed surface is scoped to reading about a known entity, which any Layer-3 consumer needs, rather than to enumerating the full catalog, which is specific to a batch job's own iteration needs. This keeps the API generic rather than shaped around the Recommender Engine's particular requirements — a concrete implementation of the pipeline-versus-application separation from Chapter 4.

Verified by seven automated tests plus manual endpoint verification against the live 411-track dataset, covering health reporting, metadata retrieval, 404 handling for unknown identifiers, similarity-search correctness (exact k, self-exclusion, descending order), and artist/genre lookup correctness.

### 5.3.4 Stage 5 — Recommender Engine

The Recommender Engine adopts a seed-track trigger model rather than a fabricated user/session identity: a trigger is a single existing track identifier, standing in for "this track was just played or selected" — matching a "fans also like" or "up next" style of recommendation. This was a deliberate choice: no user or behavioral data exists anywhere in the pipeline at this stage (Kafka is provisioned but unwired, and behavioral-event topics are explicitly scoped to the streaming use cases), so a user-personalized design at this stage would have required fabricating a data model the rest of the pipeline does not yet have.

The engine is organized into three modules. The trigger handler resolves which track identifiers a batch invocation should process — either a single explicit seed or the full catalog, optionally capped — reading the identifier column directly from PostgreSQL as a narrow, documented exception to the "everything through the Semantic API" rule: this is batch work-item bookkeeping, not a semantic read of track content, and the API deliberately has no catalog-listing endpoint for a trigger handler to call instead. The context builder gathers three candidate sources for a given seed track entirely over HTTP from the Semantic API — similarity candidates, genre-sibling tracks, and same-artist tracks — and never queries any of the three underlying stores directly, enforcing the same API boundary described in Section 5.3.3. Ranking is a pure, side-effect-free function merging the three sources into one deduplicated, scored, descending-sorted list.

The ranking formula is an additive weighted sum:

score = similarity

+ 0.05 if the candidate is a genre sibling of the seed

+ 0.15 if the candidate is by the same artist as the seed

The two boost magnitudes are grounded in the dataset's own sparsity statistics rather than chosen arbitrarily: 764 genre relationships across 77 genres give roughly 9.9 tracks per genre on average, making genre co-membership a coarse, comparatively low-precision signal that receives a small boost; 411 tracks across 201 artists give roughly 2.0 tracks per artist, making shared authorship a rare, high-precision signal that receives a larger one. Both boosts remain well under similarity's own approximate [0,1] range, so they function as re-ranking nudges and tie-breakers rather than overriding the acoustic similarity signal outright; because boosts stack additively, a candidate confirmed relevant by more than one source ranks above one confirmed by only one. This is documented, explicitly, as a tunable heuristic appropriate to a prototype at this scale — 411 tracks, no ground-truth relevance labels — rather than a learned or empirically validated ranking model. Reciprocal Rank Fusion was considered as a more conventional alternative and rejected: the genre-sibling and same-artist candidate lists returned by the graph store are not meaningfully ordered, so a rank-based fusion method would not add real rigor over a documented membership boost at this scale.

Sanity-checked against real output for seed track 1 ("Wish You Were Here" by The.madpix.project): the top-ranked recommendation, by the same artist, scored exactly its raw similarity plus the artist boost; a third-ranked recommendation, a genre sibling only, scored exactly its raw similarity plus the genre boost — confirming both boosts apply precisely as designed rather than only in aggregate.

Across the full batch of 411 seed tracks, 4,110 recommendation rows were generated (100% success, zero self-recommendations, exactly the configured top-k for every seed), with scores ranging from 0.181 to 1.179 (mean 0.794); the ceiling above 1.0 is an expected consequence of the additive boosts stacking on top of a similarity score that is not strictly re-normalized back into [0,1], and is called out here so it does not read as a defect.

Verified by seven unit tests against the pure ranking function (synthetic candidates, no live services) and one integration test running the batch entrypoint against the live stack end-to-end.

### 5.3.5 Stage 6 — End-to-End Verification

A dedicated end-to-end suite traces three "golden" tracks through every layer of the pipeline in a single test run — relational store and object storage, vector index and graph, Semantic API, and Recommender Engine — asserting data consistency at each hop. This complements, rather than duplicates, the twenty-seven stage-specific tests from Stages 2-5, each of which already verifies its own stage's output is internally consistent; what none of them individually prove is that one piece of data remains consistent as it flows through all five stages together. Stage 6 closes that specific gap.

The suite runs against the live, already-populated stack rather than performing a destructive from-scratch rebuild — a deliberate, documented scope decision, since a full rebuild is slow, destroys the current verified dataset until it completes, and re-consumes rate-limited external API quota for no additional correctness benefit beyond what lineage tracing already provides. The full rebuild sequence is preserved as an on-demand runbook, intended for a reproducibility demonstration at the thesis defense rather than for routine execution.

Building this stage surfaced one genuine integration bug, not merely a test-writing exercise: a Milvus connection-alias collision between the Semantic API's in-process test client and the test suite's own shared fixture caused the API's shutdown handler to silently tear down the vector-database connection used by later, unrelated tests in the same session. The fix — a dedicated connection alias for the API, isolated from the test suite's own alias — is a concrete example of an integration-level defect that per-stage unit testing alone could not have caught, and is reported here as a finding, not only as a changelog entry.

Final verification state: 32 of 32 tests passing across all six build stages, over the full 411-track dataset.

## 5.4 Summary

  --------------------------------------------------------------------------------------------------------------------------
  **Stage**                           **Outcome**
  ----------------------------------- --------------------------------------------------------------------------------------
  1 — Environment                     Docker Compose: Postgres, MinIO, Neo4j, Milvus, Kafka, Redis

  2 — Ingestion                       411 tracks (Jamendo -> Postgres/MinIO), 5/5 tests

  3 — Enrichment                      411/411 embedded (CLAP) + graphed (Neo4j), 6/6 tests

  4 — Semantic API                    6 endpoints, FastAPI, 7/7 tests

  5 — Recommender Engine              411/411 seeds, 4,110 recommendations, 8/8 tests

  6 — End-to-end                      3 golden tracks traced across all layers, 5/5 tests, 1 integration bug found & fixed
  --------------------------------------------------------------------------------------------------------------------------

32/32 tests pass across the whole suite. Kafka and Redis remain provisioned but intentionally unwired at this stage, reserved for the streaming use cases described in Chapter 6.
