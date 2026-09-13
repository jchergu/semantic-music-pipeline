# 7. Conclusion and Future Work

## 7.1 Conclusion

This thesis set out to design a modular, input-agnostic semantic pipeline for multimodal music systems and to demonstrate it through a context-aware Recommender Engine spanning three independent use-case modes. Chapter 4 presented a three-layer architecture — Data Ingestion/Preparation, Semantic Enrichment, and a shared, read-only Semantic API — and Chapters 5 and 6 validated that architecture with three independent Layer-3 consumers reusing the same API, the same vector index, and the same knowledge graph, rather than three parallel implementations.

Chapter 5 implemented and empirically verified use case 8.1 (batch, reactive) end-to-end against a 411-track licensed seed dataset: CLAP-based similarity, genre-sibling, and same-artist signals combined into a ranked recommendation list, evaluated across six systems-and-behavior metrics — catalog coverage, intra-list diversity, per-stage latency, knowledge-graph connectivity, signal attribution, and failure/edge cases — with no accuracy claim, since no ground-truth relevance labels exist for this dataset.

Chapter 6 extended the same platform to use case 8.2 (streaming, reactive): a Kafka/Flink/Redis pipeline maintaining a live per-session semantic profile and refreshing recommendations continuously, built and verified end-to-end, and evaluated across eight pre-registered metrics covering reactivity, latency, coherence, catalog coverage, determinism, late-event handling, and two failure-injection scenarios. Of the 215 automated tests in the repository, all of which pass, 162 belong to the streaming stages and their evaluation packs, with the remaining 53 covering the platform's shared stages and use case 8.1. Chapter 6 also set out use case 8.3 (streaming, proactive) as a design rather than a result: which of 8.2's components it would reuse unchanged (Table 6.9), which would still have to be built (Table 6.10), and which of 8.2's measured limitations — the recommendation key's missing timestamp, the Flink job's silent late-event drop — become preconditions once delivery is push rather than pull.

Taken together, the batch and streaming reactive modes meet the objectives set out in Section 1.2: a single generic Semantic API served three independent consumer modes without bespoke per-mode infrastructure, and both reactive modes were implemented and measured rather than merely designed. The proactive mode remains a validated design, bounded by measurements taken on the substrate it would run on, but not yet a measured result of its own.

## 7.2 Limitations

-   Seed dataset scale (200-500 tracks) trades catalog realism for local CPU feasibility; ranking weights are a documented heuristic, not learned or validated against ground-truth relevance judgments.

-   No user/behavioral data exists in the 8.1 prototype; the batch Recommender Engine's context is limited to the seed track itself.

-   Spark and Airflow are part of the confirmed architecture but are not exercised at this dataset scale (Section 5.1); Flink, by contrast, is exercised — Section 6.1 describes a live PyFlink job computing session profile centroids over sliding event-time windows, measured under both normal operation and induced failure.

## 7.3 Future Work

-   Implementing use case 8.3 (streaming, proactive) to the design set out in Section 6.2. The platform it needs is already built and measured; what remains unresolved is the trigger policy, and deciding when an unsolicited suggestion is worth making requires the relevance judgements this dataset does not carry.

-   Closing the two limitations Section 6.1.11 records as deliberate: an allowed lateness and a side output on the Flink job, so that late events are diverted rather than silently dropped, and a timestamp alongside the recommendation key, without which a client cannot distinguish current recommendations from ones that stopped updating when a dependency failed.

-   Scaling the seed dataset and re-introducing Spark and Airflow, plus running Flink beyond the single-machine job exercised here, as the workload grows beyond current CPU feasibility.

-   Extending the architecture to the image and video modalities surveyed in Chapter 3 but not implemented here.

-   Replacing the heuristic ranking function with a learned model once ground-truth relevance data is available.
