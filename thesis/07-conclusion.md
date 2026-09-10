# 7. Conclusion and Future Work

## 7.1 Conclusion

> **[DRAFT NOTE]** Write after Ch. 6 is finalized. Summarize: what was designed (3-layer architecture + generic Semantic API), what was built and verified (8.1, end-to-end, 32/32 tests, 411 real tracks), what 8.2/8.3 added, and how the results support the thesis's original objectives from Section 1.2.

## 7.2 Limitations

-   Seed dataset scale (200-500 tracks) trades catalog realism for local CPU feasibility; ranking weights are a documented heuristic, not learned or validated against ground-truth relevance judgments.

-   No user/behavioral data exists in the 8.1 prototype; the batch Recommender Engine's context is limited to the seed track itself.

-   Spark, Flink, and Airflow are part of the confirmed architecture but are not exercised at this dataset scale (Section 5.1).

## 7.3 Future Work

-   Implementing use case 8.3 (streaming, proactive) to the design set out in Section 6.2. The platform it needs is already built and measured; what remains unresolved is the trigger policy, and deciding when an unsolicited suggestion is worth making requires the relevance judgements this dataset does not carry.

-   Closing the two limitations Section 6.1.11 records as deliberate: an allowed lateness and a side output on the Flink job, so that late events are diverted rather than silently dropped, and a timestamp alongside the recommendation key, without which a client cannot distinguish current recommendations from ones that stopped updating when a dependency failed.

-   Scaling the seed dataset and re-introducing Spark/Flink/Airflow as the workload grows beyond single-machine CPU feasibility.

-   Extending the architecture to the image and video modalities surveyed in Chapter 3 but not implemented here.

-   Replacing the heuristic ranking function with a learned model once ground-truth relevance data is available.
