# 6. Streaming Use Cases: 8.2 and 8.3

> **[DRAFT NOTE]** This chapter is a placeholder to be completed once 8.2/8.3 are implemented. Suggested structure mirrors Chapter 5: scope, environment additions (Kafka consumer/producer, Flink job, Redis session cache wiring), pipeline stages, verification, summary. Keep the same 'deviation from general architecture' honesty pattern used in 5.1 for anything scoped down under time pressure.

## 6.1 Use Case 8.2 — Streaming, Reactive

[To be completed.] Explicit user query within a live session, over WebSocket. Redis as session cache, fed by the platform-owned Kafka consumer (Decision B) that writes each event to both Redis and PostgreSQL, the system of record, on the same poll.

## 6.2 Use Case 8.3 — Streaming, Proactive

[To be completed.] System-inferred suggestions from a live-played track, no explicit query. Simulated live stream via a Freesound Creative Commons clip. Auto-tagging reused as a classifier-only building block inside the Semantic API, with no live persistence to Neo4j during the session.

> **[DRAFT NOTE]** Explicit professor confirmation on the Freesound simulated-stream approach for 8.3 is still pending as of this draft — flag this openly in the thesis if it remains unconfirmed at submission time, rather than presenting it as settled.
