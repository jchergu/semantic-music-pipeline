Alma Mater Studiorum – Università di Bologna

School of Engineering

Master's Degree in Computer Engineering

**Semantic-Aware Multimodal Music Pipelining:**

**A Data-Driven Architecture for Context-Aware Recommendation**

*Master's Thesis in [insert exam name]*

**Supervisor:**

Prof. [IB — full name]

**Candidate:**

Jacopo Chergui

Academic Year 2025/2026

# Abstract

> **[DRAFT NOTE]** Write this last — 200-300 words. One sentence each: problem, gap, proposed pipeline, demo use case (Recommender Engine), what was built (8.1 verified end-to-end, 411 tracks), what 8.2/8.3 add, key result/finding.

Modern music platforms consume heterogeneous, multi-modal data — audio, imagery, text, and behavioral streams — yet raw signals from these sources rarely map directly onto the human-understandable concepts (mood, genre, context) that recommendation and discovery applications need. This thesis proposes a modular, input-agnostic multimedia pipeline that bridges this semantic gap through a three-layer architecture — Data Preparation, Semantic Enrichment, and a shared Semantic API — supporting both batch and streaming operation. As a concrete demonstration, a context-aware music Recommender Engine is implemented as one Layer-3 consumer of the generic Semantic API, spanning three independent use-case modes: batch/reactive, streaming/reactive, and streaming/proactive. This document reports the design of the full architecture and the implementation and empirical verification of the batch/reactive mode over a 411-track licensed seed dataset, using CLAP audio embeddings, a Neo4j knowledge graph, and Milvus vector search.
