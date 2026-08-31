# 4. System Architecture

The proposed system is structured as a modular, extensible, input-agnostic pipeline composed of three layers: Data Preparation, Semantic Enrichment, and a Semantic API / Application layer. The pipeline supports both batch processing of static datasets and stream processing of continuous, real-time data, allowing multiple specialized processing flows to be generated from the same underlying architecture.

## 4.1 Layer 1 — Data Preparation

Responsible for transforming raw inputs into structured representations: cleaning, deduplication, normalization, and modality-specific feature extraction (audio: tempo, spectral features, MFCCs; image: color/texture/visual embeddings; video: motion/temporal features; text: embeddings and keyword extraction). Streaming inputs are processed incrementally over time windows rather than requiring a complete dataset. Storage is split by concern into three distinct systems, each addressing a different access pattern: object storage (MinIO/S3) for raw media blobs, a columnar feature store (Parquet/Delta Lake) for extracted numerical features, and PostgreSQL as the relational system of record for track metadata.

## 4.2 Layer 2 — Semantic Enrichment

Maps structured features into a shared semantic space via a knowledge graph representing domain knowledge — genres, moods (e.g. relaxing, energetic), functional labels (e.g. focus, workout, chill), and the relationships between entities. For streaming data this layer is stateful, maintaining temporal context and updating semantic representations dynamically as new data arrives. CLAP embeddings populate a vector index (Milvus) for similarity search, while the Neo4j graph captures discrete relational structure (artist, genre, and related-entity edges); the two are joined on a common key — the MusicBrainz recording ID where available, or the pipeline's own internal track ID otherwise — so that the Semantic API layer can combine continuous-similarity and discrete-relational signals for the same entity.

## 4.3 Layer 3 — Semantic API / Application

Consumes the semantically enriched representations to generate outputs tailored to specific use cases. The Semantic API is a generic, shared building block reused across multiple Layer-3 applications — Recommender Engine, Similarity Search, Auto-tagging, and Playlist Generation — rather than being defined by any single one of them; this thesis's Recommender Engine is one consumer of that API, not its specification. This separation between pipeline and application is what gives the architecture its reusability: new applications can be built on the same semantic substrate without redesigning the underlying pipeline.

> **[DRAFT NOTE]** Insert the confirmed component diagram here (pipeline architecture) once finalized as a figure — distinct from the software architecture diagram (internal modules, sequence diagrams, API contracts) that belongs in Chapter 5, per the deliberate distinction agreed with the supervisor.

## 4.4 Technology Stack

Every technology in the stack is open-source and self-hostable, and the entire prototype is scoped to run on local CPU hardware by capping the seed dataset at 200-500 tracks — resolving the compute-feasibility question without altering the architecture itself.

  --------------------------------------------------------------------------------------------------------------------------------
  **Component**           **Technology**          **Role**
  ----------------------- ----------------------- --------------------------------------------------------------------------------
  Metadata store          PostgreSQL              System of record for track/entity metadata

  Graph store             Neo4j                   L2 knowledge graph (artist, genre, relations)

  Vector store            Milvus                  L2 CLAP embedding index, similarity search

  Object store            MinIO / S3              L1 raw media storage

  Event streaming         Apache Kafka            L1/L2 media & behavioral event topics (streaming use cases)

  Stream processing       Apache Flink            Stateful windowed enrichment (streaming use cases)

  Batch processing        Apache Spark            Distributed batch feature extraction / KG construction, at catalog scale

  Orchestration           Apache Airflow          Batch pipeline DAG scheduling

  Session cache           Redis                   Session state for streaming use cases (8.2/8.3), fed by a platform-owned Kafka consumer, not Flink (Decision B)

  Application API         FastAPI                 L3 Semantic API and application services
  --------------------------------------------------------------------------------------------------------------------------------

Kafka topics are deliberately separated by concern — media streams are not conflated with behavioral event streams — and the architecture follows a Kappa-style approach in which batch is the primary processing paradigm and streaming is treated as a real-time extension of the same logic, avoiding the duplicated business-logic maintenance burden of a Lambda architecture.

## 4.5 Use Case Taxonomy

The Recommender Engine — the demo application used throughout this thesis — is decomposed into three independent modules, confirmed for full prototype implementation:

  -------------------------------------------------------------------------------------------------------------------------
  **Use case**      **Mode**               **Trigger**                                                    **Transport**
  ----------------- ---------------------- -------------------------------------------------------------- -----------------
  8.1               Batch, reactive        Explicit query against a static/batch context                  REST

  8.2               Streaming, reactive    Explicit query within a live session                           WebSocket

  8.3               Streaming, proactive   System-inferred, no explicit query, from a live-played track   WebSocket
  -------------------------------------------------------------------------------------------------------------------------

For 8.2 and 8.3, Redis serves as a session cache — distinct from PostgreSQL's role as the system of record — fed by the platform-owned Kafka consumer that reads the behavioral-events topic and, on the same poll, writes each event to both Redis (under a sliding 30-minute TTL) and PostgreSQL directly, rather than through a separate stream-processing job (Decision B). For 8.3, Auto-tagging is reused as a classifier-only building block inside the Semantic API, with no live persistence to Neo4j during the streaming session. Use case 8.3's implementation simulates a live audio stream via a Creative-Commons clip served through the Freesound API, rather than live hardware capture.
