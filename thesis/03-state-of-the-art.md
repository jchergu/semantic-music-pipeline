# 3. State of the Art

This chapter surveys the scientific and technological landscape relevant to the proposed pipeline, organized around the three challenges introduced in Chapter 2: processing heterogeneous multimedia data, bridging the semantic gap between low-level features and human-understandable concepts, and supporting both batch and real-time processing within one framework.

## 3.1 Music Information Retrieval

Music Information Retrieval (MIR) is the foundational research area underlying the pipeline's feature-extraction stage. Librosa (McFee et al., 2015) offers a high-level Python API well suited to rapid prototyping, while Essentia (Bogdanov et al., 2013), developed at the Music Technology Group of Universitat Pompeu Fabra, is a production-grade C++ library with Python bindings used in industry (e.g. Spotify/AcousticBrainz). AcousticID/Chromaprint provides the fingerprinting mechanism used to link ingested audio to MusicBrainz identifiers; AcousticBrainz itself, the community feature database keyed on those identifiers, was discontinued in 2022, leaving Chromaprint/AcoustID as the active linkage standard.

## 3.2 Bridging the Semantic Gap: Cross-Modal Representations

Recent work bridges the semantic gap using contrastive learning between modalities. CLAP (Contrastive Language-Audio Pretraining; Elizalde et al., ICASSP 2023) maps audio and text into a joint embedding space, enabling zero-shot classification and semantic retrieval, and is the model adopted in this thesis's enrichment layer. CLaMP 2 extends cross-modal MIR across 101 languages, relevant to multilingual metadata enrichment. HTCL (ICMR 2025) bridges a semantic embedding space and a user-preference space specifically for recommendation, conceptually adjacent to this thesis's ranking design.

## 3.3 Knowledge Graphs for Music

Oramas et al. (ACM TIST, 2016) established the foundational case for knowledge-graph-based music recommendation, showing improved interpretability and cold-start handling relative to purely collaborative approaches — a motivation directly reflected in this thesis's use of same-artist and genre-sibling graph signals as ranking features. Subsequent KG-based recommenders (MKGCN; the MMSS_MKR framework) extend this to multi-modal and deep-learning settings. Neo4j is adopted as the property-graph engine of choice, consistent with its widespread use in comparable systems; Protégé remains the standard tool for authoring the underlying OWL ontology (genres, moods, contexts) where a formal ontology layer is warranted. Apache Jena/RDF4J were evaluated as an RDF/SPARQL-standards-compliant alternative but were not carried into the final stack, in favor of the more directly queryable Neo4j/Cypher combination.

## 3.4 Stream Processing and Data Engineering

The pipeline's real-time capability rests on the industry-standard pairing of Apache Kafka for distributed event streaming and Apache Flink for stateful, low-latency stream computation with native support for windowed aggregation — essential for incremental feature extraction over live audio or behavioral event streams. Apache Spark remains the reference engine for the batch path, distributing large-scale feature extraction and knowledge-graph construction jobs; Apache Airflow orchestrates the resulting batch DAGs. Architecturally, the pipeline follows a Kappa-inspired approach — batch as the primary paradigm, with streaming treated as a real-time extension rather than a duplicated parallel logic path (avoiding the dual-codebase maintenance burden of a full Lambda architecture).

## 3.5 Multimodal Data Processing Beyond Audio

Album artwork can be processed through CLIP (Radford et al.) or captioned via BLIP-2/LLaVA for automatic semantic annotation; OpenCV and Pillow cover conventional image preprocessing. Music videos can be decoded and normalized with FFmpeg, with motion/energy patterns extracted via OpenPose/MediaPipe or self-supervised video transformers (VideoMAE, TimeSformer). Video is architecturally supported but offers marginal semantic gain over the audio-plus-artwork combination at catalog scale, and carries real availability/copyright constraints — it is therefore treated as an architectural extension point rather than part of the implemented prototype. Textual metadata (lyrics, bios, tags) is covered by spaCy and HuggingFace Transformers, with Sentence-BERT providing efficient semantic-similarity embeddings for descriptive text.

## 3.6 Recommendation and Context Modeling

Collaborative filtering (matrix factorization such as ALS/SVD++) and content-based filtering remain the two baseline recommendation paradigms; production systems typically hybridize both. LightGCN (He et al., SIGIR 2020) represents the current graph-convolutional state of the art for collaborative filtering. Context-Aware Recommender Systems (Adomavicius & Tuzhilin, 2015) supply the foundational taxonomy for incorporating situational factors — time of day, activity, inferred user state — which motivates this thesis's context-aware framing of the Recommender Engine, even though the implemented 8.1 prototype uses a track-trigger model rather than full behavioral context (Section 5).

## 3.7 Data Sources

Table 3.1 summarizes the external data sources evaluated as candidate inputs.

  ------------------------------------------------------------------------------------------------------------------------------
  **Source**                 **Type**                        **Role in this thesis**
  -------------------------- ------------------------------- -------------------------------------------------------------------
  Jamendo API                REST, open, CC-licensed audio   Primary seed-dataset source (Section 5.3.1)

  MusicBrainz API            REST, open metadata             Fingerprint-linkage target, not an audio source

  Freesound API              REST, open, CC audio clips      Planned simulated live-stream source for use case 8.3

  Spotify API                REST                            Evaluated; audio features/metadata, not used in the 8.1 prototype

  Last.fm API                REST                            Evaluated for scrobble/tag data; not used in the 8.1 prototype

  FMA (Free Music Archive)   Static dataset                  Evaluated as seed source, rejected — see Section 5.3.1
  ------------------------------------------------------------------------------------------------------------------------------

## 3.8 Summary and Positioning

Existing research addresses individual components of this workflow in isolation: MIR contributes feature extraction, knowledge-graph research contributes semantic structuring and interpretability, and stream-processing research contributes real-time ingestion — but these three areas are rarely integrated within a single, reusable pipeline that spans both batch and streaming regimes and multiple modalities. This thesis's contribution is precisely that integration, instantiated concretely through a demo Recommender Engine that consumes — but does not define — a generic, reusable Semantic API.
