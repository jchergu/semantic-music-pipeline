# 1. Introduction

Modern music systems operate on large volumes of heterogeneous data originating from multiple modalities, including audio signals, images (e.g. album artwork), video (e.g. music videos), and textual metadata. While these data sources differ in structure and representation, they jointly contribute to the perception, interpretation, and consumption of music.

Raw multimedia data, however, is typically unstructured, noisy, and difficult to interpret at a semantic level. Low-level features extracted from different modalities — spectral characteristics in audio, visual patterns in images — do not directly correspond to human-understandable concepts like mood, genre, or usage context. This discrepancy is commonly referred to as the semantic gap.

This thesis proposes a data-driven multimedia pipeline designed to process heterogeneous inputs in a unified, input-agnostic manner within the music domain, transforming raw static and streaming data into structured, semantically enriched representations that support a variety of downstream applications. The architecture is general and modular, enabling multiple specialized pipelines to be generated for different tasks; several application scenarios are presented, spanning both batch and real-time use cases.

## 1.1 Motivation

> **[DRAFT NOTE]** Expand: why does this matter commercially/academically? Reference the recommendation-system market, the limits of pure collaborative filtering (cold start, opacity), and why a semantic layer + knowledge graph specifically helps (interpretability, cross-modal reasoning, reuse across multiple downstream applications rather than one bespoke model per feature).

## 1.2 Objectives

-   Design a unified, modular pipeline architecture spanning data ingestion, semantic enrichment, and application layers, applicable across modalities (audio, image, video, text) and processing regimes (batch and streaming).

-   Define a generic Semantic API as the shared building block between the pipeline's semantic layer and any number of downstream applications, avoiding an architecture coupled to a single use case.

-   Demonstrate the architecture concretely through a context-aware music Recommender Engine, implemented across three independent use-case modes: batch/reactive (8.1), streaming/reactive (8.2), and streaming/proactive (8.3).

-   Implement and empirically verify the batch/reactive mode end-to-end on a real, licensed seed dataset, under an explicit local-CPU compute constraint.

## 1.3 Thesis Structure

Chapter 2 states the problem this thesis addresses. Chapter 3 surveys the relevant state of the art across Music Information Retrieval, knowledge graphs, stream processing, and multimodal representation learning. Chapter 4 presents the confirmed system architecture and technology stack. Chapter 5 describes the implementation and verification of use case 8.1. Chapter 6 [reports results / discusses 8.2 and 8.3, depending on what's completed by submission]. Chapter 7 concludes and outlines future work.
