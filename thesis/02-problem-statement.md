# 2. Problem Statement

Music-related data presents several challenges due to its heterogeneous and dynamic nature. Different modalities require distinct preprocessing techniques and feature-extraction methods, while real-world data often contains noise, redundancy, and inconsistencies. The increasing importance of real-time interaction in modern platforms introduces a further requirement: user behavior, listening context, and external signals are continuously generated as data streams, requiring systems to process information incrementally rather than in isolated batches.

The key challenge addressed in this thesis is the design of a unified pipeline capable of:

-   processing heterogeneous multimedia inputs in a consistent manner;

-   supporting both batch and streaming data;

-   integrating modality-specific representations into a shared space;

-   mapping low-level features to high-level semantic concepts;

-   enabling adaptability to dynamic user context.

Without such a unified framework, downstream applications suffer from limited interpretability, reduced responsiveness, and poor integration across data sources.
