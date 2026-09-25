"""
8.3: pure CLAP zero-shot classification logic -- no I/O, unit-testable
against synthetic vectors (same testability pattern as
platform/streaming/session_profile.py and platform/scoring/ranking.py).

classify() takes a track's AUDIO embedding (already computed by stage 3,
read from Redis at call time by the caller -- see proactive_service.py)
and a dict of genre-label TEXT embeddings (precompute_genre_labels.py),
and returns the top_k labels by cosine similarity. This is CLAP's own
zero-shot classification recipe (audio embedding vs. a set of text-prompt
embeddings, both already L2-normalized by the model) -- not a trained
classifier, and it writes nothing anywhere; the caller decides what to do
with the returned tags.
"""
import math


def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


def classify(
    track_embedding: list[float],
    label_embeddings: dict[str, list[float]],
    top_k: int = 3,
) -> list[tuple[str, float]]:
    """Returns up to top_k (label, similarity) pairs, descending by
    similarity, ties broken by label name for determinism."""
    scored = [
        (label, cosine_similarity(track_embedding, label_vec))
        for label, label_vec in label_embeddings.items()
    ]
    scored.sort(key=lambda pair: (-pair[1], pair[0]))
    return scored[:top_k]
