import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from live_classifier import classify, cosine_similarity  # noqa: E402


def test_cosine_similarity_identical_vectors_is_one():
    v = [1.0, 2.0, 3.0]
    assert math.isclose(cosine_similarity(v, v), 1.0, rel_tol=1e-9)


def test_cosine_similarity_orthogonal_vectors_is_zero():
    assert cosine_similarity([1.0, 0.0], [0.0, 1.0]) == 0.0


def test_cosine_similarity_zero_vector_returns_zero_not_nan():
    assert cosine_similarity([0.0, 0.0], [1.0, 1.0]) == 0.0


def test_classify_returns_top_k_descending_by_similarity():
    track = [1.0, 0.0]
    labels = {
        "rock": [1.0, 0.0],       # cos = 1.0
        "pop": [0.7, 0.7],        # cos ~= 0.707
        "jazz": [0.0, 1.0],       # cos = 0.0
    }
    result = classify(track, labels, top_k=2)
    assert [r[0] for r in result] == ["rock", "pop"]
    assert math.isclose(result[0][1], 1.0, rel_tol=1e-9)


def test_classify_ties_broken_by_label_name():
    track = [1.0, 0.0]
    labels = {"zeta": [1.0, 0.0], "alpha": [1.0, 0.0]}
    result = classify(track, labels, top_k=2)
    assert [r[0] for r in result] == ["alpha", "zeta"]


def test_classify_respects_top_k():
    track = [1.0, 0.0]
    labels = {f"label{i}": [1.0, 0.0] for i in range(10)}
    assert len(classify(track, labels, top_k=3)) == 3
