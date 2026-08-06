"""
Generates Stage 2 (seed dataset ingestion) metrics and plots for the thesis.

Usage:
    python reports/stage2_metrics.py
Outputs to reports/stage2/: metrics.json, genre_distribution.png,
duration_histogram.png, musicbrainz_match.png
"""
import json
import os
from collections import Counter
from pathlib import Path

import matplotlib.pyplot as plt
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "reports" / "stage2"
load_dotenv(ROOT / ".env")

PG_DSN = (
    f"host=localhost port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ['POSTGRES_DB']} "
    f"user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']}"
)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = psycopg2.connect(PG_DSN)

    with conn.cursor() as cur:
        cur.execute("SELECT duration_sec, genre_tags, musicbrainz_recording_id FROM tracks")
        rows = cur.fetchall()

    durations = [r[0] for r in rows if r[0] is not None]
    genre_counts = Counter(g for r in rows for g in (r[1] or []))
    matched = sum(1 for r in rows if r[2] is not None)
    total = len(rows)

    metrics = {
        "total_tracks": total,
        "musicbrainz_match_rate": round(matched / total, 4) if total else 0,
        "musicbrainz_matched": matched,
        "duration_sec": {
            "min": min(durations),
            "max": max(durations),
            "mean": round(sum(durations) / len(durations), 1),
        },
        "distinct_genre_tags": len(genre_counts),
        "top_genres": genre_counts.most_common(10),
    }
    (OUT_DIR / "metrics.json").write_text(json.dumps(metrics, indent=2))
    print(json.dumps(metrics, indent=2))

    # Genre distribution (top 15)
    top = genre_counts.most_common(15)
    plt.figure(figsize=(8, 5))
    plt.barh([g for g, _ in reversed(top)], [c for _, c in reversed(top)])
    plt.xlabel("Track count")
    plt.title("Stage 2 seed dataset — genre tag distribution (top 15)")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "genre_distribution.png", dpi=150)
    plt.close()

    # Duration histogram
    plt.figure(figsize=(8, 5))
    plt.hist(durations, bins=30)
    plt.xlabel("Duration (seconds)")
    plt.ylabel("Track count")
    plt.title("Stage 2 seed dataset — track duration distribution")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "duration_histogram.png", dpi=150)
    plt.close()

    # MusicBrainz match rate
    plt.figure(figsize=(4, 5))
    plt.bar(["Matched", "No match"], [matched, total - matched])
    plt.ylabel("Track count")
    plt.title("AcoustID -> MusicBrainz linkage rate")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "musicbrainz_match.png", dpi=150)
    plt.close()

    conn.close()


if __name__ == "__main__":
    main()
