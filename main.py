"""
Pipeline orchestrator: fetch → dedupe → cluster → score → summarize → deliver, saving intermediate outputs.

Keywords and languages are loaded from src/keywords.json.
Date range is always the previous 7 days from runtime.
"""

import dataclasses
import datetime
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from ingest.gdelt_fetch import GDELTArticle, fetch_gdelt_articles
from process.dedupe import dedupe_articles
from process.cluster import cluster_articles
from process.score import score_clusters
from process.summarize import summarize_clusters, render_text_report
from deliver.email_sender import send_email

KEYWORDS_PATH = Path(__file__).parent / "src" / "keywords.json"


def _load_keywords() -> dict:
    with open(KEYWORDS_PATH, encoding="utf-8") as f:
        return json.load(f)


def _build_query(config: dict) -> str:
    """Build a GDELT query string from keywords.json config."""
    keywords = [entry["keyword"] for entry in config.get("query", [])]
    phrases = [f'"{kw}"' for kw in keywords]
    query = " OR ".join(phrases)

    languages = [entry["keyword"] for entry in config.get("langeuage", [])]
    if languages:
        lang_filter = " OR ".join(f"sourcelang:{lang.lower()}" for lang in languages)
        query = f"({query}) ({lang_filter})"

    return query


def _article_to_dict(a: GDELTArticle) -> dict:
    return dataclasses.asdict(a)


def _write_jsonl(path: Path, articles: list[GDELTArticle]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for a in articles:
            f.write(json.dumps(_article_to_dict(a), ensure_ascii=False) + "\n")


def main() -> int:
    outdir = Path("output")
    outdir.mkdir(parents=True, exist_ok=True)

    # Date range: previous 7 days
    end_dt = datetime.datetime.now()
    start_dt = end_dt - datetime.timedelta(days=7)

    # Load keywords from src/keywords.json
    config = _load_keywords()
    query = _build_query(config)
    print(f"Query: {query}", file=sys.stderr)
    print(f"Date range: {start_dt:%Y-%m-%d} → {end_dt:%Y-%m-%d}", file=sys.stderr)

    # 1. Fetch
    print("Fetching articles from GDELT...", file=sys.stderr)
    articles, _ = fetch_gdelt_articles(
        query=query, start_date=start_dt, end_date=end_dt,
        max_records=100,
    )
    _write_jsonl(outdir / "1_fetched.jsonl", articles)
    print(f"  → {len(articles)} articles fetched", file=sys.stderr)

    # 2. Dedupe
    unique, dupes = dedupe_articles(articles)
    _write_jsonl(outdir / "2_deduped.jsonl", unique)
    print(f"  → {len(unique)} unique, {len(dupes)} duplicates removed", file=sys.stderr)

    # 3. Cluster
    clusters = cluster_articles(unique, distance_threshold=0.6)
    clusters_out = [
        {
            "cluster_id": c.cluster_id,
            "label": c.label,
            "size": c.size,
            "articles": [_article_to_dict(a) for a in c.articles],
        }
        for c in clusters
    ]
    with open(outdir / "3_clusters.json", "w", encoding="utf-8") as f:
        json.dump(clusters_out, f, indent=2, ensure_ascii=False)
    print(f"  → {len(clusters)} clusters formed", file=sys.stderr)

    # 4. Score
    scored = score_clusters(clusters, now=end_dt)
    scored_out = [
        {
            "cluster_id": s.cluster.cluster_id,
            "label": s.cluster.label,
            "size": s.cluster.size,
            "score": round(s.score, 4),
            "signals": {k: round(v, 4) for k, v in s.signals.items()},
            "articles": [_article_to_dict(a) for a in s.cluster.articles],
        }
        for s in scored
    ]
    with open(outdir / "4_scored.json", "w", encoding="utf-8") as f:
        json.dump(scored_out, f, indent=2, ensure_ascii=False)
    print(f"  → {len(scored)} clusters scored", file=sys.stderr)

    # 5. Summarize
    summaries = summarize_clusters(scored)
    report = render_text_report(summaries)
    with open(outdir / "5_summary.txt", "w", encoding="utf-8") as f:
        f.write(report)
    summaries_out = [
        {
            "rank": s.rank,
            "headline": s.headline,
            "score": round(s.score, 4),
            "article_count": s.article_count,
            "source_count": s.source_count,
            "countries": s.countries,
            "date_range": s.date_range,
            "top_urls": s.top_urls,
        }
        for s in summaries
    ]
    with open(outdir / "5_summary.json", "w", encoding="utf-8") as f:
        json.dump(summaries_out, f, indent=2, ensure_ascii=False)
    print(f"  → {len(summaries)} trends summarized", file=sys.stderr)

    # Print report to stderr
    print("", file=sys.stderr)
    print(report, file=sys.stderr)

    # 6. Deliver (optional – only if email env vars are configured)
    if os.environ.get("EMAIL_SENDER") and os.environ.get("EMAIL_RECIPIENTS"):
        print("Sending email...", file=sys.stderr)
        try:
            send_email(summaries, report)
            print("  → Email sent successfully", file=sys.stderr)
        except Exception as exc:
            print(f"  → Email failed: {exc}", file=sys.stderr)
    else:
        print("Skipping email delivery (EMAIL_SENDER / EMAIL_RECIPIENTS not set)", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
