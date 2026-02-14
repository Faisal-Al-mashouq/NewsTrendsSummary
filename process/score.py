"""
Score and rank article clusters by trend importance.

Scoring signals (all derived from GDELTArticle fields):
  - cluster_size:     More articles → hotter topic
  - source_diversity: Unique domains covering the story
  - recency:          How recent the latest article is
  - geo_spread:       Number of distinct source countries

Each signal is normalized to [0, 1] across all clusters, then combined
with configurable weights into a final composite score.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field

from process.cluster import ArticleCluster


DEFAULT_WEIGHTS = {
    "keyword_relevance": 0.30,
    "cluster_size": 0.30,
    "source_diversity": 0.15,
    "recency": 0.15,
    "geo_spread": 0.10,
}


@dataclass
class ScoredCluster:
    cluster: ArticleCluster
    score: float
    signals: dict[str, float] = field(default_factory=dict)


def _parse_seendate(raw: str | None) -> datetime.datetime | None:
    """Parse GDELT seendate string (e.g. '20260214T120000Z') into datetime."""
    if not raw:
        return None
    cleaned = raw.replace("Z", "").replace("z", "").strip()
    for fmt in ("%Y%m%dT%H%M%S", "%Y-%m-%dT%H:%M:%S", "%Y%m%d%H%M%S"):
        try:
            return datetime.datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return None


def _keyword_relevance(cluster: ArticleCluster, keywords: list[str]) -> float:
    """Fraction of articles in the cluster whose title contains at least one keyword."""
    if not keywords:
        return 1.0
    lower_keywords = [kw.lower() for kw in keywords]
    matches = 0
    for a in cluster.articles:
        title_lower = a.title.lower()
        if any(kw in title_lower for kw in lower_keywords):
            matches += 1
    return matches / len(cluster.articles) if cluster.articles else 0.0


def _compute_raw_signals(
    cluster: ArticleCluster, now: datetime.datetime, keywords: list[str] | None = None,
) -> dict[str, float]:
    """Compute un-normalized signal values for a single cluster."""
    articles = cluster.articles

    # 0. Keyword relevance
    relevance = _keyword_relevance(cluster, keywords or [])

    # 1. Cluster size
    size = float(len(articles))

    # 2. Source diversity – unique domains
    domains = {a.domain for a in articles if a.domain}
    diversity = float(len(domains))

    # 3. Recency – hours since the most recent article (lower = more recent)
    parsed_dates = [_parse_seendate(a.seendate) for a in articles]
    valid_dates = [d for d in parsed_dates if d is not None]
    if valid_dates:
        latest = max(valid_dates)
        hours_ago = max((now - latest).total_seconds() / 3600.0, 0.0)
    else:
        hours_ago = 168.0  # fallback: 7 days

    # 4. Geographic spread – unique source countries
    countries = {a.sourcecountry for a in articles if a.sourcecountry}
    geo = float(len(countries))

    return {
        "keyword_relevance": relevance,
        "cluster_size": size,
        "source_diversity": diversity,
        "recency_hours_ago": hours_ago,
        "geo_spread": geo,
    }


def _normalize(values: list[float], invert: bool = False) -> list[float]:
    """Min-max normalize a list of values to [0, 1]. If invert, lower raw = higher score."""
    if not values:
        return []
    lo, hi = min(values), max(values)
    if hi == lo:
        return [1.0] * len(values)
    normalized = [(v - lo) / (hi - lo) for v in values]
    if invert:
        normalized = [1.0 - n for n in normalized]
    return normalized


def score_clusters(
    clusters: list[ArticleCluster],
    *,
    weights: dict[str, float] | None = None,
    now: datetime.datetime | None = None,
    keywords: list[str] | None = None,
) -> list[ScoredCluster]:
    """
    Score and rank clusters by composite trend importance.

    Returns ScoredCluster list sorted by score descending.
    """
    if not clusters:
        return []

    w = weights or DEFAULT_WEIGHTS
    now = now or datetime.datetime.now()

    # Compute raw signals for every cluster
    raw = [_compute_raw_signals(c, now, keywords) for c in clusters]

    # Extract per-signal lists and normalize
    relevances = _normalize([r["keyword_relevance"] for r in raw])
    sizes = _normalize([r["cluster_size"] for r in raw])
    diversities = _normalize([r["source_diversity"] for r in raw])
    recencies = _normalize([r["recency_hours_ago"] for r in raw], invert=True)
    geos = _normalize([r["geo_spread"] for r in raw])

    scored: list[ScoredCluster] = []
    for i, cluster in enumerate(clusters):
        signals = {
            "keyword_relevance": relevances[i],
            "cluster_size": sizes[i],
            "source_diversity": diversities[i],
            "recency": recencies[i],
            "geo_spread": geos[i],
        }
        composite = sum(signals[k] * w.get(k, 0.0) for k in signals)
        scored.append(ScoredCluster(cluster=cluster, score=composite, signals=signals))

    scored.sort(key=lambda s: s.score, reverse=True)
    return scored
