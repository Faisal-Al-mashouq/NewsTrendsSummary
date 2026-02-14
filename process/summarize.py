"""
Generate a human-readable trend summary from scored clusters.

This is a purely extractive approach (no LLM required):
  - Picks the most representative article title per cluster as headline
  - Aggregates metadata (sources, countries, date range) into a blurb
  - Produces a ranked trend report ready for delivery
"""

from __future__ import annotations

from dataclasses import dataclass

from process.score import ScoredCluster, _parse_seendate


@dataclass
class TrendSummary:
    rank: int
    headline: str
    score: float
    article_count: int
    source_count: int
    countries: list[str]
    date_range: str
    top_urls: list[str]


def _date_range_str(scored: ScoredCluster) -> str:
    """Return a compact 'MMM DD – MMM DD' string from the cluster's articles."""
    dates = [
        _parse_seendate(a.seendate)
        for a in scored.cluster.articles
        if a.seendate
    ]
    valid = [d for d in dates if d is not None]
    if not valid:
        return "unknown"
    earliest = min(valid)
    latest = max(valid)
    if earliest.date() == latest.date():
        return earliest.strftime("%b %d, %Y")
    return f"{earliest.strftime('%b %d')} – {latest.strftime('%b %d, %Y')}"


def summarize_clusters(
    scored: list[ScoredCluster],
    *,
    max_trends: int = 10,
    max_urls_per_trend: int = 5,
) -> list[TrendSummary]:
    """
    Convert scored clusters into a ranked list of TrendSummary objects.

    Args:
        scored: Clusters pre-sorted by score descending (from score_clusters).
        max_trends: Maximum number of trends to include.
        max_urls_per_trend: Maximum article URLs to list per trend.
    """
    summaries: list[TrendSummary] = []

    for rank, sc in enumerate(scored[:max_trends], start=1):
        cluster = sc.cluster
        articles = cluster.articles

        domains = {a.domain for a in articles if a.domain}
        countries = sorted({a.sourcecountry for a in articles if a.sourcecountry})
        urls = [a.url for a in articles[:max_urls_per_trend]]

        summaries.append(
            TrendSummary(
                rank=rank,
                headline=cluster.label,
                score=sc.score,
                article_count=cluster.size,
                source_count=len(domains),
                countries=countries,
                date_range=_date_range_str(sc),
                top_urls=urls,
            )
        )

    return summaries


def render_text_report(summaries: list[TrendSummary]) -> str:
    """Render trend summaries as a plain-text report."""
    lines: list[str] = []
    lines.append("=" * 60)
    lines.append("  NEWS TRENDS SUMMARY")
    lines.append("=" * 60)
    lines.append("")

    for s in summaries:
        lines.append(f"#{s.rank}  {s.headline}")
        lines.append(f"    Score: {s.score:.2f}  |  {s.article_count} articles  |  {s.source_count} sources")
        if s.countries:
            lines.append(f"    Countries: {', '.join(s.countries)}")
        lines.append(f"    Date range: {s.date_range}")
        lines.append("    Links:")
        for url in s.top_urls:
            lines.append(f"      - {url}")
        lines.append("")

    lines.append("=" * 60)
    return "\n".join(lines)
