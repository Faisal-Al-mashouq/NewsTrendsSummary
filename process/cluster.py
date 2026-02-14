"""
Cluster deduplicated articles by title similarity using TF-IDF + agglomerative clustering.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sklearn.cluster import AgglomerativeClustering
from sklearn.feature_extraction.text import TfidfVectorizer

from ingest.gdelt_fetch import GDELTArticle


@dataclass
class ArticleCluster:
    cluster_id: int
    articles: list[GDELTArticle] = field(default_factory=list)

    @property
    def size(self) -> int:
        return len(self.articles)

    @property
    def label(self) -> str:
        """Use the title of the first (representative) article as the cluster label."""
        return self.articles[0].title if self.articles else ""


def cluster_articles(
    articles: list[GDELTArticle],
    *,
    distance_threshold: float = 0.6,
) -> list[ArticleCluster]:
    """
    Cluster articles by TF-IDF cosine similarity on titles.

    Uses agglomerative clustering with a cosine distance threshold.
    Returns clusters sorted by size descending (biggest = hottest trend).
    """
    if not articles:
        return []

    if len(articles) == 1:
        return [ArticleCluster(cluster_id=0, articles=list(articles))]

    titles = [a.title for a in articles]

    tfidf = TfidfVectorizer(stop_words="english")
    X = tfidf.fit_transform(titles)

    clustering = AgglomerativeClustering(
        n_clusters=None,
        distance_threshold=distance_threshold,
        metric="cosine",
        linkage="average",
    )
    labels = clustering.fit_predict(X.toarray())

    buckets: dict[int, list[GDELTArticle]] = {}
    for label, article in zip(labels, articles):
        buckets.setdefault(int(label), []).append(article)

    clusters = [
        ArticleCluster(cluster_id=cid, articles=arts)
        for cid, arts in buckets.items()
    ]
    clusters.sort(key=lambda c: c.size, reverse=True)

    # Re-number after sorting so cluster_id 0 = biggest
    for i, c in enumerate(clusters):
        c.cluster_id = i

    return clusters
