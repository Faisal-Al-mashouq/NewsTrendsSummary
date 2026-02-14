from ingest.gdelt_fetch import GDELTArticle

import hashlib
import re
import urllib.parse


_TRACKING_KEYS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "igshid", "mc_cid", "mc_eid",
}

def _normalize_title(title: str) -> str:
    return " ".join(title.strip().lower().split())

def _canonicalize_url(url: str) -> str:
    """
    Lightweight URL canonicalization for dedupe.
    - lowercases scheme/host
    - strips fragments
    - drops common tracking query params
    - normalizes repeated slashes
    """
    u = urllib.parse.urlsplit(url.strip())
    scheme = (u.scheme or "https").lower()
    netloc = u.netloc.lower()

    path = re.sub(r"/{2,}", "/", u.path or "/")

    q = urllib.parse.parse_qsl(u.query, keep_blank_values=True)
    q2 = [(k, v) for (k, v) in q if k not in _TRACKING_KEYS]
    query = urllib.parse.urlencode(q2, doseq=True)

    return urllib.parse.urlunsplit((scheme, netloc, path, query, ""))  # drop fragment

def dedupe_articles(articles: list[GDELTArticle]) -> tuple[list[GDELTArticle], list[GDELTArticle]]:
    """Dedupe articles by canonicalized URL."""
    seen_url = set()
    seen_fallback = set()

    unique: list[GDELTArticle] = []
    dupes: list[GDELTArticle] = []

    for a in articles:
        article_url = str(a.url).strip()
        article_title = str(a.title).strip()
        if not article_url or not article_title:
            dupes.append(a)
            continue
        
        url_c = _canonicalize_url(article_url)
        if url_c in seen_url:
            dupes.append(a)
            continue
        
        title_n = _normalize_title(article_title)
        fallback = hashlib.sha256((title_n + "|" + url_c.split("?", 1)[0]).encode("utf-8")).hexdigest()

        if fallback in seen_fallback:
            dupes.append(a)
            continue

        seen_url.add(url_c)
        seen_fallback.add(fallback)
        unique.append(a)

    return unique, dupes