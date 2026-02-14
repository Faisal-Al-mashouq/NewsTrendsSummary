#!/usr/bin/env python3
"""
Fetch articles from GDELT DOC 2.0 API (Doc API v2).

Core endpoint:
  https://api.gdeltproject.org/api/v2/doc/doc

Common params used here:
  query=...
  mode=artlist
  format=json
  maxrecords=...
  startdatetime=YYYYMMDDHHMMSS
  enddatetime=YYYYMMDDHHMMSS

GDELT examples reference these params and mode usage.
"""

import argparse
import datetime
import json
import re
import sys
import time
from dataclasses import dataclass
from typing import Any

import httpx


GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
_MAX_RETRIES = 3

@dataclass(frozen=True)
class GDELTArticle:
    url: str
    title: str
    seendate: str | None = None
    sourcecountry: str | None = None
    sourcelanguage: str | None = None
    domain: str | None = None
    socialimage: str | None = None

def _format_datetime(dt: datetime.datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")

def _parse_articles(payload: dict[str, Any]) -> list[GDELTArticle]:
    """GDELT returns JSON with 'articles' key containing a list of article dicts."""

    articles = payload.get("articles", []) or payload.get("data", [])
    output: list[GDELTArticle] = []
    for a in articles:
        if not isinstance(a, dict):
            continue
        article_url = a.get("url") or a.get("sourceCollectionIdentifier")
        article_title = a.get("title") or ""
        if not article_url or not article_title:
            continue
        output.append(
            GDELTArticle(
                url=article_url,
                title=article_title,
                seendate=a.get("seendate"),
                sourcecountry=a.get("sourcecountry"),
                sourcelanguage=a.get("language") or a.get("sourcelang"),
                domain=a.get("domain"),
                socialimage=a.get("socialimage"),
            )
        )
    return output

def fetch_gdelt_articles(
        query: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        *,
        max_records: int = 100,
        sort: str = "datedesc",
        timeout_s: float = 60.0,
    ) -> tuple[list[GDELTArticle], dict[str, Any]]:
    """
    Fetch articles from GDELT Doc API v2 matching the query and date range.
    Returns a tuple of (list of GDELTArticle, raw API response dict).
    Retries up to _MAX_RETRIES times on timeout.
    """
    params = {
        "query": query,
        "mode": "artlist",
        "format": "json",
        "maxrecords": str(max_records),
        "sort": sort,
        "startdatetime": _format_datetime(start_date),
        "enddatetime": _format_datetime(end_date),
    }

    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            with httpx.Client(timeout=timeout_s) as client:
                response = client.get(GDELT_DOC_API, params=params)
                response.raise_for_status()

                text = response.text.strip()
                if not text or not text.startswith("{"):
                    if text:
                        sys.stderr.write(f"GDELT non-JSON response: {text[:200]}\n")
                    return [], {}
                # GDELT sometimes returns malformed JSON (trailing commas)
                cleaned = re.sub(r",\s*([}\]])", r"\1", text)
                payload = json.loads(cleaned)

            articles = _parse_articles(payload)
            return articles, payload
        except httpx.TimeoutException as exc:
            last_exc = exc
            wait = 2 ** attempt
            sys.stderr.write(f"GDELT timeout (attempt {attempt + 1}/{_MAX_RETRIES}), retrying in {wait}s...\n")
            time.sleep(wait)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429:
                last_exc = exc
                wait = 2 ** (attempt + 2)  # longer backoff for rate limits
                sys.stderr.write(f"GDELT rate limit (attempt {attempt + 1}/{_MAX_RETRIES}), retrying in {wait}s...\n")
                time.sleep(wait)
            else:
                raise

    raise last_exc  # type: ignore[misc]
    
def _parse_dt(s: str) -> datetime.datetime:
    """
    Accepts:
      - ISO-like: 2026-02-14T00:00:00
      - Date only: 2026-02-14 (assumes 00:00:00)
    Interpreted as local-naive; you can standardize later.
    """
    if "T" in s:
        return datetime.datetime.fromisoformat(s)
    return datetime.datetime.fromisoformat(s + "T00:00:00")

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Fetch articles from GDELT Doc API v2.")
    ap.add_argument("--query", required=True, help="GDELT query string")
    ap.add_argument("--start", required=True, help="Start date (YYYY-MM-DD or ISO)")
    ap.add_argument("--end", required=True, help="End date (YYYY-MM-DD or ISO)")
    ap.add_argument("--maxrecords", type=int, default=100, help="Max records to fetch")
    ap.add_argument("--sort", default="datedesc", help="Sort order (e.g. datedesc, dateasc)")
    ap.add_argument("--out", default="-", help="Output JSON file (defaults to stdout)")
    args = ap.parse_args(argv)

    start_dt = _parse_dt(args.start)
    end_dt = _parse_dt(args.end)

    articles, meta = fetch_gdelt_articles(
        query=args.query,
        start_date=start_dt,
        end_date=end_dt,
        max_records=args.maxrecords,
        sort=args.sort,
    )

    lines = []

    for a in articles:
        lines.append(json.dumps({
            "url": a.url,
            "title": a.title,
            "seendate": a.seendate,
            "sourcecountry": a.sourcecountry,
            "sourcelanguage": a.sourcelanguage,
            "domain": a.domain,
            "socialimage": a.socialimage,
        }, ensure_ascii=False))
    
    if args.out == "-":
        sys.stdout.write("\n".join(lines) + ("\n" if lines else ""))
    else:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + ("\n" if lines else ""))

    # Print a small stderr summary (so piping JSONL is clean)
    sys.stderr.write(f"Fetched {len(articles)} articles\n")
    if isinstance(meta, dict) and "timeline" in meta:
        sys.stderr.write("Note: response includes 'timeline'\n")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
