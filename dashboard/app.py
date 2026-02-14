"""Minimal news-trends dashboard – run with `python dashboard/app.py`."""

import json, pathlib, html as _html
from http.server import HTTPServer, SimpleHTTPRequestHandler

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "output"

# ── data helpers ──────────────────────────────────────────────────────────────

def _load_json(name):
    p = OUTPUT / name
    return json.loads(p.read_text()) if p.exists() else []


def _pipeline_stats():
    fetched  = sum(1 for _ in open(OUTPUT / "1_fetched.jsonl")) if (OUTPUT / "1_fetched.jsonl").exists() else 0
    deduped  = sum(1 for _ in open(OUTPUT / "2_deduped.jsonl")) if (OUTPUT / "2_deduped.jsonl").exists() else 0
    clusters = _load_json("3_clusters.json")
    return fetched, deduped, len(clusters)


def _build_html():
    trends = _load_json("5_summary.json")
    scored = _load_json("4_scored.json")
    fetched, deduped, n_clusters = _pipeline_stats()

    # Collect all unique countries across all trends
    all_countries = set()
    for t in trends:
        all_countries.update(t.get("countries", []))

    # Build trend cards
    cards = []
    for t in trends:
        esc = _html.escape
        countries = ", ".join(t.get("countries", []))
        links = "".join(
            f'<a href="{esc(u)}" target="_blank">{esc(u.split("/")[2])}</a>'
            for u in t.get("top_urls", [])[:3]
        )
        score_pct = t["score"] * 100
        bar_color = "#22c55e" if score_pct > 60 else "#eab308" if score_pct > 30 else "#64748b"
        cards.append(f"""
        <div class="card">
          <div class="card-header">
            <span class="rank">#{t['rank']}</span>
            <h3>{esc(t['headline'])}</h3>
          </div>
          <div class="score-bar-bg"><div class="score-bar" style="width:{score_pct:.0f}%;background:{bar_color}"></div></div>
          <div class="meta">
            <span>Score <b>{t['score']:.2f}</b></span>
            <span>Articles <b>{t['article_count']}</b></span>
            <span>Sources <b>{t['source_count']}</b></span>
            <span>Countries <b>{len(t.get('countries', []))}</b></span>
          </div>
          <div class="details">
            <p class="countries">{esc(countries)}</p>
            <p class="date">{esc(t.get('date_range', ''))}</p>
            <div class="links">{links}</div>
          </div>
        </div>""")

    # Signal breakdown for top trend
    signal_section = ""
    if scored:
        top = scored[0]
        sigs = top.get("signals", {})
        signal_section = f"""
        <div class="signals">
          <h3>Top Trend Signal Breakdown</h3>
          <div class="signal-grid">
            {"".join(f'<div class="signal"><span class="signal-label">{k.replace("_"," ").title()}</span><div class="score-bar-bg"><div class="score-bar" style="width:{v*100:.0f}%;background:#6366f1"></div></div><span class="signal-val">{v:.2f}</span></div>' for k,v in sigs.items())}
          </div>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>News Trends Dashboard</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:2rem}}
h1{{font-size:1.6rem;margin-bottom:.3rem}}
.subtitle{{color:#94a3b8;margin-bottom:1.5rem;font-size:.9rem}}
.stats{{display:flex;gap:1rem;flex-wrap:wrap;margin-bottom:2rem}}
.stat{{background:#1e293b;border-radius:10px;padding:1rem 1.4rem;min-width:140px;text-align:center}}
.stat .num{{font-size:1.8rem;font-weight:700;color:#38bdf8}}
.stat .label{{font-size:.75rem;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em;margin-top:.2rem}}
.card{{background:#1e293b;border-radius:10px;padding:1.2rem 1.4rem;margin-bottom:1rem}}
.card-header{{display:flex;align-items:baseline;gap:.7rem;margin-bottom:.6rem}}
.rank{{font-size:1.1rem;font-weight:700;color:#38bdf8;white-space:nowrap}}
h3{{font-size:1rem;font-weight:600;line-height:1.3}}
.score-bar-bg{{background:#334155;border-radius:4px;height:6px;width:100%;margin-bottom:.6rem}}
.score-bar{{height:6px;border-radius:4px;transition:width .4s}}
.meta{{display:flex;gap:1.2rem;flex-wrap:wrap;font-size:.8rem;color:#94a3b8;margin-bottom:.5rem}}
.meta b{{color:#e2e8f0}}
.details{{font-size:.78rem;color:#64748b}}
.countries{{margin-bottom:.2rem}}
.links{{display:flex;gap:.6rem;flex-wrap:wrap;margin-top:.3rem}}
.links a{{color:#38bdf8;text-decoration:none}}
.links a:hover{{text-decoration:underline}}
.signals{{background:#1e293b;border-radius:10px;padding:1.2rem 1.4rem;margin-bottom:2rem}}
.signals h3{{font-size:.95rem;margin-bottom:.8rem;color:#a5b4fc}}
.signal-grid{{display:grid;gap:.5rem}}
.signal{{display:grid;grid-template-columns:140px 1fr 40px;align-items:center;gap:.6rem;font-size:.82rem}}
.signal-label{{color:#94a3b8;text-transform:capitalize}}
.signal-val{{text-align:right;font-weight:600;color:#e2e8f0}}
</style></head><body>
<h1>News Trends Dashboard</h1>
<p class="subtitle">Weekly pipeline results &middot; data from <code>output/</code></p>
<div class="stats">
  <div class="stat"><div class="num">{fetched}</div><div class="label">Fetched</div></div>
  <div class="stat"><div class="num">{deduped}</div><div class="label">After Dedup</div></div>
  <div class="stat"><div class="num">{n_clusters}</div><div class="label">Clusters</div></div>
  <div class="stat"><div class="num">{len(trends)}</div><div class="label">Top Trends</div></div>
  <div class="stat"><div class="num">{len(all_countries)}</div><div class="label">Countries</div></div>
</div>
{signal_section}
{"".join(cards)}
</body></html>"""


# ── server ────────────────────────────────────────────────────────────────────

class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        body = _build_html().encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # quiet logs


if __name__ == "__main__":
    HTTPServer.allow_reuse_address = True
    PORT = 8050
    print(f"Dashboard → http://localhost:{PORT}")
    HTTPServer(("", PORT), Handler).serve_forever()
