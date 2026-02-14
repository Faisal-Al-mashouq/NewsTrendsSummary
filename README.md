# NewsTrendsSummary

Automated news trend extraction and delivery pipeline. Fetches articles from the GDELT API, deduplicates, clusters by similarity, scores by trend importance, and delivers a weekly summary via email.

## Pipeline

1. **Fetch** - Queries GDELT for articles matching keywords in `src/keywords.json` (past 7 days)
2. **Dedupe** - Removes duplicates by URL canonicalization and title similarity
3. **Cluster** - Groups articles using TF-IDF vectorization + agglomerative clustering
4. **Score** - Ranks clusters by keyword relevance, size, source diversity, recency, and geographic spread
5. **Summarize** - Generates a top-10 trends report (JSON + plain text)
6. **Deliver** - Sends an HTML-formatted email to configured recipients

## Setup

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

Copy `.env.example` or create a `.env` file with:

```
EMAIL_SENDER=you@gmail.com
EMAIL_PASSWORD=your-app-password
EMAIL_RECIPIENTS=recipient@example.com
```

## Usage

### Run once

```bash
python main.py
```

### Weekly scheduler

Runs the pipeline every Sunday at 9:00 AM (Asia/Riyadh):

```bash
python scheduler.py
```

To run in the background:

```bash
nohup uv run python scheduler.py > scheduler.log 2>&1 &
```

Check if the scheduler is running:

```bash
ps aux | grep 'scheduler.py' | grep -v grep
```

Stop the scheduler:

```bash
# Find the PID from the ps command above, then:
kill <PID>
```

## Project Structure

```
ingest/          GDELT API fetching
process/         Deduplication, clustering, scoring, summarization
deliver/         Email delivery
src/             Configuration (keywords, languages, countries)
output/          Intermediate pipeline outputs (auto-generated)
```
