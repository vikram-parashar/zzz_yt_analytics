# ZZZ YouTube Analytics — Character Popularity & Meta Shift Tracker

## What Is This?

A data engineering project that analyzes YouTube content to quantify character popularity and detect meta shifts in Zenless Zone Zero. The system ingests YouTube data on a scheduled basis, transforms it into an analytics-ready schema in DuckDB, and produces time-series insights about emerging trends, declining characters, and banner/patch impact.

## Architecture

```
YouTube API ──► src/youtube.py ──► src/warehouse.py ──► DuckDB
Wiki scraping ─► src/agents.py  ──────────────────────────┘
Fuzzy matching ─► src/matching.py ────────────────────────┘
                                                      │
                                             Analytics / Dashboard (TODO)
```

### Data Model

| Table | Type | Description |
|---|---|---|
| `dim_agent` | Dimension | ZZZ characters with rank, attribute, faction |
| `dim_video` | Dimension | YouTube videos with title, description, channel |
| `dim_channel` | Dimension | YouTube channels |
| `bridge_agent_alias` | Bridge | Agent name variants for fuzzy matching |
| `bridge_video_agent` | Bridge | Video-to-agent associations with confidence scores |
| `fact_video_daily` | Fact | Daily snapshot of video engagement metrics |
| `fact_channel_daily` | Fact | Daily snapshot of channel subscriber/view counts |

## Project Structure

```
zzz_yt_analytics/
├── .github/workflows/daily-pipeline.yml
├── .env.example
├── .gitignore
├── main.py                    # CLI entrypoint — run any pipeline
├── pyproject.toml             # Dependencies (managed by uv)
└── src/
    ├── config.yaml            # DB path, daily ingestion limit
    ├── utils.py               # Config loader, logger, DB context manager, chunk helper
    ├── youtube.py             # ALL YouTube API calls (search, video stats, channel stats)
    ├── agents.py              # COMPLETE agent domain (wiki scrape → parse → load)
    ├── warehouse.py           # ALL DuckDB operations (schema, transforms, reads, writes)
    ├── matching.py            # Video-agent fuzzy matching
    └── data/aliases.json      # Agent name aliases for matching
```

## Local Development

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager
- YouTube Data API v3 key

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/vikram-parashar/zzz_yt_analytics.git
cd zzz_yt_analytics

# 2. Add your API key
cp .env.example .env
# Edit .env and set YT_DATA_API=your_key_here

# 3. Install dependencies
uv sync
uv pip install -e .
```

### Running Pipelines

```bash
# First-time setup (creates tables, scrapes agents, discovers videos, enriches metadata)
uv run python main.py setup

# Daily incremental pipeline (discovers new videos, enriches stats)
uv run python main.py daily
```

### Individual Commands

```bash
uv run python main.py init-tables        # Create DuckDB tables only
uv run python main.py scrape-agents      # Scrape agent data from wiki
uv run python main.py discover           # Daily video discovery
uv run python main.py initial-discover   # Full discovery (all agent topics)
uv run python main.py enrich-videos      # Enrich video metadata
uv run python main.py enrich-channels    # Enrich channel metadata
uv run python main.py match              # Build video-agent associations
```

### Ad-hoc Queries

```bash
uv run python main.py query "SELECT * FROM dim_agent LIMIT 10"
uv run python main.py query "SELECT COUNT(*) FROM fact_video_daily"
```

## GitHub Actions (Production)

The workflow at `.github/workflows/daily-pipeline.yml` runs `python main.py daily` every midnight UTC and commits the updated warehouse.

### Setup

1. GitHub → **Settings** → **Secrets and variables** → **Actions**
2. Add secret `YT_DATA_API` with your YouTube API key
3. The workflow runs automatically, or trigger manually from the **Actions** tab

## Design Decisions

### Why DuckDB?

Simplest analytics DB — no server, no config, one line to connect. Unless it struggles, no reason to upgrade.

### Agent Aliases

Manual aliases (`src/data/aliases.json`) are needed because:
- Surname-first naming causes false matches (e.g., "Demara" matches Nicole and Anby)
- Some characters are known by nicknames (Rina, Lucy)
- Variant spellings exist (Yixuan vs Yi Xuan)

### API Quota

| Endpoint | Cost | Notes |
|---|---|---|
| `search.list` | 100 units | Discovery — expensive, run sparingly |
| `videos.list` | 1 unit | Enrichment — cheap, run daily |
| `channels.list` | 1 unit | Enrichment — cheap, run daily |

## Current Stage

**Stage 1: Data Ingestion & Storage** — ETL pipeline functional, scheduled via GitHub Actions, DuckDB warehouse populated with daily fact snapshots.

### Next Steps

- implement dim_patch
- billy matches to starlight - billy videos, what do i do :<
