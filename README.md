# ZZZ YouTube Analytics

A data engineering project that ingests Zenless Zone Zero (ZZZ) character data from YouTube and web sources to quantify character popularity. The pipeline backfills YouTube data from Jan 2024, ingests on a daily schedule, transforms it into an analytics-ready schema in DuckDB, and updates a Next.js dashboard at [zzz-yt-trends.vercel.app](https://zzz-yt-trends.vercel.app) every day around 9:30 AM IST.

**Live Dashboard:** [zzz-yt-trends.vercel.app](https://zzz-yt-trends.vercel.app)

<!-- ![Dashboard Preview](dashboard-preview.png) -->

---

## Table of Contents

- [Architecture](#architecture)
- [Data Model](#data-model)
- [How It Works](#how-it-works)
  - [Discovery: Backfill Strategy](#discovery-backfill-strategy)
  - [Discovery: Daily Pipeline](#discovery-daily-pipeline)
  - [Matching Algorithm](#matching-algorithm)
  - [Attribution System](#attribution-system)
- [Dashboard](#dashboard)
  - [Main Page](#main-page)
  - [Agent Detail Page](#agent-detail-page)
- [Setup](#setup)
  - [Local Setup](#local-setup)
  - [Dashboard Setup](#dashboard-setup)
  - [Getting a YouTube API Key](#getting-a-youtube-api-key)
  - [Adding YT_DATA_API as a GitHub Secret](#adding-yt_data_api-as-a-github-secret)
- [YouTube API Quota Math](#youtube-api-quota-math)
- [CLI Commands](#cli-commands)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)

---

## Architecture

<!-- ![Architecture Diagram](docs/architecture.png) -->

The system has three layers:

1. **CI/CD (GitHub Actions)** — A scheduled workflow runs daily (~9:30 AM IST). It restores the warehouse from the previous run's artifact, runs the pipeline, exports parquet files for the dashboard, and uploads the updated warehouse as a new artifact.

2. **Python Pipeline** — The core ETL. It scrapes agent data and banner schedules from the web, discovers and enriches YouTube videos via the Data API v3, matches videos to characters using fuzzy text matching, and builds daily aggregates with an attribution system. Everything lands in DuckDB.

3. **Next.js Dashboard** — Fully client-side. DuckDB-WASM runs in the browser and loads 11 parquet files exported from the warehouse. No backend server needed — the entire analytics database lives in the user's browser.

---

## Data Model

<!-- ![Data Model](docs/datamodel.png) -->

### Dimensions

| Table | PK | What it stores |
|---|---|---|
| `dim_agent` | `name` | ZZZ characters — name, image, rank (S/A), attribute, speciality, faction, release date |
| `dim_video` | `video_id` | YouTube videos — title, description, channel, published_at, tags, duration, latest view/like/comment counts, discovery type |
| `dim_channel` | `channel_id` | YouTube channels — name, thumbnail, country |
| `dim_patch` | `(version, agent_name)` | Banner schedule — game version, agent, start/end dates |

### Bridge Tables

| Table | PK | What it stores |
|---|---|---|
| `bridge_agent_alias` | `(name, alias)` | Searchable aliases per agent for fuzzy matching (65 agents) |
| `bridge_video_agent` | `(video_id, agent_name)` | Video↔Agent associations with `confidence` score and `attribution_weight` |

### Fact Tables (daily snapshots)

| Table | PK | What it stores |
|---|---|---|
| `fact_video_daily` | `(video_id, snapshot_date)` | Daily video metrics — views, likes, comments |
| `fact_channel_daily` | `(channel_id, snapshot_date)` | Daily channel metrics — subscribers, views, video count |
| `fact_agent_daily` | `(agent_name, snapshot_date)` | Attribution-weighted aggregates — attributed views/likes/comments, video count |

### Operational Tables

| Table | PK | What it stores |
|---|---|---|
| `pipeline_runs` | `id` | Run tracking — pipeline name, status, timing, error info |
| `pipeline_info` | `key` | Key-value store for pipeline state (backfill progress, etc.) |

---

## How It Works

### Discovery: Backfill Strategy

Historical data from Jan 2024 onwards is filled using two complementary sampling strategies that run until caught up, then become no-ops.

**Type I — Popular by viewCount** (`TYPE1_MAX_SEARCHES = 60` per run)

Walks day-by-day from the start date, searching YouTube with `order=viewCount` for each day's window. This captures the most popular videos for each date — the ones that drove the most views. Since it processes one day per search, it needs ~550 searches to cover Jan 2024 → present, which at 60 searches/run takes ~9 daily runs to complete.

**Type II — Random by date** (`TYPE2_MAX_SEARCHES = 20` per run)

For each completed month, picks 10 random timestamps within that month and searches with `order=date` using `publishedAfter=month_start` and `publishedBefore=random_timestamp`. This samples the long tail of videos that aren't necessarily popular but are still relevant. For the current month, it does `day//3` searches instead. This avoids the popularity bias of Type I and catches niche content.

**Why two strategies?** Type I gives you the hits — videos that blew up. Type II gives you coverage — videos that exist but didn't trend. Together they paint a more complete picture of each character's YouTube presence. The pipeline tracks progress in `pipeline_info` and resumes from where it left off on each run.

### Discovery: Daily Pipeline

Once backfill is complete, the daily pipeline switches to incremental mode:

- **Type I daily:** One search with `order=viewCount`, `publishedAfter=now()-27h`, `publishedBefore=now()`. Catches popular videos from the last day.
- **Type II daily:** One search with `order=date` every 3 days (when `day%3==0`). Uses a random timestamp from the last 3 days as `publishedAfter`. Catches recent niche content without burning quota daily.

After discovery, the pipeline enriches video stats and channel stats (cheap API calls at 1 unit each), runs matching, and rebuilds the `fact_agent_daily` aggregate for today.

### Matching Algorithm

Videos are matched to agents using a weighted fuzzy text matching system in `src/matching.py`. Here's how it works:

1. **Normalization** — Strip special characters (`_`, `/`, `-`, `|`), remove possessives (`'s`), collapse whitespace, lowercase everything.

2. **Word matching** — For each alias in `bridge_agent_alias`, check if it appears as a whole word (`\b` bounded) in the video's title, description, or tags. CJK names use substring matching instead of word boundaries since CJK doesn't have word separators.

3. **Scoring weights:**

   | Field | Weight | Rationale |
   |-------|--------|-----------|
   | Title | 10 | If a character name is in the title, the video is definitely about them |
   | Tags | 5 | Tags are a strong signal but less prominent than titles |
   | Description | 2 | Descriptions are noisy — a mention doesn't mean the video is about that character |

4. **Tag dilution** — If multiple different agents match in the tags, each agent's tag score gets multiplied by `1 / (1 + log(num_tag_agents))`. A video tagging 5 different agents shouldn't give full credit to each one.

5. **Best alias wins** — If multiple aliases for the same agent match, the one with the highest score wins. Ties are broken by alias length (longer alias = more specific = wins).

### Attribution System

A video mentioning 3 agents shouldn't give each one 100% credit for its views. The attribution system solves this:

1. **Confidence → Weight:** For each video, `attribution_weight = confidence / SUM(all confidences for this video)`. Weights per video always sum to 1.

2. **Attributed metrics:** `fact_agent_daily.attributed_views = SUM(attribution_weight × view_count)` across all videos for that agent on that date. A video with 10,000 views split 60/40 between two agents gives 6,000 and 4,000 attributed views respectively.

This means a character's "popularity" in the dashboard reflects their share of attention, not raw view counts that would double-count videos about multiple characters.

---

## Dashboard

The dashboard is a Next.js app that runs DuckDB-WASM entirely in the browser. It loads 11 parquet files from `/public/data/` into an in-memory database and runs SQL queries client-side. No backend, no API, no server costs.

### Main Page

**Agent Popularity & Trends** — A line chart showing the top 5 agents by monthly video count over a selectable date range. This reveals which characters are gaining or losing YouTube attention over time. Useful for spotting meta shifts — when a new banner drops, you can see the corresponding spike in content creation.

**Banner Sudden Popularity** — A bar chart showing view gain per agent during a specific banner version. When you select a version, it calculates the attributed view difference between the banner period and the preceding period, showing which characters got the biggest "banner bump". This directly measures banner impact on YouTube engagement.

**Rising ZZZ Creators** — A table ranking channels by a composite growth score: 50% subscriber growth + 30% view growth + 20% video count growth. This surfaces content creators who are accelerating their ZZZ coverage — useful for identifying emerging influencers in the community.

**Agent Cards Grid** — A filterable, sortable grid of all agents showing their attributed view counts, like counts, and video counts. Filter by rank (S/A), attribute, or faction. Click any card to dive into the agent detail page.

### Agent Detail Page

**Agent Header** — Shows the character's image, badges for rank/attribute/speciality/faction, and an "On Banner" indicator if they're currently featured.

**Recent Engagement Trend** — A line chart of attributed views and likes over the past 30 days from `fact_agent_daily`. Shows whether a character is trending up or down in engagement.

**Videos Published Per Month** — A line chart of monthly video count for this agent, with shaded reference areas marking banner periods. This makes it obvious when a banner drives content creation — you see the video count jump during banner months and fade after.

**Most Liked Videos** — A table of top videos ranked by a Bayesian average like score (`(likes + prior) / (views + prior)`, where prior = 1000 views). This prevents a video with 10 likes on 20 views from ranking above one with 10,000 likes on 100,000 views. Each video links to YouTube.

**Top Channels by Views** — Channels ranked by attributed views for this agent. Shows which content creators dominate the conversation for a specific character.

**Co-occurring Agents** — Other agents that frequently appear in the same videos. A video tagged with both "Miyabi" and "Yanagi" would count as a co-occurrence. This reveals which character pairings and team comps generate the most content.

---

## Setup

### Local Setup

**Prerequisites:** Python 3.13+, [uv](https://docs.astral.sh/uv/), YouTube Data API v3 key

```bash
# 1. Clone
git clone https://github.com/vikram-parashar/zzz_yt_analytics.git
cd zzz_yt_analytics

# 2. Add your API key
cp .env.example .env
# Edit .env → YT_DATA_API=your_key_here

# 3. Install deps
uv sync
uv pip install -e .

# 4. First-time setup (creates tables + scrapes agents + banners)
uv run python main.py setup

# 5. Start backfill (runs both types, resumes on re-run)
uv run python main.py backfill

# 6. Run daily pipeline
uv run python main.py daily

# 7. Check status
uv run python main.py status
```

### Dashboard Setup

```bash
cd dashboard
npm install
npm run dev      # Development server at localhost:3000
npm run build    # Production build
```

The dashboard loads parquet files from `dashboard/public/data/`. These are exported from the warehouse by the GitHub Actions workflow. If you're running locally, you'll need to export them manually or the dashboard will show the last committed data.

### Getting a YouTube API Key

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select an existing one)
3. Navigate to **APIs & Services** → **Library**
4. Search for **YouTube Data API v3** and enable it
5. Go to **APIs & Services** → **Credentials**
6. Click **Create Credentials** → **API Key**
7. Copy the key — this is your `YT_DATA_API` value
8. (Recommended) Restrict the key to only the YouTube Data API v3

### Adding YT_DATA_API as a GitHub Secret

1. Go to your repo on GitHub
2. **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Name: `YT_DATA_API`
5. Value: paste your API key
6. Click **Add secret**

The daily pipeline workflow reads this secret and sets it as an environment variable during the run.

---

## YouTube API Quota Math

The YouTube Data API v3 gives you **10,000 quota units per day** on the free tier. Quota resets at midnight Pacific Time. Here's how this project uses them:

### API Costs

| Endpoint | Cost per call | What it does |
|---|---|---|
| `search.list` | 100 units | Discover videos by keyword, date range, sort order |
| `videos.list` | 1 unit | Fetch stats (views, likes, comments) for up to 50 videos |
| `channels.list` | 1 unit | Fetch stats (subscribers, views) for up to 50 channels |

### Daily Quota Breakdown (once backfill is complete)

| Operation | Searches | Units | Notes |
|---|---|---|---|
| Daily Type I discover | 1 | 100 | `order=viewCount`, last 27 hours |
| Daily Type II discover | 0–1 | 0–100 | Only on days where `day%3==0` |
| Enrich videos | ~N/50 calls | ~N/50 | N = total videos, 50 per call, 1 unit each |
| Enrich channels | ~M/50 calls | ~M/50 | M = total channels, 50 per call, 1 unit each |
| **Total** | | **~200–300** | For ~5000 videos and ~2000 channels |

Daily usage is well under 1,000 units — plenty of headroom.

### Backfill Quota Breakdown (while catching up)

| Operation | Searches | Units | Notes |
|---|---|---|---|
| Type I backfill | 60 | 6,000 | 60 searches per run |
| Type II backfill | 20 | 2,000 | 20 searches per run |
| Enrichment | varies | ~500–1,000 | New videos/channels from backfill |
| **Total** | | **~8,500–9,000** | Per run during backfill phase |

During backfill, daily usage is close to the 10,000 limit. The 2-second delay between searches (`SEARCH_DELAY_SECONDS = 2.0`) and the per-run caps (`TYPE1_MAX_SEARCHES = 60`, `TYPE2_MAX_SEARCHES = 20`) keep search costs at 8,000 units, leaving ~2,000 for enrichment. If enrichment exceeds that, the pipeline's retry logic handles quota errors gracefully.

### What happens if you hit the limit?

The pipeline has retry logic with exponential backoff for 429 (rate limit) errors. If it exhausts retries, it raises an error and the pipeline run is marked as failed in `pipeline_runs`. Since backfill progress is saved in `pipeline_info`, the next run will resume from where it left off — no data is lost.

---

## CLI Commands

```
uv run python main.py <command>

setup              First-time setup: init tables + scrape agents + banners
daily              Daily incremental pipeline (scrape → discover → enrich → match → aggregate)
backfill           Run both Type I + Type II backfill
backfill-popular   Type I only: popular by viewCount, 60 searches/run
backfill-random    Type II only: random by date, 20 searches/run
init-tables        Create DuckDB tables only
scrape-agents      Scrape agent data from wiki
scrape-banners     Scrape banner schedule from gamerant.com
enrich-videos      Fetch up-to-date stats for all known videos
enrich-channels    Fetch up-to-date stats for all known channels
match              Rebuild all video-agent associations
build-agent-daily  Rebuild fact_agent_daily aggregates
publish            Checkpoint WAL + create versioned warehouse snapshot
status             Show current pipeline state
query <sql>        Run an ad-hoc SQL query against the warehouse
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Pipeline | Python 3.13, DuckDB, pandas, pendulum |
| Web Scraping | BeautifulSoup4, requests |
| YouTube API | google-api-python-client |
| Package Management | uv |
| Dashboard | Next.js 16, React 19, DuckDB-WASM |
| Charts | Recharts 3 |
| Styling | Tailwind CSS 4, DaisyUI 5, Catppuccin theme |
| CI/CD | GitHub Actions (artifact-based persistence) |
