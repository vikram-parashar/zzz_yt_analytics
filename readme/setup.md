# Setup & Run

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager
- YouTube Data API v3 key
- (Optional) MotherDuck token for cloud DB

### Install

```bash
git clone https://github.com/vikram-parashar/zzz_yt_analytics.git
cd zzz_yt_analytics
cp .env.example .env        # set YT_DATA_API, MOTHERDUCK_TOKEN, MOTHERDUCK_DB
uv sync
uv pip install -e .
```

### Commands

| Command | What it does |
|---|---|
| `uv run main.py setup` | First-time: create all tables |
| `uv run main.py daily` | Full daily pipeline (auto-selects backfill or incremental) |
| `uv run main.py scrape-agents` | Scrape agent data from wiki |
| `uv run main.py scrape-banners` | Scrape banner schedule |
| `uv run main.py enrich` | Enrich video + channel metadata |
| `uv run main.py match-remaining` | Match unmatched videos to agents |
| `uv run main.py match-all` | Re-match ALL videos |
| `uv run main.py build-agent-daily` | Rebuild fact_agent_daily |
| `uv run main.py backup` | Backup MotherDuck → local file |
| `uv run main.py status` | Pipeline status overview |
| `uv run main.py query <sql>` | Run ad-hoc SQL |

### GitHub Actions

`.github/workflows/daily-pipeline.yml` runs `main.py daily` at midnight UTC. Set `YT_DATA_API` and `MOTHERDUCK_TOKEN` as repo secrets.
