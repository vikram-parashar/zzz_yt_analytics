# Ingestion: Backfill & Daily

Two discovery strategies using YouTube `search.list` (100 quota units each). Daily always runs first, then backfill.

## Daily Pipeline Order

`main.py daily` runs: **daily_popular → daily_random → backfill_popular → backfill_random → scrape agents → scrape banners → enrich → match remaining → build fact_agent_daily → backup**. Idempotency guard: skips if a `completed` run already exists for today.

## Type I — Popular (`order=viewCount`)

- **Daily**: Searches last 48 hours, `order=viewCount`.
- **Backfill**: Walks **backward day-by-day** starting from yesterday's yestesday. {TYPE1_MAX_SEARCHES} search per day. State tracked in `pipeline_info` (`type1_last_day` = oldest processed day). Marked `type1_completed` when `date < 2024-01-01`.

## Type II — Random (`order=date`)

- **Daily**: Runs every 3rd day (`day % 3 == 0`). Picks a random timestamp between now and 3 month ago, searches with `publishedBefore=that_timestamp`.
- **Backfill**: Walks **backward month-by-month** starting from yesterday's month. For each month, runs `TYPE2_SEARCHES_PER_MONTH` (10) searches with random timestamps. State tracked via `type2_current_month`, `type2_searches_done`. Marked `type2_completed` when `month < 2024-01-01`.

Both types insert into `dim_video` (with `discovery_type` tag) and `dim_channel` (if new). Each backfill type completes independently.

### Throttle

2-second delay between search API calls. YouTube API retry with exponential backoff on 429/403 rate-limit errors.
