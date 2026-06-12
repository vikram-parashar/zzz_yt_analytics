# Enrichment & Daily Facts

## Which Videos Get Enriched

The `get_enrichment_ids` query selects videos that are:

1. Recently discovered videos
2. **Matched to an agent** (exist in `bridge_video_agent`), AND recent:
   - On the 1st of the month → **all** matched videos
   - On Mondays → videos published in the last **3 months**
   - Other days → videos published in the last **30 days**

## Which Channels Get Enriched

All `channel_id`s from the video selection above.

## What Gets Written

| Target Table | Data | API |
|---|---|---|
| `dim_video` | `tags`, `duration_seconds`, `thumbnail` | `videos.list` (1 unit) |
| `fact_video_daily` | `view_count`, `like_count`, `comment_count` (daily snapshot) | same call |
| `dim_video.latest_*` | Latest counts denormalized onto dimension | same call |
| `dim_channel` | `country`, `thumbnail` | `channels.list` (1 unit) |
| `fact_channel_daily` | `subscriber_count`, `view_count`, `video_count` (daily snapshot) | same call |

Both fact tables use `INSERT OR REPLACE` on `(id, snapshot_date)` — one row per entity per day.

### API Costs

| Endpoint | Cost | Frequency |
|---|---|---|
| `search.list` | 100 units | Discovery only |
| `videos.list` | 1 unit | Enrichment (daily) |
| `channels.list` | 1 unit | Enrichment (daily) |
