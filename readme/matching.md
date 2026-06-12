# Agent ↔ Video Matching

## How It Works

Fuzzy matching via regex on `bridge_agent_alias` patterns. For each unmatched video, every alias pattern is tested against three fields with weighted confidence scores:

| Field | Weight |
|---|---|
| `title` | 10 |
| `tags` | 5 |
| `description` | 2 |

A video-agent pair is created when `confidence > 0`. Uses word-boundary regex (`\b...\b`) to avoid partial matches.

## Attribution

Since one video can match multiple agents, `attribution_weight` distributes credit proportionally:

```
attribution_weight = confidence / SUM(confidence)   -- per video
```

Example: A video matching "Miyabi" (conf=12) and "Yanagi" (conf=5) gets weights 0.71 and 0.29 respectively.

## Commands

- `match-remaining` — only processes `is_matched = FALSE` videos
- `match-all` — deletes all existing matches and re-runs from scratch

## Fact Agent Daily

`fact_agent_daily` aggregates per agent per day using attribution weights:

| Field | Formula |
|---|---|
| `attributed_views` | `SUM(attribution_weight × view_count)` |
| `attributed_likes` | `SUM(attribution_weight × like_count)` |
| `attributed_comments` | `SUM(attribution_weight × comment_count)` |
| `video_count` | `COUNT(DISTINCT video_id)` |

Uses the most recent `fact_video_daily` snapshot on or before the target date.
