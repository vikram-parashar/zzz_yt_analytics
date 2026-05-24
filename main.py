"""
Usage:
    uv run main.py setup              First-time warehouse setup
    uv run main.py daily              Daily incremental pipeline
    uv run main.py backfill           Backfill pipeline: 40 searches/day from 2024-01-01
    uv run main.py init-tables        Create DuckDB tables only
    uv run main.py scrape-agents      Scrape agent data from wiki
    uv run main.py discover           Single-day video discovery (legacy)
    uv run main.py initial-discover   Full initial video discovery (legacy)
    uv run main.py enrich-videos      Enrich video metadata
    uv run main.py enrich-channels    Enrich channel metadata
    uv run main.py score              Score all unscored videos
    uv run main.py match              Build video-agent associations
    uv run main.py query <sql>        Run SQL queries in the warehouse
    uv run main.py publish            Copy versioned warehouse
    uv run main.py status             Show pipeline status
"""

import sys
import time
import pendulum
from pathlib import Path
import shutil
from src.scoring import load_scoring_config, score_existing_videos, score_videos
from src.utils import DB_PATH, get_logger, get_db, chunk_list
from src.youtube import (
    search_videos,
    fetch_video_stats,
    fetch_channel_stats,
)
from src.warehouse import (
    get_pipeline_info,
    init_tables,
    insert_discovered_videos,
    set_pipeline_info,
    upsert_video_details,
    upsert_channel_details,
    _video_search_to_df,
    _video_stats_to_df,
    _channel_stats_to_df,
    get_video_ids,
    get_all_channel_ids,
    start_pipeline_run,
    finish_pipeline_run,
    did_pipeline_run_today,
)
from src.agents import scrape_and_load
from src.matching import match_videos_to_agents

logger = get_logger("main")
config = load_scoring_config()

BACKFILL_TOPIC = "Zenless Zone Zero"
BACKFILL_START_DATE = "2024-01-01"
BACKFILL_MAX_SEARCHES = 40

SEARCH_DELAY_SECONDS = 2.0


def run_tracked(pipeline_name: str):
    """Decorator that wraps a pipeline function with pipeline_runs tracking."""

    def decorator(fn):
        def wrapper(*args, **kwargs):
            run_id = start_pipeline_run(pipeline_name)
            try:
                result = fn(*args, **kwargs)
                finish_pipeline_run(run_id)
                return result
            except Exception as e:
                finish_pipeline_run(run_id, error=str(e))
                raise

        wrapper.__name__ = fn.__name__
        wrapper.__doc__ = fn.__doc__
        return wrapper

    return decorator


def setup():
    logger.info("=" * 50)
    logger.info("SETUP PIPELINE")
    logger.info("=" * 50)

    init_tables()
    scrape_and_load()

    logger.info("Setup complete — warehouse is ready")


def backfill():
    """
    Backfill data from 2024-01-01 to yesterday.
    Each search covers a 24hr window with max 50 results.
    Runs up to 40 searches per run (40 × 100 = 4000 quota units).
    Pipeline state is tracked in pipeline_info.
    If backfill is already completed, then skip.
    Includes throttling to respect YouTube API per-minute rate limits.
    """
    if get_pipeline_info("backfill_completed", "false") == "true":
        logger.info("Backfill already completed — skipping")
        return

    run_id = start_pipeline_run("backfill")
    try:
        _run_backfill()
        finish_pipeline_run(run_id)
    except Exception as e:
        finish_pipeline_run(run_id, error=str(e))
        raise


def _run_backfill():
    last_day = get_pipeline_info("last_processed_day")
    if last_day is None:
        current_date = pendulum.parse(BACKFILL_START_DATE)
        logger.info(f"Backfill starting from {BACKFILL_START_DATE}")
    else:
        current_date = pendulum.parse(last_day).add(days=1)
        logger.info(f"Backfill resuming from {current_date.to_date_string()}")

    yesterday = pendulum.yesterday()
    total_new_videos = 0
    searches_used = 0

    with get_db() as con:
        while searches_used < BACKFILL_MAX_SEARCHES:
            if current_date > yesterday:
                set_pipeline_info("backfill_completed", "true")
                set_pipeline_info("last_processed_day", yesterday.to_date_string())

                logger.info(
                    f"Backfill COMPLETE! Reached yesterday "
                    f"({yesterday.to_date_string()}). "
                    f"Total new videos: {total_new_videos}"
                )
                return

            day_start = current_date.start_of("day")
            day_end = current_date.add(days=1).start_of("day")

            if searches_used > 0:
                logger.debug(
                    f"Throttling: waiting {SEARCH_DELAY_SECONDS}s before next search"
                )
                time.sleep(SEARCH_DELAY_SECONDS)

            items, nextToken = search_videos(
                query=BACKFILL_TOPIC,
                published_after=day_start.to_rfc3339_string(),
                published_before=day_end.to_rfc3339_string(),
            )
            searches_used += 1

            if items:
                df = _video_search_to_df(items)
                df = score_videos(df, config)
                relevant_df = df[df["is_relevant"]].copy()
                insert_discovered_videos(con, relevant_df)

                n_new = len(relevant_df)
                total_new_videos += n_new

                logger.info(
                    f"[{current_date.to_date_string()}] "
                    f"Found {len(items)} raw -> {len(relevant_df)} relevant videos "
                    f"(searches: {searches_used}/{BACKFILL_MAX_SEARCHES})"
                )
            else:
                logger.info(
                    f"[{current_date.to_date_string()}] "
                    f"No videos found "
                    f"(searches: {searches_used}/{BACKFILL_MAX_SEARCHES})"
                )

            set_pipeline_info("last_processed_day", current_date.to_date_string())
            current_date = current_date.add(days=1)

        logger.info(
            f"Backfill paused after {searches_used} searches. "
            f"Last processed day: {get_pipeline_info('last_processed_day')}. "
            f"Total new videos: {total_new_videos}"
        )


def daily():
    if did_pipeline_run_today("daily"):
        logger.info("Daily pipeline already ran today — skipping")
        return

    if get_pipeline_info("backfill_completed", "false") == "false":
        logger.warning(
            "Backfill is not yet completed. Run 'backfill' command first. "
            "Falling back to daily discovery for today only."
        )

    logger.info("=" * 50)
    logger.info("DAILY PIPELINE")
    logger.info("=" * 50)

    run_id = start_pipeline_run("daily")
    try:
        _run_daily_discover()
        enrich_videos()
        enrich_channels()
        finish_pipeline_run(run_id)
    except Exception as e:
        finish_pipeline_run(run_id, error=str(e))
        raise

    logger.info("Daily pipeline complete")


def _run_daily_discover():
    yesterday = pendulum.yesterday()
    day_start = yesterday.start_of("day")

    total_new = 0

    with get_db() as con:
        for window_idx in range(2):
            window_start = day_start.add(hours=12 * window_idx)
            window_end = day_start.add(hours=12 * (window_idx + 1))

            if window_idx > 0:
                time.sleep(SEARCH_DELAY_SECONDS)

            items, _ = search_videos(
                query=BACKFILL_TOPIC,
                published_after=window_start.to_rfc3339_string(),
                published_before=window_end.to_rfc3339_string(),
            )

            if items:
                df = _video_search_to_df(items)
                df = score_videos(df, config)
                relevant_df = df[df["is_relevant"]].copy()
                insert_discovered_videos(con, relevant_df)
                total_new += len(relevant_df)
                logger.info(
                    f"[Yesterday window {window_idx + 1}] "
                    f"Found {len(items)} raw -> {len(relevant_df)} relevant"
                )
            else:
                logger.info(f"[Yesterday window {window_idx + 1}] No videos found")

    logger.info(f"Daily discovery: {total_new} new relevant videos")


def enrich_videos():
    """Fetch up-to-date stats for all known videos."""
    with get_db() as con:
        ids = get_video_ids(con)
        if not ids:
            logger.info("No videos to enrich")
            return

        for chunk in chunk_list(ids, 50):
            items = fetch_video_stats(chunk)
            df = _video_stats_to_df(items)
            upsert_video_details(con, df)

    logger.info("Video enrichment complete")


def enrich_channels():
    with get_db() as con:
        ids = get_all_channel_ids(con)
        if not ids:
            logger.info("No channels to enrich")
            return

        for chunk in chunk_list(ids, 50):
            items = fetch_channel_stats(chunk)
            df = _channel_stats_to_df(items)
            upsert_channel_details(con, df)

    logger.info("Channel enrichment complete")

def score_cmd():
    """Re-score all unscored videos in the warehouse."""
    with get_db() as con:
        count = score_existing_videos(con)
    logger.info(f"Scored {count} videos")

def status():
    """Show current pipeline status."""
    backfill_done = get_pipeline_info("backfill_completed", "false") == "true"
    last_day = get_pipeline_info("last_processed_day")

    n_videos = n_relevant = n_channels = n_agents = 0
    with get_db() as con:
        try:
            n_videos = con.execute("SELECT COUNT(*) FROM dim_video").fetchone()[0]
            n_relevant = con.execute(
                "SELECT COUNT(*) FROM dim_video WHERE is_relevant"
            ).fetchone()[0]
            n_channels = con.execute("SELECT COUNT(*) FROM dim_channel").fetchone()[0]
            n_agents = con.execute("SELECT COUNT(*) FROM dim_agent").fetchone()[0]
        except Exception:
            n_videos = n_relevant = n_channels = n_agents = 0

    print("\n" + "=" * 50)
    print("  PIPELINE STATUS")
    print("=" * 50)
    print(f"  Videos:         {n_videos} ({n_relevant} relevant)")
    print(f"  Channels:       {n_channels}")
    print(f"  Agents:         {n_agents}")
    print(f"  Backfill done:  {backfill_done}")
    print(f"  Last processed: {last_day or 'not started'}")
    if not backfill_done and last_day:
        yesterday = pendulum.yesterday().to_date_string()
        remaining = (pendulum.parse(yesterday) - pendulum.parse(last_day)).days
        print(f"  Days remaining: ~{remaining}")
        runs_needed = remaining / (BACKFILL_MAX_SEARCHES)
        print(f"  Backfill runs:  ~{runs_needed:.0f} more runs needed")
    print("=" * 50 + "\n")


def query(sql: str):
    """Run an ad-hoc SQL query and display results."""
    with get_db() as con:
        con.sql(sql).show()


def publish():
    ts = pendulum.now("UTC").format("YYYY-MM-DDTHH-mm-ss[Z]")

    out_dir = Path("artifacts/warehouse")
    out_dir.mkdir(parents=True, exist_ok=True)

    if not DB_PATH.exists():
        raise FileNotFoundError("warehouse.db not found after pipeline run")

    versioned = out_dir / f"warehouse_{ts}.db"
    latest = out_dir / "latest.db"

    shutil.copy2(DB_PATH, versioned)
    shutil.copy2(DB_PATH, latest)

    db_size_mb = DB_PATH.stat().st_size / (1024 * 1024)
    logger.info(
        f"Published warehouse snapshot -> {versioned.name} ({db_size_mb:.1f} MB)"
    )
    logger.info("Published latest copy -> latest.db")


COMMANDS = {
    "setup": setup,
    "daily": daily,
    "backfill": backfill,
    "init-tables": init_tables,
    "scrape-agents": run_tracked("scrape-agents")(scrape_and_load),
    "enrich-videos": run_tracked("enrich-videos")(enrich_videos),
    "enrich-channels": run_tracked("enrich-channels")(enrich_channels),
    "match": run_tracked("match")(match_videos_to_agents),
    "status": status,
    "publish": publish,
    "score": run_tracked("score")(score_cmd),
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help", "help"):
        print("\nCommands:")
        for cmd in COMMANDS:
            print(f"  {cmd}")
        print("  query <sql>")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "query":
        if len(sys.argv) < 3:
            print("Usage: uv run main.py query <sql>")
            sys.exit(1)
        query(sys.argv[2])
    elif cmd in COMMANDS:
        COMMANDS[cmd]()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
