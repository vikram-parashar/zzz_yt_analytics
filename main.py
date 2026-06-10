"""
Usage:
    uv run main.py setup              First-time warehouse setup
    uv run main.py daily              Daily incremental pipeline (includes match)
    uv run main.py backfill           Run both Type I + Type II backfill (60+20 searches/day)
    uv run main.py backfill-popular   Type I backfill: popular by viewCount, 60 searches/day
    uv run main.py backfill-random    Type II backfill: random by date, 20 searches/day
    uv run main.py init-tables        Create DuckDB tables only
    uv run main.py scrape-agents      Scrape agent data from wiki
    uv run main.py scrape-banners     Scrape banner schedule from game8.co
    uv run main.py enrich             Enrich video and channel metadata
    uv run main.py match              Build video-agent associations
    uv run main.py build-agent-daily  Rebuild fact_agent_daily aggregates
    uv run main.py query <sql>        Run SQL queries in the warehouse
    uv run main.py backup             create a backup copy from motherduck
    uv run main.py status             Show pipeline status
"""

import functools
import random
import sys
import time
import pendulum
from pathlib import Path
import shutil
from src.utils import (
    get_logger,
    get_db,
    chunk_list,
    MOTHERDUCK_TOKEN,
    MOTHERDUCK_DB,
)
from src.youtube import search_videos, fetch_video_stats, fetch_channel_stats
from src.warehouse import (
    get_enrichment_ids,
    get_pipeline_info,
    init_tables,
    insert_discovered_videos,
    set_pipeline_info,
    upsert_video_details,
    upsert_channel_details,
    _video_search_to_df,
    _video_stats_to_df,
    _channel_stats_to_df,
    start_pipeline_run,
    finish_pipeline_run,
    did_pipeline_run_today,
    build_fact_agent_daily,
)
from src.agents import scrape_and_load
from src.matching import match_all_videos
from src.banners import scrape_banners

logger = get_logger("main")

BACKFILL_TOPIC = "Zenless Zone Zero"
BACKFILL_START_DATE = "2024-01-01"

TYPE1_MAX_SEARCHES = 50

TYPE2_MAX_SEARCHES = 15
TYPE2_SEARCHES_PER_MONTH = 10

SEARCH_DELAY_SECONDS = 2.0


def run_tracked(pipeline_name: str):
    """Decorator that wraps a pipeline function with pipeline_runs tracking."""

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            run_id = start_pipeline_run(pipeline_name)
            try:
                result = fn(*args, **kwargs)
                finish_pipeline_run(run_id)
                return result
            except Exception as e:
                finish_pipeline_run(run_id, error=str(e))
                raise

        return wrapper

    return decorator


def _ingest_search_results(con, items, discovery_type: str) -> int:
    """Convert search items to DataFrame and insert to warehouse. Returns count of new videos."""
    if not items:
        return 0
    df = _video_search_to_df(items)
    insert_discovered_videos(con, df, discovery_type=discovery_type)


def setup():
    """One-time setup: init tables + scrape agents."""
    init_tables()
    scrape_and_load()
    scrape_banners()


def _run_backfill_type1():
    """Type I: Daily windows from BACKFILL_START_DATE, order=viewCount.

    Progress tracked in pipeline_info:
      - type1_last_day:   last fully-processed date string (e.g. '2024-03-15')
      - type1_completed:  'true' when we've reached yesterday
    """
    if get_pipeline_info("type1_completed", "false") == "true":
        logger.info("Type I backfill already completed — skipping")
        return

    last_day = get_pipeline_info("type1_last_day")
    if last_day is None:
        current_date = pendulum.parse(BACKFILL_START_DATE)
    else:
        current_date = pendulum.parse(last_day).add(days=1)

    yesterday = pendulum.yesterday()
    searches_used = 0

    with get_db() as con:
        while searches_used < TYPE1_MAX_SEARCHES:
            if current_date > yesterday:
                set_pipeline_info("type1_completed", "true")
                set_pipeline_info("type1_last_day", yesterday.to_date_string())
                logger.info(
                    f"Type I COMPLETE! Reached yesterday ({yesterday.to_date_string()}). "
                )
                return

            day_start = current_date.start_of("day")
            day_end = current_date.add(days=1).start_of("day")

            if searches_used > 0:
                time.sleep(SEARCH_DELAY_SECONDS)

            items = search_videos(
                query=BACKFILL_TOPIC,
                published_after=day_start.to_rfc3339_string(),
                published_before=day_end.to_rfc3339_string(),
                order="viewCount",
            )
            searches_used += 1

            _ingest_search_results(con, items, discovery_type="popular")

            logger.info(
                f"[Type I] {current_date.to_date_string()} | "
                f"searches: {searches_used}/{TYPE1_MAX_SEARCHES}"
            )

            set_pipeline_info("type1_last_day", current_date.to_date_string())
            current_date = current_date.add(days=1)

        logger.info(
            f"Type I paused after {searches_used} searches. "
            f"Last day: {get_pipeline_info('type1_last_day')}. "
        )


def _random_timestamp_in_month(year: int, month: int) -> pendulum.DateTime:
    """Return a random timestamp uniformly distributed within the given month."""
    start = pendulum.datetime(year, month, 1, 0, 0, 0)
    if month == 12:
        end = pendulum.datetime(year + 1, 1, 1, 0, 0, 0)
    else:
        end = pendulum.datetime(year, month + 1, 1, 0, 0, 0)

    delta_seconds = int((end - start).total_seconds())
    random_offset = random.randint(0, max(delta_seconds - 1, 0))
    return start.add(seconds=random_offset)


def _run_backfill_type2():
    """Type II: Random timestamp sampling per month, order=date.

    For each completed month (before current month): 10 random searches.
    For the current month: (day_of_month // 3) random searches.
    Max 20 searches per run. Progress tracked in pipeline_info:
      - type2_current_month:  'YYYY-MM' of the month currently being processed
      - type2_searches_done:  how many searches done for the current month so far
      - type2_completed:      'true' when all complete months are done
    """
    if get_pipeline_info("type2_completed", "false") == "true":
        logger.info("Type II backfill already completed — skipping")
        return

    now = pendulum.now()
    current_month_start = pendulum.datetime(now.year, now.month, 1, 0, 0, 0)

    saved_month = get_pipeline_info("type2_current_month")
    saved_done = get_pipeline_info("type2_searches_done")

    if saved_month:
        year, month = saved_month.split("-")
        cursor = pendulum.datetime(int(year), int(month), 1, 0, 0, 0)
        searches_done_in_month = int(saved_done) if saved_done else 0
    else:
        cursor = pendulum.parse(BACKFILL_START_DATE).start_of("month")
        searches_done_in_month = 0

    total_searches = 0

    with get_db() as con:
        while total_searches < TYPE2_MAX_SEARCHES:
            is_current_month = cursor >= current_month_start

            if is_current_month:
                target_count = max(now.day // 3, 1)
            else:
                target_count = TYPE2_SEARCHES_PER_MONTH

            remaining_for_month = target_count - searches_done_in_month
            if remaining_for_month <= 0:
                if is_current_month:
                    set_pipeline_info("type2_current_month", cursor.format("YYYY-MM"))
                    set_pipeline_info("type2_searches_done", str(target_count))
                    return

                cursor = cursor.add(months=1)
                searches_done_in_month = 0
                if cursor >= current_month_start:
                    set_pipeline_info("type2_completed", "true")
                    set_pipeline_info("type2_current_month", cursor.format("YYYY-MM"))
                    set_pipeline_info("type2_searches_done", "0")
                    return
                continue

            month_searches = 0
            while remaining_for_month > 0 and total_searches < TYPE2_MAX_SEARCHES:
                if total_searches > 0 or month_searches > 0:
                    time.sleep(SEARCH_DELAY_SECONDS)

                rand_ts = _random_timestamp_in_month(cursor.year, cursor.month)
                month_start = cursor.start_of("month")

                items = search_videos(
                    query=BACKFILL_TOPIC,
                    published_after=month_start.to_rfc3339_string(),
                    published_before=rand_ts.to_rfc3339_string(),
                    order="date",
                )
                total_searches += 1
                month_searches += 1
                searches_done_in_month += 1
                remaining_for_month -= 1

                _ingest_search_results(con, items, discovery_type="random")

                logger.info(
                    f"[Type II] {cursor.format('YYYY-MM')} search #{searches_done_in_month}/{target_count} | "
                    f"before={rand_ts.format('YYYY-MM-DD HH:mm')} | "
                    f"total: {total_searches}/{TYPE2_MAX_SEARCHES}"
                )

            set_pipeline_info("type2_current_month", cursor.format("YYYY-MM"))
            set_pipeline_info("type2_searches_done", str(searches_done_in_month))

            if total_searches >= TYPE2_MAX_SEARCHES:
                logger.info(
                    f"Type II paused after {total_searches} searches at {cursor.format('YYYY-MM')}. "
                )
                return

            if is_current_month:
                logger.info(f"Type II current month {cursor.format('YYYY-MM')} done. ")
                return

            cursor = cursor.add(months=1)
            searches_done_in_month = 0

            if cursor >= current_month_start:
                set_pipeline_info("type2_completed", "true")
                set_pipeline_info("type2_current_month", cursor.format("YYYY-MM"))
                set_pipeline_info("type2_searches_done", "0")
                return


def backfill():
    """Run both Type I and Type II backfill in sequence"""
    type1_done = get_pipeline_info("type1_completed", "false") == "true"
    type2_done = get_pipeline_info("type2_completed", "false") == "true"

    if type1_done and type2_done:
        logger.info("Both backfill types already completed — skipping")
        return

    run_id = start_pipeline_run("backfill")
    try:
        if not type1_done:
            _run_backfill_type1()

        if not type2_done:
            _run_backfill_type2()

        today = pendulum.now().to_date_string()
        with get_db() as con:
            build_fact_agent_daily(con, snapshot_date=today)

        finish_pipeline_run(run_id)
    except Exception as e:
        finish_pipeline_run(run_id, error=str(e))
        raise


def backfill_popular():
    """Run only Type I backfill (popular / viewCount)"""
    if get_pipeline_info("type1_completed", "false") == "true":
        logger.info("Type I backfill already completed — skipping")
        return

    run_id = start_pipeline_run("backfill-popular")
    try:
        _run_backfill_type1()
        today = pendulum.now().to_date_string()
        with get_db() as con:
            build_fact_agent_daily(con, snapshot_date=today)
        finish_pipeline_run(run_id)
    except Exception as e:
        finish_pipeline_run(run_id, error=str(e))
        raise


def backfill_random():
    """Run only Type II backfill (random / date)"""
    if get_pipeline_info("type2_completed", "false") == "true":
        logger.info("Type II backfill already completed — skipping")
        return

    run_id = start_pipeline_run("backfill-random")
    try:
        _run_backfill_type2()
        today = pendulum.now().to_date_string()
        with get_db() as con:
            build_fact_agent_daily(con, snapshot_date=today)
        finish_pipeline_run(run_id)
    except Exception as e:
        finish_pipeline_run(run_id, error=str(e))
        raise


def daily():
    """Daily pipeline: scrape agents -> discover -> match -> aggregate.

    Discovery strategy depends on which backfill types are complete:
      - Type I complete:  fetch once with order='viewCount',
                          publishedBefore=now(), publishedAfter=now()-1d3h
      - Type II complete: fetch once with order='date',
                          publishedBefore=now(), publishedAfter=random timestamp
                          (random between now() and max(published_at) from existing videos)
    """
    if did_pipeline_run_today("daily"):
        logger.info("Daily pipeline already ran today — skipping")
        return

    run_id = start_pipeline_run("daily")
    try:
        scrape_and_load()
        scrape_banners()

        _run_daily_discover()

        enrich()

        today = pendulum.now().to_date_string()
        with get_db() as con:
            build_fact_agent_daily(con, snapshot_date=today)

        finish_pipeline_run(run_id)
    except Exception as e:
        finish_pipeline_run(run_id, error=str(e))
        raise

    logger.info("Daily pipeline complete")


def _run_daily_discover():
    """Discover new videos based on which backfill types are complete."""
    type1_done = get_pipeline_info("type1_completed", "false") == "true"
    type2_done = get_pipeline_info("type2_completed", "false") == "true"

    now = pendulum.now()

    with get_db() as con:
        if type1_done:
            published_after = now.subtract(hours=27).to_rfc3339_string()
            published_before = now.to_rfc3339_string()

            items = search_videos(
                query=BACKFILL_TOPIC,
                published_after=published_after,
                published_before=published_before,
                order="viewCount",
            )
            _ingest_search_results(con, items, discovery_type="popular")
        else:
            logger.info("[Daily Type I] skipped — Type I backfill not complete")

        time.sleep(SEARCH_DELAY_SECONDS)

        if type2_done:
            if now.day % 3 == 0:
                published_before = now.to_rfc3339_string()
                lower_bound = now.subtract(days=3, hours=3)
                delta_seconds = int((now - lower_bound).total_seconds())
                random_offset = random.randint(0, max(delta_seconds - 1, 0))
                random_ts = lower_bound.add(seconds=random_offset)
                published_after = random_ts.to_rfc3339_string()

                items = search_videos(
                    query=BACKFILL_TOPIC,
                    published_after=published_after,
                    published_before=published_before,
                    order="date",
                )
                _ingest_search_results(con, items, discovery_type="random")
            else:
                logger.info(
                    f"[Daily Type II] skipped — day={now.day} (runs when day%3==0)"
                )
        else:
            logger.info("[Daily Type II] skipped — Type II backfill not complete")


def enrich():
    with get_db() as con:
        video_ids, channel_ids = get_enrichment_ids(con)
        if not video_ids and not channel_ids:
            logger.info("Nothing to enrich")
            return

        if video_ids:
            logger.info(f"Enriching {len(video_ids)} videos")
            total_videos = 0
            for chunk in chunk_list(video_ids, 50):
                items = fetch_video_stats(chunk)
                df = _video_stats_to_df(items)
                upsert_video_details(con, df)
                total_videos += len(df)
            logger.info(f"Enriched {total_videos} videos")

        if channel_ids:
            logger.info(f"Enriching {len(channel_ids)} channels")
            for chunk in chunk_list(channel_ids, 50):
                items = fetch_channel_stats(chunk)
                df = _channel_stats_to_df(items)
                upsert_channel_details(con, df)
            logger.info(f"Enriched {len(channel_ids)} channels")


def status():
    """Show current pipeline status."""
    type1_done = get_pipeline_info("type1_completed", "false") == "true"
    type2_done = get_pipeline_info("type2_completed", "false") == "true"
    type1_last_day = get_pipeline_info("type1_last_day")
    type2_current_month = get_pipeline_info("type2_current_month")

    n_videos = n_channels = n_agents = 0
    with get_db() as con:
        try:
            n_videos = con.execute("SELECT COUNT(*) FROM dim_video").fetchone()[0]
            n_channels = con.execute("SELECT COUNT(*) FROM dim_channel").fetchone()[0]
            n_agents = con.execute("SELECT COUNT(*) FROM dim_agent").fetchone()[0]
        except Exception:
            n_videos = n_channels = n_agents = 0

    print("\n" + "=" * 60)
    print("  PIPELINE STATUS")
    print("=" * 60)
    print(f"  Videos:             {n_videos}")
    print(f"  Channels:           {n_channels}")
    print(f"  Agents:             {n_agents}")
    print(f"  Type I (popular):   {'DONE' if type1_done else 'IN PROGRESS'}")
    print(f"    Last processed:   {type1_last_day or 'not started'}")
    print(f"  Type II (random):   {'DONE' if type2_done else 'IN PROGRESS'}")
    print(f"    Current month:    {type2_current_month or 'not started'}")
    if not type1_done and type1_last_day:
        yesterday = pendulum.yesterday().to_date_string()
        remaining = (pendulum.parse(yesterday) - pendulum.parse(type1_last_day)).days
        runs_needed = remaining / TYPE1_MAX_SEARCHES
        print(f"  Type I remaining:   ~{remaining} days (~{runs_needed:.0f} runs)")
    print("=" * 60 + "\n")


def query(sql: str):
    """Run an ad-hoc SQL query and display results."""
    with get_db() as con:
        con.sql(sql).show()


def backup():
    import duckdb

    if not MOTHERDUCK_TOKEN:
        raise RuntimeError("MOTHERDUCK_TOKEN environment variable is not set")

    ts = pendulum.now("UTC").format("YYYY-MM-DDTHH-mm-ss[Z]")
    out_dir = Path("artifacts/warehouse")
    out_dir.mkdir(parents=True, exist_ok=True)

    versioned = out_dir / f"warehouse_{ts}.db"
    con = duckdb.connect(MOTHERDUCK_DB)
    try:
        source_db = MOTHERDUCK_DB.split(":")[1]

        con.execute(f"ATTACH '{versioned}' AS backup")
        con.execute(f"COPY FROM DATABASE {source_db} TO backup")
        con.execute("CHECKPOINT backup")
        con.execute("DETACH backup")
    finally:
        con.close()

    db_size_mb = versioned.stat().st_size / (1024 * 1024)

    logger.info(
        f"Published MotherDuck backup -> {versioned.name} ({db_size_mb:.1f} MB)"
    )

    latest = out_dir / "latest.db"
    shutil.copy2(versioned, latest)


def build_agent_daily_cmd():
    """Rebuild fact_agent_daily from bridge + fact_video_daily."""
    with get_db() as con:
        build_fact_agent_daily(con)


COMMANDS = {
    "setup": setup,
    "daily": daily,
    "backfill": backfill,
    "backfill-popular": backfill_popular,
    "backfill-random": backfill_random,
    "init-tables": init_tables,
    "scrape-agents": run_tracked("scrape-agents")(scrape_and_load),
    "scrape-banners": run_tracked("scrape-banners")(scrape_banners),
    "enrich": run_tracked("enrich")(enrich),
    "match": run_tracked("match")(match_all_videos),
    "build-agent-daily": run_tracked("build-agent-daily")(build_agent_daily_cmd),
    "status": status,
    "backup": backup,
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
