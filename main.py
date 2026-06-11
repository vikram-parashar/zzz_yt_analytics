"""
Usage:
    uv run main.py setup              First-time warehouse setup
    uv run main.py daily              Daily pipeline (auto-selects backfill or incremental)
    uv run main.py init-tables        Create DuckDB tables only
    uv run main.py scrape-agents      Scrape agent data from wiki
    uv run main.py scrape-banners     Scrape banner schedule from game8.co
    uv run main.py enrich             Enrich video and channel metadata
    uv run main.py match-remaining    Match unmatched videos against agents
    uv run main.py match-all          Re-match ALL videos against agents
    uv run main.py build-agent-daily  Rebuild fact_agent_daily aggregates
    uv run main.py backup             Create a backup copy from motherduck
    uv run main.py status             Show pipeline status
    uv run main.py query <sql>        Run SQL queries in the warehouse
"""

import functools
from pathlib import Path
import random
import time
from pandas.core.frame import sys
import pendulum
from src.matching import match_all, match_remaining
from src.utils import MOTHERDUCK_DB, MOTHERDUCK_TOKEN, chunk_list, get_db, get_logger
from src.warehouse import (
    _channel_stats_to_df,
    _video_stats_to_df,
    build_fact_agent_daily,
    did_pipeline_run_today,
    get_enrichment_ids,
    get_pipeline_info,
    init_tables,
    insert_discovered_videos,
    _video_search_to_df,
    set_pipeline_info,
    start_pipeline_run,
    finish_pipeline_run,
    upsert_channel_details,
    upsert_video_details,
)
from src.agents import scrape_and_load
from src.banners import scrape_banners
import shutil

from src.youtube import fetch_channel_stats, fetch_video_stats, search_videos

logger = get_logger("main")

BACKFILL_TOPIC = "Zenless Zone Zero"
BACKFILL_START_DATE = "2024-01-01"

TYPE1_MAX_SEARCHES = 0

TYPE2_MAX_SEARCHES = 0
TYPE2_SEARCHES_PER_MONTH = 10

SEARCH_DELAY_SECONDS = 2.0


def run_tracked(pipeline_name: str):
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


def _ingest_search_results(con, items, discovery_type: str):
    if not items:
        return 0
    df = _video_search_to_df(items)
    insert_discovered_videos(con, df, discovery_type=discovery_type)


def setup():
    init_tables()


def _run_backfill_type1():
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
                f"[Backfill Popular] {current_date.to_date_string()} | "
                f"searches: {searches_used}/{TYPE1_MAX_SEARCHES}"
            )

            set_pipeline_info("type1_last_day", current_date.to_date_string())
            current_date = current_date.add(days=1)

        logger.info(
            f"Type I backfill paused after {searches_used} searches. "
            f"Last day: {get_pipeline_info('type1_last_day')}. "
        )


def _run_daily_popular():
    now = pendulum.now()
    published_after = now.subtract(hours=48).to_rfc3339_string()
    published_before = now.to_rfc3339_string()

    with get_db() as con:
        items = search_videos(
            query=BACKFILL_TOPIC,
            published_after=published_after,
            published_before=published_before,
            order="viewCount",
        )
        _ingest_search_results(con, items, discovery_type="popular")
        logger.info(f"[Daily Popular] {len(items)} raw -> ingested")


def _random_timestamp_in_month(year: int, month: int) -> pendulum.DateTime:
    start = pendulum.datetime(year, month, 1, 0, 0, 0)
    if month == 12:
        end = pendulum.datetime(year + 1, 1, 1, 0, 0, 0)
    else:
        end = pendulum.datetime(year, month + 1, 1, 0, 0, 0)

    delta_seconds = int((end - start).total_seconds())
    random_offset = random.randint(0, max(delta_seconds - 1, 0))
    return start.add(seconds=random_offset)


def _run_backfill_type2():
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
                    f"[Backfill Random] {cursor.format('YYYY-MM')} search #{searches_done_in_month}/{target_count} | "
                    f"before={rand_ts.format('YYYY-MM-DD HH:mm')} | "
                    f"total: {total_searches}/{TYPE2_MAX_SEARCHES}"
                )

            set_pipeline_info("type2_current_month", cursor.format("YYYY-MM"))
            set_pipeline_info("type2_searches_done", str(searches_done_in_month))

            if total_searches >= TYPE2_MAX_SEARCHES:
                logger.info(
                    f"Type II backfill paused after {total_searches} searches at {cursor.format('YYYY-MM')}. "
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


def _run_daily_random():
    now = pendulum.now()

    if now.day % 3 != 0:
        logger.info(f"[Daily Random] skipped — day={now.day} (runs when day%3==0)")
        return

    published_after = "2024-01-01T00:00:00Z"

    with get_db() as con:
        last_discovered = con.sql("""
            SELECT MAX(ingested_date)
            FROM dim_video
            WHERE discovery_type = 'random'
        """).fetchone()[0]

        if last_discovered is not None:
            last_discovered = pendulum.instance(last_discovered)
        else:
            last_discovered = now.subtract(days=7)

        start_ts = last_discovered.int_timestamp
        end_ts = now.int_timestamp

        random_ts = pendulum.from_timestamp(
            random.randint(start_ts, end_ts), tz=now.timezone
        )

        published_before = random_ts.to_iso8601_string()

        items = search_videos(
            query=BACKFILL_TOPIC,
            published_after=published_after,
            published_before=published_before,
            order="date",
        )

        _ingest_search_results(con, items, discovery_type="random")

        logger.info(
            f"[Daily Random] {len(items)} raw -> ingested | "
            f"before={random_ts.format('YYYY-MM-DD HH:mm')}"
        )


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


def daily():
    if did_pipeline_run_today("daily"):
        logger.info("Daily pipeline already ran today — skipping")
        return

    run_id = start_pipeline_run("daily")
    try:
        type1_done = get_pipeline_info("type1_completed", "false") == "true"
        if type1_done:
            logger.info("[Step 1] Popular backfill complete -> running daily popular")
            _run_daily_popular()
        else:
            logger.info("[Step 1] Popular backfill in progress -> continuing backfill")
            _run_backfill_type1()

        time.sleep(SEARCH_DELAY_SECONDS)

        type2_done = get_pipeline_info("type2_completed", "false") == "true"
        if type2_done:
            logger.info("[Step 2] Random backfill complete -> running daily random")
            _run_daily_random()
        else:
            logger.info("[Step 2] Random backfill in progress -> continuing backfill")
            _run_backfill_type2()

        logger.info("[Step 3] Scrape agents")
        scrape_and_load()

        logger.info("[Step 4] Scrape banners")
        scrape_banners()

        logger.info("[Step 5] Enrich")
        enrich()

        logger.info("[Step 6] Match remaining")
        match_remaining()

        logger.info("[Step 7] Build agent daily for today")
        today = pendulum.now().to_date_string()
        with get_db() as con:
            build_fact_agent_daily(con, snapshot_date=today)

        logger.info("[Step 8] Backup")
        backup()

        finish_pipeline_run(run_id)
    except Exception as e:
        finish_pipeline_run(run_id, error=str(e))
        raise

    logger.info("Daily pipeline complete")


def build_agent_daily_cmd():
    with get_db() as con:
        build_fact_agent_daily(con)


def status():
    type1_done = get_pipeline_info("type1_completed", "false") == "true"
    type2_done = get_pipeline_info("type2_completed", "false") == "true"
    type1_last_day = get_pipeline_info("type1_last_day")
    type2_current_month = get_pipeline_info("type2_current_month")

    n_videos = n_channels = n_agents = n_unmatched = 0
    with get_db() as con:
        try:
            n_videos = con.execute("SELECT COUNT(*) FROM dim_video").fetchone()[0]
            n_channels = con.execute("SELECT COUNT(*) FROM dim_channel").fetchone()[0]
            n_agents = con.execute("SELECT COUNT(*) FROM dim_agent").fetchone()[0]
            n_unmatched = con.execute(
                "SELECT COUNT(*) FROM dim_video WHERE is_matched = FALSE OR is_matched IS NULL"
            ).fetchone()[0]
        except Exception:
            pass

    print("\n" + "=" * 60)
    print("  PIPELINE STATUS")
    print("=" * 60)
    print(f"  Videos:             {n_videos}")
    print(f"  Unmatched videos:   {n_unmatched}")
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
    with get_db() as con:
        con.sql(sql).show()


def backup():
    import duckdb

    if not MOTHERDUCK_TOKEN:
        logger.warning("MOTHERDUCK_TOKEN not set — skipping backup")
        return

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


COMMANDS = {
    "setup": setup,
    "daily": daily,
    "init-tables": init_tables,
    "scrape-agents": run_tracked("scrape-agents")(scrape_and_load),
    "scrape-banners": run_tracked("scrape-banners")(scrape_banners),
    "enrich": run_tracked("enrich")(enrich),
    "match-remaining": run_tracked("match-remaining")(match_remaining),
    "match-all": run_tracked("match-all")(match_all),
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
