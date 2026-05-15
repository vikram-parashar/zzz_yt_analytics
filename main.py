"""
Usage:
    uv run main.py setup              First-time warehouse setup
    uv run main.py daily              Daily incremental pipeline
    uv run main.py init-tables        Create DuckDB tables only
    uv run main.py scrape-agents      Scrape agent data from wiki
    uv run main.py discover           Daily video discovery
    uv run main.py initial-discover   Full initial video discovery
    uv run main.py enrich-videos      Enrich video metadata
    uv run main.py enrich-channels    Enrich channel metadata
    uv run main.py match              Build video-agent associations
    uv run main.py query <sql>        Run SQL queries in the warehouse
    uv run main.py publish            Copy versioned warehouse
"""

import sys
import pendulum
from pathlib import Path
import shutil
from src.utils import DB_PATH, WORK_DIR, get_logger, get_db, chunk_list
from src.youtube import search_videos, fetch_video_stats, fetch_channel_stats
from src.warehouse import (
    init_tables,
    insert_discovered_videos,
    upsert_video_details,
    upsert_channel_details,
    _video_search_to_df,
    _video_stats_to_df,
    _channel_stats_to_df,
    get_video_ids,
    get_channel_ids_needing_enrichment,
    get_agent_names,
)
from src.agents import scrape_and_load
from src.matching import match_videos_to_agents

logger = get_logger("main")

# ── Pipelines ─────────────────────────────────────────────────────


def setup():
    """One-time setup: init tables → scrape agents → discover → enrich."""
    logger.info("=" * 50)
    logger.info("SETUP PIPELINE")
    logger.info("=" * 50)

    init_tables()
    scrape_and_load()
    initial_discover()
    enrich_videos()
    enrich_channels()

    logger.info("Setup complete — warehouse is ready")


def daily():
    """Daily pipeline: discover new videos → enrich stats."""
    logger.info("=" * 50)
    logger.info("DAILY PIPELINE")
    logger.info("=" * 50)

    discover()
    enrich_videos()
    enrich_channels()

    logger.info("Daily pipeline complete")


def initial_discover():
    """Discover videos for all agent-related topics. Uses 100 quota units per topic."""
    with get_db() as con:
        topics = [
            "Zenless Zone Zero guide",
            "Zenless Zone Zero character showcase",
            "Zenless Zone Zero tier list",
            "Zenless Zone Zero gameplay",
            "Zenless Zone Zero news",
        ]
        for name in get_agent_names(con):
            topics.append(f"Zenless Zone Zero {name}")

        logger.info(f"Running initial discovery for {len(topics)} topics")

        for topic in topics:
            items = search_videos(
                topic, 50, pendulum.datetime(2024, 1, 1).to_rfc3339_string()
            )
            df = _video_search_to_df(items)
            insert_discovered_videos(con, df)


def discover():
    """Discover recently published videos. Uses ~100 quota units."""
    with get_db() as con:
        topic = "Zenless Zone Zero"
        max_results = 30  # can go upto 50
        published_after = pendulum.now().subtract(days=1).to_rfc3339_string()

        items = search_videos(topic, max_results, published_after)
        df = _video_search_to_df(items)
        insert_discovered_videos(con, df)


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
    """Fetch channel details for channels missing thumbnail/country."""
    with get_db() as con:
        ids = get_channel_ids_needing_enrichment(con)
        if not ids:
            logger.info("No channels to enrich")
            return

        for chunk in chunk_list(ids, 50):
            items = fetch_channel_stats(chunk)
            df = _channel_stats_to_df(items)
            upsert_channel_details(con, df)

    logger.info("Channel enrichment complete")


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
        f"Published warehouse snapshot → {versioned.name} ({db_size_mb:.1f} MB)"
    )
    logger.info("Published latest copy → latest.db")


COMMANDS = {
    "setup": setup,
    "daily": daily,
    "init-tables": init_tables,
    "scrape-agents": scrape_and_load,
    "discover": discover,
    "initial-discover": initial_discover,
    "enrich-videos": enrich_videos,
    "enrich-channels": enrich_channels,
    "match": match_videos_to_agents,
    "publish": publish,
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
