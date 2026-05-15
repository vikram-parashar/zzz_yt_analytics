import re
import pandas as pd
import pendulum

from src.utils import get_logger, get_db

logger = get_logger(__name__)

TABLE_DDL = {
    "dim_agent": """
        CREATE TABLE IF NOT EXISTS dim_agent (
            name VARCHAR UNIQUE PRIMARY KEY,
            img VARCHAR,
            rank VARCHAR,
            attribute VARCHAR,
            speciality VARCHAR,
            faction VARCHAR,
            release_date DATE,
            release_version VARCHAR
        )
    """,
    "bridge_agent_alias": """
        CREATE TABLE IF NOT EXISTS bridge_agent_alias (
            name VARCHAR,
            alias VARCHAR,
            PRIMARY KEY (name, alias),
            FOREIGN KEY (name) REFERENCES dim_agent(name)
        )
    """,
    "dim_video": """
        CREATE TABLE IF NOT EXISTS dim_video (
            video_id VARCHAR PRIMARY KEY,
            title VARCHAR,
            description VARCHAR,
            channel_id VARCHAR,
            published_at TIMESTAMP,
            thumbnail VARCHAR,
            tags VARCHAR[],
            duration_seconds INT,
            ingested_date DATE
        )
    """,
    "dim_channel": """
        CREATE TABLE IF NOT EXISTS dim_channel (
            channel_id VARCHAR PRIMARY KEY,
            channel_name VARCHAR,
            thumbnail VARCHAR,
            country VARCHAR,
            ingested_date DATE
        )
    """,
    "fact_video_daily": """
        CREATE TABLE IF NOT EXISTS fact_video_daily (
            video_id VARCHAR,
            snapshot_date DATE,
            view_count BIGINT,
            like_count BIGINT,
            comment_count BIGINT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (video_id, snapshot_date),
            FOREIGN KEY (video_id) REFERENCES dim_video(video_id)
        )
    """,
    "fact_channel_daily": """
        CREATE TABLE IF NOT EXISTS fact_channel_daily (
            channel_id VARCHAR,
            snapshot_date DATE,
            subscriber_count BIGINT,
            view_count BIGINT,
            video_count INTEGER,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (channel_id, snapshot_date),
            FOREIGN KEY (channel_id) REFERENCES dim_channel(channel_id)
        )
    """,
    "bridge_video_agent": """
        CREATE TABLE IF NOT EXISTS bridge_video_agent (
            video_id VARCHAR,
            agent_name VARCHAR,
            confidence REAL,
            PRIMARY KEY (video_id, agent_name),
            FOREIGN KEY (video_id) REFERENCES dim_video(video_id),
            FOREIGN KEY (agent_name) REFERENCES dim_agent(name)
        )
    """,
    "dim_patch": """
        CREATE TABLE IF NOT EXISTS dim_patch (
            version      VARCHAR PRIMARY KEY,
            release_date DATE,
            banner_agent VARCHAR,
            banner_start DATE,
            banner_end   DATE,
            notes        VARCHAR,
            FOREIGN KEY (banner_agent) REFERENCES dim_agent(name)
        )
    """,
}


def init_tables():
    with get_db() as con:
        for name, ddl in TABLE_DDL.items():
            con.execute(ddl)
            logger.info(f"Ensured table exists: {name}")


def _video_search_to_df(items: list[dict]) -> pd.DataFrame:
    records = []
    for item in items:
        try:
            snippet = item["snippet"]
            records.append(
                {
                    "video_id": item["id"]["videoId"],
                    "title": snippet["title"],
                    "description": snippet["description"],
                    "channel_id": snippet["channelId"],
                    "channel_title": snippet["channelTitle"],
                    "published_at": snippet["publishedAt"],
                }
            )
        except KeyError:
            logger.warning(
                f"Skipping search item with missing keys: {item.get('id', '?')}"
            )
            continue

    df = pd.DataFrame(records)
    if not df.empty:
        df["published_at"] = pd.to_datetime(df["published_at"])
    return df


_ISO8601_DURATION = re.compile(r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?")


def _parse_duration(duration: str) -> int | None:
    # Parse ISO 8601 duration (e.g. 'PT1H30M') → total seconds.
    m = _ISO8601_DURATION.fullmatch(duration)
    if not m:
        return None
    d, h, mi, s = m.groups(default="0")
    return int(d) * 86400 + int(h) * 3600 + int(mi) * 60 + int(s)


def _video_stats_to_df(items: list[dict]) -> pd.DataFrame:
    records = []
    for item in items:
        try:
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            records.append(
                {
                    "video_id": item["id"],
                    "tags": snippet.get("tags", []),
                    "duration_seconds": _parse_duration(
                        item.get("contentDetails", {}).get("duration", "")
                    ),
                    "thumbnail": snippet.get("thumbnails", {})
                    .get("default", {})
                    .get("url", ""),
                    "view_count": stats.get("viewCount"),
                    "like_count": stats.get("likeCount"),
                    "comment_count": stats.get("commentCount"),
                }
            )
        except KeyError:
            logger.warning(f"Skipping video stats item with missing keys")
            continue
    return pd.DataFrame(records)


# ── Channel detail transforms ──────────────────────────────────────


def _channel_stats_to_df(items: list[dict]) -> pd.DataFrame:
    records = []
    for item in items:
        try:
            snippet = item["snippet"]
            stats = item["statistics"]
            records.append(
                {
                    "channel_id": item["id"],
                    "thumbnail": snippet.get("thumbnails", {})
                    .get("default", {})
                    .get("url", ""),
                    "country": snippet.get("country"),
                    "view_count": stats["viewCount"],
                    "subscriber_count": stats["subscriberCount"],
                    "video_count": stats["videoCount"],
                }
            )
        except KeyError:
            logger.warning(f"Skipping channel stats item with missing keys")
            continue
    return pd.DataFrame(records)


# ── Dimension writes ───────────────────────────────────────────────


def insert_discovered_videos(con, df: pd.DataFrame):
    if df.empty:
        return

    today = pendulum.now().to_date_string()
    con.register("tmp", df)

    con.execute(
        """
        INSERT INTO dim_video (video_id, title, description, channel_id, published_at, ingested_date)
        SELECT video_id, title, description, channel_id, published_at, ?
        FROM tmp
        ON CONFLICT (video_id) DO NOTHING
        """,
        [today],
    )
    con.execute(
        """
        INSERT INTO dim_channel (channel_id, channel_name, ingested_date)
        SELECT channel_id, channel_title, ?
        FROM tmp
        ON CONFLICT (channel_id) DO NOTHING
        """,
        [today],
    )


def upsert_video_details(con, df: pd.DataFrame):
    if df.empty:
        return

    con.register("tmp", df)

    # Update dimension columns
    con.execute("""
        UPDATE dim_video AS v
        SET duration_seconds = t.duration_seconds,
            tags = t.tags,
            thumbnail = t.thumbnail
        FROM tmp AS t
        WHERE v.video_id = t.video_id
    """)

    # Write daily fact snapshot
    now = pendulum.now()
    con.execute(
        """
        INSERT OR REPLACE INTO fact_video_daily
            (video_id, snapshot_date, view_count, like_count, comment_count, ingested_at)
        SELECT video_id, ?, view_count, like_count, comment_count, ?
        FROM tmp
        """,
        [now.to_date_string(), now.to_datetime_string()],
    )


def upsert_channel_details(con, df: pd.DataFrame):
    if df.empty:
        return

    con.register("tmp", df)

    # Update dimension columns
    con.execute("""
        UPDATE dim_channel AS c
        SET country = t.country,
            thumbnail = t.thumbnail
        FROM tmp AS t
        WHERE c.channel_id = t.channel_id
    """)

    # Write daily fact snapshot
    now = pendulum.now()
    con.execute(
        """
        INSERT OR REPLACE INTO fact_channel_daily
            (channel_id, snapshot_date, subscriber_count, view_count, video_count, ingested_at)
        SELECT channel_id, ?, subscriber_count, view_count, video_count, ?
        FROM tmp
        """,
        [now.to_date_string(), now.to_datetime_string()],
    )


def get_video_ids(con) -> list[str]:
    try:
        df = con.sql("SELECT video_id FROM dim_video").to_df()
        return list(df["video_id"])
    except Exception:
        logger.exception("Failed to fetch video IDs")
        return []


def get_channel_ids_needing_enrichment(con) -> list[str]:
    try:
        df = con.sql(
            "SELECT channel_id FROM dim_channel WHERE thumbnail IS NULL"
        ).to_df()
        return list(df["channel_id"])
    except Exception:
        logger.exception("Failed to fetch channel IDs")
        return []


def get_agent_names(con) -> list[str]:
    try:
        return con.sql("SELECT name FROM dim_agent").to_df()["name"].tolist()
    except Exception:
        logger.warning("Agent table unavailable")
        return []
