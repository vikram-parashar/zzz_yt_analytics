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
            release_date DATE
        )
    """,
    "bridge_agent_alias": """
        CREATE TABLE IF NOT EXISTS bridge_agent_alias (
            name VARCHAR,
            alias VARCHAR,
            PRIMARY KEY (name, alias)
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
            ingested_date DATE,
            latest_view_count BIGINT,
            latest_like_count BIGINT,
            latest_comment_count BIGINT,
            is_matched BOOLEAN DEFAULT FALSE,
            discovery_type VARCHAR DEFAULT 'popular'
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
            PRIMARY KEY (video_id, snapshot_date)
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
            PRIMARY KEY (channel_id, snapshot_date)
        )
    """,
    "bridge_video_agent": """
        CREATE TABLE IF NOT EXISTS bridge_video_agent (
            video_id VARCHAR,
            agent_name VARCHAR,
            confidence REAL,
            attribution_weight REAL DEFAULT 0.0,
            PRIMARY KEY (video_id, agent_name)
        )
    """,
    "dim_patch": """
        CREATE TABLE IF NOT EXISTS dim_patch (
            version      VARCHAR,
            agent_name   VARCHAR,
            banner_start DATE,
            banner_end   DATE,
            PRIMARY KEY (version, agent_name)
        )
    """,
    "pipeline_runs_seq": """
        CREATE SEQUENCE IF NOT EXISTS pipeline_runs_seq START 1
    """,
    "pipeline_runs": """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id           INTEGER PRIMARY KEY DEFAULT nextval('pipeline_runs_seq'),
            pipeline     VARCHAR NOT NULL,
            run_date     DATE    NOT NULL,
            started_at   TIMESTAMP,
            completed_at TIMESTAMP,
            status       VARCHAR DEFAULT 'running',
            rows_affected INTEGER DEFAULT 0,
            error        VARCHAR
        )
    """,
    "fact_agent_daily": """
        CREATE TABLE IF NOT EXISTS fact_agent_daily (
            agent_name VARCHAR,
            snapshot_date DATE,
            attributed_views BIGINT,
            attributed_likes BIGINT,
            attributed_comments BIGINT,
            video_count BIGINT,
            PRIMARY KEY (agent_name, snapshot_date)
        )
    """,
    "pipeline_info": """
        CREATE TABLE IF NOT EXISTS pipeline_info (
            key   VARCHAR PRIMARY KEY,
            value VARCHAR
        )
    """,
}


def init_tables():
    with get_db() as con:
        for _, ddl in TABLE_DDL.items():
            con.execute(ddl)


def get_pipeline_info(key: str, default: str | None = None) -> str | None:
    with get_db() as con:
        try:
            result = con.execute(
                "SELECT value FROM pipeline_info WHERE key = ?", [key]
            ).fetchone()
            return result[0] if result else default
        except Exception:
            logger.debug(f"pipeline_info read failed for key={key}")
            return default


def set_pipeline_info(key: str, value: str):
    with get_db() as con:
        con.execute(
            """
            INSERT INTO pipeline_info (key, value) VALUES (?, ?)
            ON CONFLICT (key) DO UPDATE SET value = excluded.value
            """,
            [key, value],
        )


def start_pipeline_run(pipeline: str) -> int:
    now = pendulum.now()
    with get_db() as con:
        run_id = con.execute(
            """
            INSERT INTO pipeline_runs (pipeline, run_date, started_at, status)
            VALUES (?, ?, ?, 'running')
            RETURNING id
            """,
            [pipeline, now.to_date_string(), now.to_datetime_string()],
        ).fetchone()[0]
    return run_id


def finish_pipeline_run(run_id: int, rows_affected: int = 0, error: str | None = None):
    status = "failed" if error else "completed"
    now = pendulum.now()
    with get_db() as con:
        con.execute(
            """
            UPDATE pipeline_runs
            SET completed_at = ?, status = ?, rows_affected = ?, error = ?
            WHERE id = ?
            """,
            [now.to_datetime_string(), status, rows_affected, error, run_id],
        )
    logger.info(f"pipeline_runs | {status} | id={run_id} rows={rows_affected}")


def did_pipeline_run_today(pipeline: str) -> bool:
    today = pendulum.now().to_date_string()
    with get_db() as con:
        result = con.execute(
            """
            SELECT COUNT(*)
            FROM pipeline_runs
            WHERE pipeline = ?
              AND run_date  = ?
              AND status    = 'completed'
            """,
            [pipeline, today],
        ).fetchone()
    return result[0] > 0


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
            logger.warning("Skipping video stats item with missing keys")
            continue
    return pd.DataFrame(records)


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
            logger.warning("Skipping channel stats item with missing keys")
            continue
    return pd.DataFrame(records)




def insert_discovered_videos(con, df: pd.DataFrame, discovery_type: str = "popular"):
    if df.empty:
        return []

    today = pendulum.now().to_date_string()
    con.register("tmp_video_search", df)

    con.execute(
        """
        INSERT INTO dim_video (video_id, title, description, channel_id, published_at, ingested_date, discovery_type)
        SELECT video_id, title, description, channel_id, published_at, ?, ?
        FROM tmp_video_search
        ON CONFLICT (video_id) DO NOTHING
        """,
        [today, discovery_type],
    )
    con.execute(
        """
        INSERT INTO dim_channel (channel_id, channel_name, ingested_date)
        SELECT channel_id, channel_title, ?
        FROM tmp_video_search
        ON CONFLICT (channel_id) DO NOTHING
        """,
        [today],
    )


def upsert_video_details(con, df: pd.DataFrame):
    if df.empty:
        return

    con.register("tmp_video_stats", df)

    con.execute("""
        UPDATE dim_video AS v
        SET duration_seconds = t.duration_seconds,
            tags = t.tags,
            thumbnail = t.thumbnail
        FROM tmp_video_stats AS t
        WHERE v.video_id = t.video_id
    """)

    now = pendulum.now()
    con.execute(
        """
        INSERT OR REPLACE INTO fact_video_daily
            (video_id, snapshot_date, view_count, like_count, comment_count, ingested_at)
        SELECT video_id, ?, view_count, like_count, comment_count, ?
        FROM tmp_video_stats
        """,
        [now.to_date_string(), now.to_datetime_string()],
    )

    con.execute("""
        UPDATE dim_video AS dv
        SET latest_view_count = t.view_count,
            latest_like_count = t.like_count,
            latest_comment_count = t.comment_count
        FROM tmp_video_stats AS t
        WHERE dv.video_id = t.video_id
    """)


def upsert_channel_details(con, df: pd.DataFrame):
    if df.empty:
        return

    con.register("tmp_channel_stats", df)

    con.execute("""
        UPDATE dim_channel AS c
        SET country = t.country,
            thumbnail = t.thumbnail
        FROM tmp_channel_stats AS t
        WHERE c.channel_id = t.channel_id
    """)

    now = pendulum.now()
    con.execute(
        """
        INSERT OR REPLACE INTO fact_channel_daily
            (channel_id, snapshot_date, subscriber_count, view_count, video_count, ingested_at)
        SELECT channel_id, ?, subscriber_count, view_count, video_count, ?
        FROM tmp_channel_stats
        """,
        [now.to_date_string(), now.to_datetime_string()],
    )




def get_enrichment_ids(con) -> tuple[list[str], list[str]]:
    query = """
        WITH target_videos AS (
            SELECT DISTINCT bva.video_id, dv.channel_id
            FROM bridge_video_agent bva
            JOIN dim_video dv
                ON bva.video_id = dv.video_id
            WHERE
                CASE
                    WHEN EXTRACT(DAY FROM CURRENT_DATE) = 1 THEN TRUE

                    WHEN EXTRACT(ISODOW FROM CURRENT_DATE) = 1 THEN
                        dv.published_at >= CURRENT_DATE - INTERVAL '3 months'

                    ELSE
                        dv.published_at >= CURRENT_DATE - INTERVAL '30 days'
                END
        ),

        missing_view_videos AS (
            SELECT video_id, channel_id
            FROM dim_video
            WHERE latest_view_count IS NULL
        )

        SELECT DISTINCT video_id, channel_id
        FROM (
            SELECT video_id, channel_id FROM target_videos
            UNION ALL
            SELECT video_id, channel_id FROM missing_view_videos
        ) t
        WHERE video_id IS NOT NULL
    """

    try:
        df = con.execute(query).fetchdf()
    except Exception:
        logger.exception("Failed to fetch enrichment IDs")
        return [], []

    if df.empty:
        logger.info("No videos to enrich")
        return [], []

    video_ids = df["video_id"].astype(str).unique().tolist()
    channel_ids = df["channel_id"].dropna().astype(str).unique().tolist()

    return video_ids, channel_ids


def get_agent_names(con) -> list[str]:
    try:
        return con.sql("SELECT name FROM dim_agent").to_df()["name"].tolist()
    except Exception:
        logger.warning("Agent table unavailable")
        return []




def update_attribution_weights(con):
    con.execute("""
        UPDATE bridge_video_agent AS b
        SET attribution_weight = b.confidence / total_conf
        FROM (
            SELECT video_id, SUM(confidence) AS total_conf
            FROM bridge_video_agent
            GROUP BY video_id
        ) AS totals
        WHERE b.video_id = totals.video_id
          AND totals.total_conf > 0
    """)


def build_fact_agent_daily(con, snapshot_date: str | None = None):
    if snapshot_date:
        con.execute(
            "DELETE FROM fact_agent_daily WHERE snapshot_date = ?",
            [snapshot_date],
        )
    else:
        con.execute("DELETE FROM fact_agent_daily")

    con.execute(
        f"""
        INSERT INTO fact_agent_daily
            (agent_name, snapshot_date,
             attributed_views, attributed_likes, attributed_comments, video_count)
        SELECT
            b.agent_name,
            f.snapshot_date,
            ROUND(SUM(b.attribution_weight * f.view_count))     AS attributed_views,
            ROUND(SUM(b.attribution_weight * f.like_count))     AS attributed_likes,
            ROUND(SUM(b.attribution_weight * f.comment_count))  AS attributed_comments,
            COUNT(DISTINCT b.video_id)                    AS video_count
        FROM bridge_video_agent AS b
        JOIN fact_video_daily   AS f
          ON b.video_id = f.video_id
        {"WHERE f.snapshot_date = ?" if snapshot_date else ""}
        GROUP BY b.agent_name, f.snapshot_date
    """,
        [snapshot_date] if snapshot_date else [],
    )

    row_count = con.execute("SELECT COUNT(*) FROM fact_agent_daily").fetchone()[0]
    logger.info(
        f"Built fact_agent_daily ({'date=' + snapshot_date if snapshot_date else 'full rebuild'}): "
        f"{row_count} rows"
    )
