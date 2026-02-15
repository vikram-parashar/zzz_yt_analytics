import pendulum
from duckdb import DuckDBPyConnection
import pandas as pd
import utils

logger = utils.get_logger(__name__)


def load_discovered_videos(con: DuckDBPyConnection, df: pd.DataFrame):
    if df.empty:
        return

    con.register("video_tmp", df)

    con.execute(
        """
        INSERT INTO dim_video (
            video_id, title, description, channel_id, published_at, ingested_date
        )
        SELECT
            video_id,
            title,
            description,
            channel_id,
            published_at,
            ?
        FROM video_tmp
        ON CONFLICT (video_id) DO NOTHING
        """,
        [pendulum.now().to_date_string()],
    )

    con.execute(
        """
        INSERT INTO dim_channel (
            channel_id, channel_name, ingested_date
        )
        SELECT
            channel_id,
            channel_title,
            ?
        FROM video_tmp
        ON CONFLICT (channel_id) DO NOTHING
        """,
        [pendulum.now().to_date_string()],
    )


def update_video_metadata(con: DuckDBPyConnection, df: pd.DataFrame):
    if df.empty:
        logger.info("Recived empty df")
        return

    con.register("video_tmp", df)

    logger.info("Updating dim_video table")
    con.execute("""
        UPDATE dim_video AS v
        SET 
            duration_seconds = t.duration_seconds,
            tags = t.tags,
            thumbnail = t.thumbnail
        FROM video_tmp AS t
        WHERE v.video_id = t.video_id
    """)

    con.execute(
        """
        INSERT OR REPLACE INTO fact_video_daily (
            video_id, snapshot_date, view_count, like_count, comment_count, dislike_count, favorite_count, ingested_at
        )
        SELECT
            video_id,
            ?,
            view_count,
            like_count,
            comment_count,
            dislike_count,
            favorite_count,
            ?
        FROM video_tmp
    """,
        [pendulum.now().to_date_string(), pendulum.now().to_datetime_string()],
    )
