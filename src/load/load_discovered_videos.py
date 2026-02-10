import logging
import pendulum
import duckdb
from duckdb import DuckDBPyConnection
import pandas as pd

logger = logging.getLogger(__name__)


def connect_db(path: str) -> DuckDBPyConnection:
    return duckdb.connect(path)


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
