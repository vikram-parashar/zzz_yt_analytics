from duckdb import DuckDBPyConnection
import pandas as pd
import pendulum
import utils

logger = utils.get_logger(__name__)


def update_channel_metadata(con: DuckDBPyConnection, df: pd.DataFrame):
    if df.empty:
        return

    con.register("channel_tmp", df)

    logger.info("Updating dim_channel table")
    con.execute("""
        UPDATE dim_channel AS v
        SET 
            country = t.country,
            thumbnail = t.thumbnail
        FROM channel_tmp AS t
        WHERE v.channel_id = t.channel_id
    """)
    logger.info("Inserting into fact_channel_daily table")
    con.execute(
        """
        INSERT OR REPLACE INTO fact_channel_daily (
            channel_id, snapshot_date, subscriber_count, view_count, video_count, ingested_at
        )
        SELECT 
            channel_id,
            ?,
            subscriber_count,
            view_count,
            video_count,
            ?
        FROM channel_tmp
    """,
        [pendulum.now().to_date_string(), pendulum.now().to_datetime_string()],
    )
