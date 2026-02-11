import logging
from duckdb import DuckDBPyConnection
import pandas as pd

logger = logging.getLogger(__name__)


def update_channel_metadata(con: DuckDBPyConnection, df: pd.DataFrame):
    if df.empty:
        return

    con.register("channel_tmp", df)

    print(df)
    logger.info("Updating dim_channel table")
    con.execute("""
        UPDATE dim_channel AS v
        SET 
            country = t.country,
            thumbnail = t.thumbnail
        FROM channel_tmp AS t
        WHERE v.channel_id = t.channel_id
    """)
