import duckdb
from extract.discover_channels import fetch_channel_metadata
from load.load_channel_metadata import update_channel_metadata
from transform.channel_to_df import channel_metadata_df
import utils

logger = utils.get_logger(__name__)
config = utils.load_config()


def get_channel_ids(con) -> list[str]:
    logger.info("Fetching channel IDs from warehouse")
    try:
        df = con.sql(
            "SELECT channel_id FROM dim_channel WHERE thumbnail IS NULL"
        ).to_df()
        logger.info(f"{len(df)} channel to enrich")
        return list(df["channel_id"])
    except Exception as e:
        logger.exception("Failed to fetch channel IDs")
        return []


def get_chunks(arr: list[str], size: int) -> list[list[str]]:
    n = len(arr)
    res = []
    i = 0
    while i < n:
        res.append(arr[i : min(i + size, n)])
        i += size
    return res


def enrich_channels_func():
    con = duckdb.connect(config["db"]["path"])

    channel_ids = get_channel_ids(con)
    channel_id_chunks = get_chunks(channel_ids, 50)

    for channel_id_chunk in channel_id_chunks:
        items = fetch_channel_metadata(channel_id_chunk)
        df = channel_metadata_df(items)
        update_channel_metadata(con, df)


if __name__ == "__main__":
    enrich_channels_func()
