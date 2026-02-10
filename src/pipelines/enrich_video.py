import logging

import duckdb
from extract.discover_videos import fetch_video_metadata
from load.load_discovered_videos import update_video_metadata
from load_config import load_config
from transform.videos_to_df import video_metadata_df

logger = logging.getLogger(__name__)
config = load_config()


def get_video_ids(con) -> list[str]:
    logger.info("Fetching video IDs from warehouse")
    try:
        df = con.sql(
            "SELECT video_id FROM dim_video WHERE duration_seconds IS NULL"
        ).to_df()
        logger.info(f"{len(df)} videos to enrich")
        return list(df["video_id"])
    except Exception as e:
        logger.exception("Failed to fetch video IDs")
        return []


def get_chunks(arr: list[str], size: int) -> list[list[str]]:
    n = len(arr)
    res = []
    i = 0
    while i < n:
        res.append(arr[i : min(i + size, n)])
        i += size
    return res


def enrich_videos_func():
    con = duckdb.connect(config["db"]["path"])

    video_ids = get_video_ids(con)
    video_id_chunks = get_chunks(video_ids, 50)

    for video_id_chunk in video_id_chunks:
        items = fetch_video_metadata(video_id_chunk)
        df = video_metadata_df(items)
        update_video_metadata(con, df)


if __name__ == "__main__":
    enrich_videos_func()
