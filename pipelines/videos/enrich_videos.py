import duckdb
import os
import re
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import logging
import time

"""
Setup
"""
load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

API_KEY = os.getenv("YT_DATA_API")
DB_PATH = "data/warehouse.db"

VIDEO_BATCH_SIZE = 50

"""
YT Client Init
"""
try:
    youtube = build(serviceName="youtube", version="v3", developerKey=API_KEY)
except Exception as e:
    logger.critical(f"Failed to initialize YouTube client: {e}")
    raise


def get_video_ids(con) -> list[str]:
    logger.info("Fetching video IDs from warehouse")
    try:
        df = con.sql("SELECT video_id FROM dim_video").to_df()
        logger.info(f"{len(df)} videos to enrich")
        return list(df["video_id"])
    except Exception as e:
        logger.exception("Failed to fetch video IDs")
        return []


def chunk(arr, batch_size: int):
    for i in range(0, len(arr), batch_size):
        yield arr[i : i + batch_size]


ISO_8601_DURATION_REGEX = re.compile(
    r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?"
)


def parse_iso8601_duration(duration: str) -> int | None:
    try:
        match = ISO_8601_DURATION_REGEX.fullmatch(duration)
        if not match:
            raise ValueError

        days, hours, minutes, seconds = match.groups(default="0")
        return int(days) * 86400 + int(hours) * 3600 + int(minutes) * 60 + int(seconds)
    except Exception:
        logger.warning(f"Failed to parse duration: {duration}")
        return None


def fetch_video_metadata(video_ids: list[str]) -> list[dict]:
    results = []

    for batch_num, batch in enumerate(chunk(video_ids, VIDEO_BATCH_SIZE), start=1):
        logger.info(f"Processing batch {batch_num} with {len(batch)} videos")

        try:
            res = (
                youtube.videos()
                .list(part="snippet,contentDetails", id=",".join(batch))
                .execute()
            )

        except HttpError as e:
            logger.error(f"YouTube API HTTP error: {e}")
            continue
        except Exception as e:
            logger.exception(f"Unexpected API error: {e}")
            continue

        items = res.get("items", [])
        logger.info(f"Received {len(items)} video records")

        for item in items:
            try:
                results.append(
                    {
                        "video_id": item["id"],
                        "tags": item["snippet"].get("tags", []),
                        "duration_seconds": parse_iso8601_duration(
                            item["contentDetails"]["duration"]
                        ),
                        "thumbnail": item["snippet"]["thumbnails"]
                        .get("default", {})
                        .get("url", ""),
                    }
                )
            except Exception as e:
                logger.warning(f"Malformed video metadata skipped: {e}")

        time.sleep(0.2)

    logger.info(f"Total enriched videos: {len(results)}")
    return results


def main():
    logger.info("Video enrichment job started")

    try:
        con = duckdb.connect(DB_PATH)
    except Exception as e:
        logger.critical(f"DB connection failed: {e}")
        return

    video_ids = get_video_ids(con)
    if not video_ids:
        logger.warning("No videos found to enrich")
        return

    video_metadata = fetch_video_metadata(video_ids)

    if not video_metadata:
        logger.warning("No metadata fetched; skipping update")
        return

    try:
        df = pd.DataFrame(video_metadata)
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

        logger.info("Database update successful")

    except Exception as e:
        logger.exception(f"Database update failed: {e}")

    logger.info("Video enrichment job completed")


if __name__ == "__main__":
    main()
