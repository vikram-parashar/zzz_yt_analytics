import json
import os
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

API_KEY = os.getenv("YT_DATA_API")

if not API_KEY:
    raise RuntimeError("Missing YouTube Data API key")

youtube_client = build("youtube", "v3", developerKey=API_KEY)


def fetch_channel_metadata(channel_ids: list[str]) -> list[dict]:
    results = []

    try:
        res = (
            youtube_client.channels()
            .list(part="snippet,contentDetails", id=",".join(channel_ids))
            .execute()
        )
        # https://developers.google.com/youtube/v3/docs/channels#resource

    except HttpError as e:
        logger.error(f"YouTube API HTTP error: {e}")
    except Exception as e:
        logger.exception(f"Unexpected API error: {e}")

    items = res.get("items", [])
    logger.info(f"Received {len(items)} channel records")
    results.extend(items)

    return results
