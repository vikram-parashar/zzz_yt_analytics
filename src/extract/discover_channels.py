import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
import utils

logger = utils.get_logger(__name__)
load_dotenv()

API_KEY = os.getenv("YT_DATA_API")

if not API_KEY:
    raise RuntimeError("Missing YouTube Data API key")

youtube_client = build("youtube", "v3", developerKey=API_KEY)


def fetch_channel_metadata(channel_ids: list[str]) -> list[dict]:
    results = []

    try:
        res = (
            youtube_client.channels()
            .list(part="snippet,contentDetails,statistics", id=",".join(channel_ids))
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
