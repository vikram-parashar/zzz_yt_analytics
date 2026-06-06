import os
import time
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from src.utils import get_logger

logger = get_logger(__name__)
load_dotenv()


_client = None

SEARCH_DELAY_SECONDS = 2.0
MAX_RETRIES = 5
RETRY_BASE_DELAY = 10


def _get_client():
    global _client
    if _client is not None:
        return _client

    api_key = os.getenv("YT_DATA_API")
    if not api_key:
        raise RuntimeError("Missing YT_DATA_API environment variable")
    _client = build("youtube", "v3", developerKey=api_key)
    return _client


def _is_rate_limit_error(e: HttpError) -> bool:
    if e.resp.status == 429:
        return True
    if e.resp.status == 403:
        try:
            reasons = [d.get("reason", "") for d in e.error_details]
            if "rateLimitExceeded" in reasons or "quotaExceeded" in reasons:
                return True
        except Exception:
            pass
    return False


def search_videos(
    query: str,
    published_after: str,
    published_before: str,
    order: str = "date",
) -> list[dict]:
    logger.info(
        f"Searching videos | query='{query}' | order={order} | "
        f"after={published_after} | before={published_before}"
    )

    params = {
        "q": query,
        "part": "snippet",
        "type": "video",
        "maxResults": 50,
        "order": order,
        "videoCategoryId": 20,
        "relevanceLanguage": "en",
        "publishedBefore": published_before,
        "publishedAfter": published_after,
    }

    for attempt in range(MAX_RETRIES + 1):
        try:
            res = _get_client().search().list(**params).execute()
            items = res.get("items", [])
            logger.info(f"Found {len(items)} videos for '{query}' (order={order})")
            return items

        except HttpError as e:
            if _is_rate_limit_error(e):
                if attempt < MAX_RETRIES:
                    delay = RETRY_BASE_DELAY * (2**attempt)
                    logger.warning(
                        f"Rate limited (429) on attempt {attempt + 1}/{MAX_RETRIES + 1}. "
                        f"Waiting {delay}s before retry..."
                    )
                    time.sleep(delay)
                    continue
                else:
                    logger.error(
                        f"Rate limited (429) after {MAX_RETRIES + 1} attempts. Giving up."
                    )
                    raise RuntimeError(
                        f"YouTube API rate limit exceeded after {MAX_RETRIES + 1} retries. "
                        f"Last error: {e}"
                    ) from e
            else:
                logger.error(f"YouTube search API error: {e}")
                return []

        except Exception as e:
            logger.error(f"Unexpected error during YouTube search: {e}")
            return []

    return []


def fetch_video_stats(video_ids: list[str]) -> list[dict]:
    if not video_ids:
        return []
    try:
        res = (
            _get_client()
            .videos()
            .list(
                part="snippet,contentDetails,statistics",
                id=",".join(video_ids),
            )
            .execute()
        )
        items = res.get("items", [])
        logger.info(f"Fetched stats for {len(items)} videos")
        return items
    except HttpError as e:
        logger.error(f"YouTube video stats API error: {e}")
        return []


def fetch_channel_stats(channel_ids: list[str]) -> list[dict]:
    if not channel_ids:
        return []
    try:
        res = (
            _get_client()
            .channels()
            .list(
                part="snippet,contentDetails,statistics",
                id=",".join(channel_ids),
            )
            .execute()
        )
        items = res.get("items", [])
        logger.info(f"Fetched stats for {len(items)} channels")
        return items
    except HttpError as e:
        logger.error(f"YouTube channel stats API error: {e}")
        return []
