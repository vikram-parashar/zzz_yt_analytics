import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from src.utils import get_logger

logger = get_logger(__name__)
load_dotenv()


_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client

    api_key = os.getenv("YT_DATA_API")
    if not api_key:
        raise RuntimeError("Missing YT_DATA_API environment variable")
    _client = build("youtube", "v3", developerKey=api_key)
    return _client


def search_videos(
    query: str,
    max_results: int = 50,
    published_after: str | None = None,
    published_before: str | None = None,
    max_pages: int = 10,
) -> list[dict]:
    all_items: list[dict] = []
    page_token: str | None = None
    pages_fetched = 0

    try:
        while pages_fetched < max_pages:
            request = _get_client().search().list(
                q=query,
                part="snippet",
                type="video",
                maxResults=min(max_results, 50),
                order="date",
                publishedAfter=published_after,
                publishedBefore=published_before,
                videoCategoryId=20,
                relevanceLanguage="en",
                pageToken=page_token,
            )
            res = request.execute()
            pages_fetched += 1

            items = res.get("items", [])
            all_items.extend(items)

            page_token = res.get("nextPageToken")
            if not page_token:
                break

        logger.info(
            f"Found {len(all_items)} videos for '{query}' "
            f"across {pages_fetched} page(s)"
        )
        return all_items

    except HttpError as e:
        logger.error(f"YouTube search API error: {e}")
        return all_items 


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
