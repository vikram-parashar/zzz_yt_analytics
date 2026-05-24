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
    published_after: str,
    published_before: str,
    page_token: str | None = None,
) -> tuple[list[dict], str | None]:
    logger.info(
        f"Searching videos | query='{query}' "
        f"after={published_after} | before={published_before} | "
    )
    try:
        params = {
            "q": query,
            "part": "snippet",
            "type": "video",
            "maxResults": 50,
            "order": "date",
            "videoCategoryId": "20",
            "relevanceLanguage": "en",
            "publishedBefore": published_before,
            "publishedAfter": published_after,
        }
        if page_token:
            params["pageToken"] = page_token

        res = _get_client().search().list(**params).execute()
        items = res.get("items", [])
        next_token = res.get("nextPageToken")
        logger.info(
            f"Found {len(items)} videos for '{query}' "
            f"(nextPageToken={'yes' if next_token else 'none'})"
        )
        return items, next_token
    except HttpError as e:
        logger.error(f"YouTube search API error: {e}")
        return [], None


def search_videos_paginated(
    query: str,
    max_pages: int = 1,
    published_after: str | None = None,
    published_before: str | None = None,
) -> list[dict]:
    all_items = []
    next_token = None

    for page in range(max_pages):
        items, next_token = search_videos(
            query=query,
            published_after=published_after,
            published_before=published_before,
            page_token=next_token,
        )
        all_items.extend(items)

        if not next_token:
            break

        logger.info(
            f"Paginated search page {page + 1}/{max_pages}, got {len(items)} items"
        )

    logger.info(
        f"Paginated search complete: {len(all_items)} total items across {min(page + 1, max_pages)} pages"
    )
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
