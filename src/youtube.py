"""All YouTube Data API v3 interactions: search, video stats, channel stats.

This module owns every API call. No other module should touch the YouTube client.
"""

import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from src.utils import get_logger

logger = get_logger(__name__)
load_dotenv()

# ── Lazy YouTube client ────────────────────────────────────────────
# Built once on first use, not at import time — so importing this module
# without an API key won't crash (useful for tests / offline work).

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


# ── Search ─────────────────────────────────────────────────────────

def search_videos(query: str, max_results: int, published_after: str) -> list[dict]:
    """Search YouTube for videos matching a query.

    Args:
        query: Search term (e.g. "Zenless Zone Zero Miyabi")
        max_results: Max items to return (1-50)
        published_after: RFC 3339 timestamp — only videos published after this

    Returns:
        List of search result items (snippet only, no statistics).
    """
    logger.info(f"Searching videos | query='{query}'")
    try:
        res = _get_client().search().list(
            q=query,
            part="snippet",
            type="video",
            maxResults=max_results,
            order="relevance",
            publishedAfter=published_after,
        ).execute()
        items = res.get("items", [])
        logger.info(f"Found {len(items)} videos for '{query}'")
        return items
    except HttpError as e:
        logger.error(f"YouTube search API error: {e}")
        return []


# ── Video details ──────────────────────────────────────────────────

def fetch_video_stats(video_ids: list[str]) -> list[dict]:
    """Fetch full metadata + statistics for a batch of video IDs (max 50).

    Returns items with snippet, contentDetails, and statistics.
    """
    if not video_ids:
        return []
    try:
        res = _get_client().videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(video_ids),
        ).execute()
        items = res.get("items", [])
        logger.info(f"Fetched stats for {len(items)} videos")
        return items
    except HttpError as e:
        logger.error(f"YouTube video stats API error: {e}")
        return []


# ── Channel details ────────────────────────────────────────────────

def fetch_channel_stats(channel_ids: list[str]) -> list[dict]:
    """Fetch metadata + statistics for a batch of channel IDs (max 50).

    Returns items with snippet, contentDetails, and statistics.
    """
    if not channel_ids:
        return []
    try:
        res = _get_client().channels().list(
            part="snippet,contentDetails,statistics",
            id=",".join(channel_ids),
        ).execute()
        items = res.get("items", [])
        logger.info(f"Fetched stats for {len(items)} channels")
        return items
    except HttpError as e:
        logger.error(f"YouTube channel stats API error: {e}")
        return []
