import os
import logging
import pandas as pd
import pendulum
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from duckdb import DuckDBPyConnection

load_dotenv()
logger = logging.getLogger(__name__)

API_KEY = os.getenv("YT_DATA_API")

if not API_KEY:
    raise RuntimeError("Missing YouTube Data API key")

youtube_client = build("youtube", "v3", developerKey=API_KEY)


def get_topic_list(con: DuckDBPyConnection) -> list[str]:
    topics = [
        "Zenless Zone Zero guide",
        "Zenless Zone Zero character showcase",
        "Zenless Zone Zero tier list",
        "Zenless Zone Zero gameplay",
        "Zenless Zone Zero news",
    ]

    try:
        agent_names = con.sql("SELECT name FROM dim_agent").to_df()["name"]
        for agent in agent_names:
            topics.append(f"Zenless Zone Zero {agent}")
    except Exception:
        logger.warning("Agent table unavailable")

    return topics


def discover_videos(query: str, max_results: int):
    logger.info(f"Extracting videos | query='{query}'")

    try:
        req = youtube_client.search().list(
            q=query,
            part="snippet",
            type="video",
            maxResults=max_results,
            order="relevance",
            publishedAfter=pendulum.datetime(2024, 1, 1).to_rfc3339_string(),
        )
        # https://developers.google.com/youtube/v3/docs/videos#resource
        res = req.execute()
        items = res.get("items", [])

    except HttpError as e:
        logger.error(f"YouTube API error: {e}")

    return items
