import os
import pendulum
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from duckdb import DuckDBPyConnection
import utils

logger = utils.get_logger(__name__)
load_dotenv()
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


def discover_videos(query: str, max_results: int, publishedAfter: pendulum.DateTime):
    logger.info(f"Extracting videos | query='{query}'")

    try:
        req = youtube_client.search().list(
            q=query,
            part="snippet",
            type="video",
            maxResults=max_results,
            order="relevance",
            publishedAfter=publishedAfter.to_rfc3339_string(),
        )
        # https://developers.google.com/youtube/v3/docs/videos#resource
        res = req.execute()
        items = res.get("items", [])

    except HttpError as e:
        logger.error(f"YouTube API error: {e}")

    return items


def fetch_video_metadata(video_ids: list[str]) -> list[dict]:
    results = []

    try:
        res = (
            youtube_client.videos()
            .list(part="snippet,contentDetails,statistics", id=",".join(video_ids))
            .execute()
        )
        # https://developers.google.com/youtube/v3/docs/videos#resource

    except HttpError as e:
        logger.error(f"YouTube API HTTP error: {e}")
    except Exception as e:
        logger.exception(f"Unexpected API error: {e}")

    items = res.get("items", [])
    logger.info(f"Received {len(items)} video records")
    results.extend(items)

    return results
