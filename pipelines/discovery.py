import json
import os
from dotenv import load_dotenv
import duckdb
from duckdb import DuckDBPyConnection
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import time
import pendulum
import logging
from pathlib import Path

"""
Setup
"""
load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

CONFIG_PATH = Path("pipelines/config.json")
DB_PATH = "data/warehouse.db"
API_KEY = os.getenv("YT_DATA_API")


def discover_videos(youtube, query: str, max_results=10):
    video_data = []

    logger.info(f"searching for {query}")
    try:
        # consumes 100 quota out of 10000, is very costly
        req = youtube.search().list(
            q=query,
            part="snippet",
            type="video",
            maxResults=max_results,
            order="relevance",
            publishedAfter=pendulum.datetime(2024, 1, 1).to_rfc3339_string(),
        )

        res = req.execute()

    except HttpError as e:
        logger.error(f"YouTube API HTTP Error for '{query}': {e}")
    except Exception as e:
        logger.exception(f"Unexpected error during API call: {e}")

    items = res.get("items", [])
    logger.info(f"Fetched {len(items)} videos")

    for item in items:
        try:
            video_data.append(
                {
                    "video_id": item["id"]["videoId"],
                    "title": item["snippet"]["title"],
                    "description": item["snippet"]["description"],
                    "channel_id": item["snippet"]["channelId"],
                    "channel_title": item["snippet"]["channelTitle"],
                    "published_at": item["snippet"]["publishedAt"],
                    "search_query": query,
                    "discovered_at": pendulum.now(),
                }
            )
        except KeyError as e:
            logger.warning(f"Malformed video item skipped: {e}")

    logger.info(f"Total videos discovered for '{query}': {len(video_data)}")
    return video_data


def get_topic_list(con: DuckDBPyConnection):
    topics = [
        "Zenless Zone Zero guide",
        "Zenless Zone Zero character showcase",
        "Zenless Zone Zero tier list",
        "Zenless Zone Zero gameplay",
        "Zenless Zone Zero news",
    ]

    try:
        agent_names = con.sql("SELECT name FROM dim_agent").to_df()["name"]
        for agent_name in agent_names:
            topics.append(f"Zenless Zone Zero {agent_name}")
    except Exception as e:
        logger.warning(f"Could not fetch agents: {e}")

    return topics


def init_tables(con: DuckDBPyConnection):
    logger.info("Initializing tables")
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_video(
           video_id VARCHAR PRIMARY KEY,
           title VARCHAR,
           description VARCHAR,
           channel_id VARCHAR,
           published_at TIMESTAMP,
           thumbnail VARCHAR,
           tags VARCHAR[],
           duration_seconds int,
           ingested_date DATE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_channel(
           channel_id VARCHAR PRIMARY KEY,
           channel_name VARCHAR,
           thumbnail VARCHAR,
           language VARCHAR,
           country VARCHAR,
           ingested_date DATE
        )
    """)


def main():
    logger.info("Pipeline started")

    try:
        con = duckdb.connect(DB_PATH)
        init_tables(con)
    except Exception as e:
        logger.critical(f"Database connection failed: {e}")
        return

    youtube = build(serviceName="youtube", version="v3", developerKey=API_KEY)
    topics = get_topic_list(con)

    for query in topics:
        time.sleep(0.5)
        try:
            result = discover_videos(
                youtube, query, max_results=50
            )  # 50 is hard limit from server side :<

            if not result:
                logger.info(f"No new videos for '{query}'")
                continue

            df = pd.DataFrame(result)
            con.register("video_tmp", df)

            logger.info("Inserting into dim_video")
            con.execute(f"""
            INSERT INTO dim_video (
                video_id, title, description, channel_id, published_at, ingested_date
            ) 
            SELECT 
                video_id,
                title,
                description,
                channel_id,
                published_at,
                '{pendulum.now().to_date_string()}'
            FROM video_tmp
            ON CONFLICT (video_id)
            DO NOTHING
            """)

            logger.info("Inserting into dim_channel")
            con.execute(f"""
            INSERT INTO dim_channel (
                channel_id, channel_name, ingested_date
            ) 
            SELECT 
                channel_id,
                channel_title,
                '{pendulum.now().to_date_string()}'
            FROM video_tmp
            ON CONFLICT (channel_id)
            DO NOTHING
            """)

        except Exception as e:
            logger.exception(f"Failed processing query '{query}': {e}")


if __name__ == "__main__":
    main()
