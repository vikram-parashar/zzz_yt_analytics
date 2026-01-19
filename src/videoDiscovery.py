import os
from dotenv import load_dotenv
from googleapiclient.discovery import build
import pandas as pd
import time
import pendulum

load_dotenv()

API_KEY = os.getenv("YT_DATA_API")
youtube = build(serviceName="youtube", version="v3", developerKey=API_KEY)

data_path = "data"


def get_last_discovery():
    path = f"{data_path}/dim_videos_discovered.csv"

    if not os.path.exists(path):
        return None

    df = pd.read_csv(path)

    if "discovered_at" not in df.columns:
        return None

    dates = pd.to_datetime(df["discovered_at"], errors="coerce")

    if dates.isna().all():
        return None

    return pendulum.instance(dates.max())


def get_videos(
    query: str,
    last_discovery,
    max_results=10,
    pages=1,
):
    video_data = []
    page_token = None

    for page in range(pages):
        print(f"finding {query} on page {page + 1}")
        req = youtube.search().list(
            q=query,
            part="snippet",
            type="video",
            maxResults=max_results,
            order="relevance",
            pageToken=page_token,
            publishedAfter=last_discovery.to_rfc3339_string(),  # https://developers.google.com/youtube/v3/docs/search/list#publishedAfter,
        )

        res = req.execute()

        for item in res.get("items", []):
            video_data.append(
                {
                    "video_id": item["id"]["videoId"],
                    "title": item["snippet"]["title"],
                    "description": item["snippet"]["description"],
                    "channel_id": item["snippet"]["channelId"],
                    "channel_title": item["snippet"]["channelTitle"],
                    "publish_date": item["snippet"]["publishedAt"],
                    "search_query": query,
                    "discovered_at": pendulum.now(),
                }
            )

        page_token = res.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.5)

    return video_data


def get_topic_list():
    topics = [
        "Zenless Zone Zero guide",
        "Zenless Zone Zero character showcase",
        "Zenless Zone Zero deadly assualt",
        "Zenless Zone Zero shiyu defence",
        "Zenless Zone Zero tier list",
        "Zenless Zone Zero news",
        "Zenless Zone Zero gameplay",
        "Zenless Zone Zero cosplay",
    ]
    agents = pd.read_csv(f"{data_path}/dim_agent.csv")["name"]
    for agent in agents:
        topics.append(f"Zenless Zone Zero {agent}")
    return topics


def main():
    last_discovery = get_last_discovery()
    if last_discovery is None:
        last_discovery = pendulum.now("UTC").subtract(days=90)

    topics = get_topic_list()

    videos = []
    for query in topics:
        videos.extend(get_videos(query, last_discovery, max_results=50, pages=1))
    df = pd.DataFrame(videos)
    df.drop_duplicates(subset=["video_id"])

    if not os.path.exists(data_path):
        os.mkdir(data_path)
    df.to_csv(f"{data_path}/dim_videos.csv", index=False)
