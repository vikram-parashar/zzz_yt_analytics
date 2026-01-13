import os
from dotenv import load_dotenv
from googleapiclient.discovery import build
import pandas as pd
import time

load_dotenv()


API_KEY = os.getenv("YT_DATA_API")
youtube = build(serviceName="youtube", version="v3", developerKey=API_KEY)


def get_seed_videos(query: str, max_results=10, pages=1):
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
                }
            )

        page_token = res.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.5)

    return video_data


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

videos = []
for query in topics:
    videos.extend(get_seed_videos(query, max_results=50, pages=3))  # max goes till 50


df = pd.DataFrame(videos)

data_path = "data"
if not os.path.exists(data_path):
    os.mkdir(data_path)
df.to_csv(f"{data_path}/dim_videos_discovered.csv", index=False)
