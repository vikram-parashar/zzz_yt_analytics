import os
import ast
import re
from dotenv import load_dotenv
from googleapiclient.discovery import build
import pandas as pd
import pendulum

load_dotenv()

API_KEY = os.getenv("YT_DATA_API")
youtube = build(serviceName="youtube", version="v3", developerKey=API_KEY)

VIDEO_BATCH_SIZE = 50
CHANNEL_BATCH_SIZE = 50

data_path = "data"

try:
    videos_df = pd.read_csv(f"{data_path}/videos.csv")
except FileNotFoundError:
    print("ERROR: first run video discovery to generate data/videos.csv")
    exit()


def chunk(arr, batch_size: int):
    res = []
    i = 0
    n = len(arr)
    while i < n:
        res.append(arr[i : min(n, i + batch_size)])
        i += batch_size
    return res


ISO_8601_DURATION_REGEX = re.compile(
    r"P"  # starts with 'P'
    r"(?:(\d+)D)?"  # days
    r"(?:T"  # time component
    r"(?:(\d+)H)?"  # hours
    r"(?:(\d+)M)?"  # minutes
    r"(?:(\d+)S)?"  # seconds
    r")?"  # time component optional
)


def parse_iso8601_duration(duration: str) -> int:
    if not duration or not duration.startswith("P"):
        raise ValueError(f"Invalid ISO 8601 duration: {duration}")

    match = ISO_8601_DURATION_REGEX.fullmatch(duration)
    if not match:
        raise ValueError(f"Invalid ISO 8601 duration: {duration}")

    days, hours, minutes, seconds = match.groups(default="0")

    total_seconds = (
        int(days) * 86400 + int(hours) * 3600 + int(minutes) * 60 + int(seconds)
    )

    return total_seconds


def fetch_video_metadata(video_ids: list[str]) -> list[dict]:
    results = []

    for batch in chunk(video_ids, VIDEO_BATCH_SIZE):
        res = (
            youtube.videos()
            .list(part="snippet,statistics,contentDetails", id=",".join(batch))
            .execute()
        )

        for item in res.get("items", []):
            results.append(
                {
                    "video_id": item["id"],
                    "channel_id": item["snippet"]["channelId"],
                    "title": item["snippet"]["title"],
                    "description": item["snippet"].get("description", ""),
                    "tags": item["snippet"].get("tags", []),
                    "published_at": pendulum.parse(item["snippet"]["publishedAt"]),
                    "duration_seconds": parse_iso8601_duration(
                        item["contentDetails"]["duration"]
                    ),  # https://developers.google.com/youtube/v3/docs/videos#contentDetails.duration
                    "views": int(item["statistics"].get("viewCount", 0)),
                    "likes": int(item["statistics"].get("likeCount", 0)),
                    "comments": int(item["statistics"].get("commentCount", 0)),
                }
            )

    return results


video_ids = list(videos_df["video_id"])
print(f"found {len(video_ids)} videos from csv")
video_metadata = fetch_video_metadata(video_ids)
print(f"fetched metadata of {len(video_metadata)} videos")


def fetch_channel_metadata(channel_ids: list[str]) -> dict:
    channels = {}

    for batch in chunk(channel_ids, CHANNEL_BATCH_SIZE):
        res = (
            youtube.channels()
            .list(part="snippet,statistics", id=",".join(batch))
            .execute()
        )

        for item in res.get("items", []):
            channels[item["id"]] = {
                "channel_id": item["id"],
                "channel_name": item["snippet"]["title"],
                "subscribers": int(item["statistics"].get("subscriberCount", 0)),
                "video_count": int(item["statistics"].get("videoCount", 0)),
                "created_at": pendulum.parse(item["snippet"]["publishedAt"]),
            }

    return channels


channel_ids = list(set(videos_df["channel_id"]))
print(f"csv has {len(channel_ids)} channels")
channel_metadata = fetch_channel_metadata(channel_ids)
print(f"fetched metadata of {len(channel_metadata)} channels")


def compute_engagement(video: dict) -> float:
    if video["views"] == 0:
        return 0.0
    return (video["likes"] + video["comments"]) / video["views"]


try:
    agents = pd.read_csv(f"{data_path}/dim_agent.csv")
except FileNotFoundError:
    print("ERROR: first run agentScraping to generate data/dim_agent.csv")
    exit()
agents["aliases"] = agents["aliases"].apply(ast.literal_eval)


def extract_characters(title: str, description: str, tags: list[str]) -> list[dict]:
    text = f"{title} {description} {' '.join(tags)}".lower()

    matched = []

    for _, agent in agents.iterrows():
        if any(alias.lower() in text for alias in agent["aliases"]):
            matched.append(agent["name"])

    weight = 1 / len(matched) if matched else 0

    return [{"name": name, "weight": weight} for name in matched]


def build_enriched_video_record(video: dict, channel: dict) -> dict:
    engagement_rate = compute_engagement(video)

    characters = extract_characters(
        title=video["title"], description=video["description"], tags=video["tags"]
    )

    return {
        **video,
        "channel_name": channel["channel_name"],
        "subscribers": channel["subscribers"],
        "engagement_rate": engagement_rate,
        "publish_hour": video["published_at"].hour,
        "day_of_week": video["published_at"].weekday(),
        "characters": characters,
        "run_date": pendulum.now(),
    }


enriched_videos = []
for video in video_metadata:
    channel = channel_metadata.get(video["channel_id"])
    if not channel:
        continue

    enriched = build_enriched_video_record(video=video, channel=channel)
    enriched_videos.append(enriched)

result_df = pd.DataFrame(enriched_videos)
print(f"created dataframe of size {result_df.shape}")

if not os.path.exists(data_path):
    os.mkdir(data_path)
result_df.to_csv(f"{data_path}/enriched_videos.csv", index=False)
print(f"result saved at {data_path}/enriched_videos.csv")
