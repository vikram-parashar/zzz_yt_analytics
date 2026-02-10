import pandas as pd
import pendulum
import re

ISO_8601_DURATION_REGEX = re.compile(
    r"P"
    r"(?:(\d+)D)?"
    r"(?:T"
    r"(?:(\d+)H)?"
    r"(?:(\d+)M)?"
    r"(?:(\d+)S)?"
    r")?"
)


def prepare_video_df(items) -> pd.DataFrame:
    records = []

    for item in items:
        try:
            records.append(
                {
                    "video_id": item["id"]["videoId"],
                    "title": item["snippet"]["title"],
                    "description": item["snippet"]["description"],
                    "channel_id": item["snippet"]["channelId"],
                    "channel_title": item["snippet"]["channelTitle"],
                    "published_at": item["snippet"]["publishedAt"],
                    "discovered_at": pendulum.now(),
                }
            )
        except KeyError:
            continue

    df = pd.DataFrame(records)
    df["published_at"] = pd.to_datetime(df["published_at"])

    return df
