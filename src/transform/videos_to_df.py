import pandas as pd
import pendulum
import re
import utils

logger = utils.get_logger(__name__)
ISO_8601_DURATION_REGEX = re.compile(
    r"P"  # starts with 'P'
    r"(?:(\d+)D)?"  # days
    r"(?:T"  # time component
    r"(?:(\d+)H)?"  # hours
    r"(?:(\d+)M)?"  # minutes
    r"(?:(\d+)S)?"  # seconds
    r")?"  # time component optional
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


def parse_iso8601_duration(duration: str) -> int | None:
    try:
        match = ISO_8601_DURATION_REGEX.fullmatch(duration)
        if not match:
            raise ValueError

        days, hours, minutes, seconds = match.groups(default="0")
        return int(days) * 86400 + int(hours) * 3600 + int(minutes) * 60 + int(seconds)
    except Exception:
        logger.warning(f"Failed to parse duration: {duration}")
        return None


def video_metadata_df(items) -> pd.DataFrame:
    res = []
    for item in items:
        try:
            res.append(
                {
                    "video_id": item["id"],
                    "tags": item["snippet"].get("tags", []),
                    "duration_seconds": parse_iso8601_duration(
                        item["contentDetails"]["duration"]
                    ),
                    "thumbnail": item["snippet"]["thumbnails"]
                    .get("default", {})
                    .get("url", ""),
                    "view_count": item["statistics"]["viewCount"],
                    "like_count": item["statistics"]["likeCount"],
                    "dislike_count": item["statistics"]["dislikeCount"],
                    "favorite_count": item["statistics"]["favoriteCount"],
                    "comment_count": item["statistics"]["commentCount"],
                }
            )
        except KeyError:
            continue
    return pd.DataFrame(res)
