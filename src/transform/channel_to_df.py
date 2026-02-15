import pandas as pd


def channel_metadata_df(items) -> pd.DataFrame:
    res = []
    for item in items:
        try:
            res.append(
                {
                    "channel_id": item["id"],
                    "thumbnail": item["snippet"]["thumbnails"]
                    .get("default", {})
                    .get("url", ""),
                    "country": item["snippet"].get("country", None),
                    "view_count": item["statistics"]["viewCount"],
                    "subscriber_count": item["statistics"]["subscriberCount"],
                    "video_count": item["statistics"]["videoCount"],
                }
            )
        except KeyError:
            print("key error in channel item")
            continue
    return pd.DataFrame(res)
