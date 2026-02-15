import duckdb
from extract.discover_videos import discover_videos
from load.load_discovered_videos import load_discovered_videos
import utils
import pendulum
from transform.videos_to_df import prepare_video_df

config = utils.load_config()


def daily_video_discovery_func():
    con = duckdb.connect(config["db"]["path"])

    topics = ["Zenless Zone Zero"]

    for t in topics:
        items = discover_videos(
            t, config["app"]["daily_video_ingestion"], pendulum.now().subtract(days=1)
        )
        df = prepare_video_df(items)
        load_discovered_videos(con, df)

    con.close()


if __name__ == "__main__":
    daily_video_discovery_func()
