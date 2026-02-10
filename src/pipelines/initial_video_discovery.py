import duckdb
from extract.discover_videos import discover_videos, get_topic_list
from load.load_discovered_videos import load_discovered_videos
from load_config import load_config
from transform.videos_to_df import prepare_video_df

config = load_config()


def initial_video_discovery_func():
    con = duckdb.connect(config["db"]["path"])

    topics = get_topic_list(con)

    for t in topics:
        items = discover_videos(t, 50)
        df = prepare_video_df(items)
        load_discovered_videos(con, df)

    con.close()


if __name__ == "__main__":
    initial_video_discovery_func()
