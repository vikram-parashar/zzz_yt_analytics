from extract.discover_videos import discover_videos, get_topic_list
from load.load_discovered_videos import connect_db, load_discovered_videos
from transform.videos_to_df import prepare_video_df


def initial_video_discovery_func():
    con = connect_db("data/warehouse.db")

    topics = get_topic_list(con)

    for t in topics:
        items = discover_videos(t, 50)
        df = prepare_video_df(items)
        load_discovered_videos(con, df)

    con.close()


if __name__ == "__main__":
    initial_video_discovery_func()
