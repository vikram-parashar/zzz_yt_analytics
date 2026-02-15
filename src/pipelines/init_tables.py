import duckdb
import utils

config = utils.load_config()


def init_tables_func():
    con = duckdb.connect(config["db"]["path"])

    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_agent (
        name VARCHAR UNIQUE PRIMARY KEY,
        rank VARCHAR,
        attribute VARCHAR,
        speciality VARCHAR,
        faction VARCHAR,
        release_date DATE,
        release_version VARCHAR
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS bridge_agent_alias (
        name VARCHAR,
        alias VARCHAR,
        PRIMARY KEY (name, alias)
    )
    """)

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
           country VARCHAR,
           ingested_date DATE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_video_daily (
            video_id VARCHAR,
            snapshot_date DATE,
            view_count BIGINT,
            like_count BIGINT,
            comment_count BIGINT,
            dislike_count BIGINT,
            favorite_count BIGINT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (video_id, snapshot_date),
            FOREIGN KEY (video_id) REFERENCES dim_video(video_id)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_channel_daily (
            channel_id VARCHAR,
            snapshot_date DATE,
            subscriber_count BIGINT,
            view_count BIGINT,
            video_count INTEGER,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (channel_id, snapshot_date),
            FOREIGN KEY (channel_id) REFERENCES dim_channel(channel_id)
        )
    """)


if __name__ == "__main__":
    init_tables_func()
