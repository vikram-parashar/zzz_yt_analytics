import duckdb

DB_PATH = "data/warehouse.db"


def init_tables_func():
    con = duckdb.connect(DB_PATH)

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
           language VARCHAR,
           country VARCHAR,
           ingested_date DATE
        )
    """)


if __name__ == "__main__":
    init_tables_func()
