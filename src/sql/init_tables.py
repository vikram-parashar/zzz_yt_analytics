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


if __name__ == "__main__":
    init_tables_func()
