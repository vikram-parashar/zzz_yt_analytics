import logging
import duckdb
import pandas as pd
from aliases import alias_dict


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

"""
Paths
"""
DB_PATH = "data/warehouse.db"
agent_dir = "data/agents"
cleaned_path = f"{agent_dir}/clean_agent.csv"

try:
    logger.info("agent/load_to_db.py started")

    """
    DB Connection
    """
    logger.info(f"Connecting to DuckDB at {DB_PATH}")
    con = duckdb.connect(DB_PATH)

    """
    Create Tables
    """
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

    """
    Refresh Tables
    """
    con.execute("DELETE FROM dim_agent")
    con.execute("DELETE FROM bridge_agent_alias")

    """
    READ AND PUSH TO DB
    """
    agents_df = duckdb.read_csv(cleaned_path).to_df()
    con.register("agents_temp", agents_df)
    con.execute("""
    INSERT INTO dim_agent (name, rank, attribute, speciality, faction, release_date, release_version)
    SELECT name, rank, attribute, speciality, faction, release_date, release_version
    FROM agents_temp
    """)

    inserted_agents = con.execute("SELECT COUNT(*) FROM dim_agent").fetchone()[0]
    logger.info(f"dim_agent rows inserted: {inserted_agents}")

    """
    Build Alias Bridge
    """
    agent_names = con.execute("SELECT name FROM dim_agent").df()["name"]
    alias_rows = []

    for agent_name in agent_names:
        if agent_name in alias_dict:
            for alias in alias_dict[agent_name]:
                alias_rows.append({"name": agent_name, "alias": alias})
        else:
            logger.warning(f"Missing aliases for agent: {agent_name}")

    alias_df = pd.DataFrame(alias_rows)
    logger.info(f"Alias rows prepared: {len(alias_df)}")

    if not alias_df.empty:
        con.register("alias_temp", alias_df)

        con.execute("""
        INSERT INTO bridge_agent_alias
        SELECT * FROM alias_temp
        """)

        inserted_aliases = con.execute(
            "SELECT COUNT(*) FROM bridge_agent_alias"
        ).fetchone()[0]
        logger.info(f"bridge_agent_alias rows inserted: {inserted_aliases}")
    else:
        logger.warning("No alias rows to insert")

    con.close()
    logger.info("DB load job completed successfully")

except Exception as e:
    logger.exception("DB load job failed due to unexpected error")
    raise
