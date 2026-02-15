import json
from pathlib import Path
import duckdb
import pandas as pd
import utils


logger = utils.get_logger(__name__)
config = utils.load_config()
DB_PATH = config["db"]["path"]
STAGE_DIR = Path("data/stage")
JSON_PATH = Path("src/data/aliases.json")


def refresh_tables(con: duckdb.DuckDBPyConnection):
    logger.info("Refreshing dimension tables")
    con.execute("DELETE FROM dim_agent")
    con.execute("DELETE FROM bridge_agent_alias")


def insert_agents(con):
    stage_path = STAGE_DIR / "agents.csv"
    logger.info(f"Loading staged agents from {stage_path}")

    agents_df = duckdb.read_csv(stage_path).to_df()
    con.register("agents_temp", agents_df)

    con.execute("""
        INSERT INTO dim_agent (name, rank, attribute, speciality, faction, release_date, release_version)
        SELECT name, rank, attribute, speciality, faction, release_date, release_version
        FROM agents_temp
    """)

    inserted_agents = con.execute("SELECT COUNT(*) FROM dim_agent").fetchone()[0]
    logger.info(f"{inserted_agents} agents inserted into dim_agent")


def insert_aliases(con):
    logger.info("Loading aliases JSON")

    with open(JSON_PATH) as f:
        alias_json = json.load(f)

    agent_names = con.execute("SELECT name FROM dim_agent").df()["name"]
    alias_rows = []

    for agent_name in agent_names:
        if agent_name in alias_json:
            for alias in alias_json[agent_name]:
                alias_rows.append({"name": agent_name, "alias": alias})
        else:
            logger.warning(f"Missing aliases for agent: {agent_name}")

    alias_df = pd.DataFrame(alias_rows)

    if not alias_df.empty:
        con.register("alias_temp", alias_df)

        con.execute("""
            INSERT INTO bridge_agent_alias
            SELECT * FROM alias_temp
        """)

        inserted_aliases = con.execute(
            "SELECT COUNT(*) FROM bridge_agent_alias"
        ).fetchone()[0]

        logger.info(f"{inserted_aliases} rows inserted into bridge_agent_alias")
    else:
        logger.warning("No alias rows to insert")


def load_agents_func():
    logger.info("Starting agent load pipeline")
    con = duckdb.connect(DB_PATH)

    try:
        refresh_tables(con)
        insert_agents(con)
        insert_aliases(con)
        logger.info("Agent load pipeline completed successfully")
    finally:
        con.close()
        logger.info("DuckDB connection closed")


if __name__ == "__main__":
    load_agents_func()
