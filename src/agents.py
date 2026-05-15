import json
import hashlib
import os
from pathlib import Path

import pandas as pd
import requests
import pendulum
from bs4 import BeautifulSoup

from src.utils import get_logger, load_config, get_db

logger = get_logger(__name__)
WORK_DIR = os.getenv("WORK_DIR", "./")
WIKI_URL = "https://zenless-zone-zero.fandom.com/wiki/Agent"
RAW_DIR = Path(WORK_DIR + "/data/raw")
STAGE_DIR = Path(WORK_DIR + "/data/stage")
ALIASES_PATH = Path(WORK_DIR + "/data/aliases.json")


def _make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        }
    )
    return session


def scrape_wiki() -> str:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Fetching agents page from wiki")
    html = _make_session().get(WIKI_URL).text

    new_hash = hashlib.md5(html.encode()).hexdigest()
    latest_path = RAW_DIR / "agents_latest.html"
    old_hash = (
        hashlib.md5(latest_path.read_bytes()).hexdigest()
        if latest_path.exists()
        else None
    )

    if new_hash == old_hash:
        logger.info("No change detected in agents page — skipping save")
    else:
        run_date = pendulum.now("UTC").to_datetime_string()
        latest_path.write_text(html, encoding="utf-8")
        (RAW_DIR / f"agents_{run_date}.html").write_text(html, encoding="utf-8")
        logger.info(f"Saved snapshot | hash={new_hash}")

    return html


def _extract_agent_row(cells, is_upcoming: bool = False) -> dict | None:
    try:
        icon = cells[0].img.get("data-src") or cells[0].img.get("src")
        name = cells[1].a.text
        attribute = cells[3].a["title"]
        speciality = cells[4].a["title"]
        faction = cells[6].a["title"]

        if is_upcoming:
            return {
                "icon": icon,
                "name": name,
                "rank": None,
                "attribute": attribute,
                "speciality": speciality,
                "faction": faction,
                "release_date": None,
                "release_version": None,
            }

        rank = cells[2].span["title"].split(" ")[1].strip()
        release_strings = list(cells[7].stripped_strings)
        release_date = release_strings[0]
        release_version = release_strings[3].split(" ")[1]

        return {
            "icon": icon,
            "name": name,
            "rank": rank,
            "attribute": attribute,
            "speciality": speciality,
            "faction": faction,
            "release_date": release_date,
            "release_version": release_version,
        }
    except Exception:
        logger.exception("Failed to parse agent row — skipping")
        return None


def parse_agents(html: str | None = None) -> pd.DataFrame:
    if html is None:
        html = (RAW_DIR / "agents_latest.html").read_text()

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")

    # Playable agents (2nd table)
    agents = []
    logger.info(soup)
    try:
        for row in tables[1].tbody.find_all("tr")[1:]:
            logger.info(row.find_all("td"))
            fields = _extract_agent_row(row.find_all("td"))
            if fields:
                agents.append(fields)
    except (AttributeError, IndexError):
        logger.error("Could not find playable agents table")

    # Upcoming agents (3rd table)
    try:
        for row in tables[2].tbody.find_all("tr")[1:]:
            fields = _extract_agent_row(row.find_all("td"), is_upcoming=True)
            if fields:
                agents.append(fields)
    except (AttributeError, IndexError):
        logger.warning("Could not find upcoming agents table")

    df = pd.DataFrame(agents)
    if df.empty:
        logger.warning("No agents parsed")
        return df

    # Convert release_date strings → actual dates
    mask = df["release_date"].notna()
    df.loc[mask, "release_date"] = df.loc[mask, "release_date"].apply(
        lambda x: pendulum.from_format(x, "MMMM DD, YYYY").date()
    )

    # Known data fix
    df.loc[df["name"] == "Jane Doe", "faction"] = (
        "Criminal Investigation Special Response Team"
    )

    logger.info(f"Parsed {len(df)} agents total")
    return df


def _build_alias_rows(agent_names: pd.Series, alias_map: dict) -> pd.DataFrame:
    """Build a DataFrame of (name, alias) pairs from the alias JSON."""
    rows = []
    for name in agent_names:
        if name in alias_map:
            for alias in alias_map[name]:
                rows.append({"name": name, "alias": alias})
        else:
            logger.warning(f"No aliases defined for: {name}")
    return pd.DataFrame(rows)


def load_agents(df: pd.DataFrame):
    """Replace dim_agent and bridge_agent_alias with fresh data."""
    config = load_config()

    STAGE_DIR.mkdir(parents=True, exist_ok=True)
    stage_path = STAGE_DIR / "agents.csv"
    df.to_csv(stage_path, index=False)
    logger.info(f"Staged agents CSV → {stage_path}")

    with get_db(config) as con:
        # Refresh dimension tables
        con.execute("DELETE FROM dim_agent")
        con.execute("DELETE FROM bridge_agent_alias")

        # Insert agents
        con.register(
            "agents_tmp",
            df[
                [
                    "name",
                    "rank",
                    "attribute",
                    "speciality",
                    "faction",
                    "release_date",
                    "release_version",
                ]
            ],
        )
        con.execute("""
            INSERT INTO dim_agent (name, rank, attribute, speciality, faction, release_date, release_version)
            SELECT name, rank, attribute, speciality, faction, release_date, release_version
            FROM agents_tmp
        """)
        count = con.execute("SELECT COUNT(*) FROM dim_agent").fetchone()[0]
        logger.info(f"Loaded {count} agents into dim_agent")

        # Insert aliases
        with open(ALIASES_PATH) as f:
            alias_map = json.load(f)

        agent_names = con.execute("SELECT name FROM dim_agent").df()["name"]
        alias_df = _build_alias_rows(agent_names, alias_map)

        if not alias_df.empty:
            con.register("alias_tmp", alias_df)
            con.execute("INSERT INTO bridge_agent_alias SELECT * FROM alias_tmp")
            logger.info(f"Loaded {len(alias_df)} alias rows into bridge_agent_alias")


def scrape_and_load():
    html = scrape_wiki()
    df = parse_agents(html)
    if not df.empty:
        load_agents(df)
    logger.info("Agent pipeline complete")
