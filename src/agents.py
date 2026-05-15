import time
import hashlib
import json
from pathlib import Path

import pandas as pd
import pendulum
import requests
from bs4 import BeautifulSoup

from src.utils import WORK_DIR, get_db, get_logger
from src.warehouse import get_agent_names

logger = get_logger(__name__)

WIKI_URL = "https://www.prydwen.gg/zenless/characters"
WIKI_BASE_URL = "https://www.prydwen.gg"

RAW_DIR = Path(WORK_DIR) / "data" / "raw"
ALIASES_PATH = Path(WORK_DIR) / "data" / "aliases.json"


def _make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        }
    )
    return session


def scrape_wiki(session: requests.Session) -> str:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("scrape.start url=%s", WIKI_URL)
    start = time.perf_counter()

    try:
        resp = session.get(WIKI_URL, timeout=30)
        resp.raise_for_status()
    except Exception:
        logger.exception("scrape.http_failed url=%s", WIKI_URL)
        raise

    html = resp.text
    elapsed = time.perf_counter() - start

    logger.info(
        "scrape.fetched status=%s bytes=%s latency=%.2fs",
        resp.status_code,
        len(html),
        elapsed,
    )

    new_hash = hashlib.md5(html.encode()).hexdigest()
    latest_path = RAW_DIR / "agents_latest.html"

    old_hash = (
        hashlib.md5(latest_path.read_bytes()).hexdigest()
        if latest_path.exists()
        else None
    )

    if new_hash == old_hash:
        logger.info("scrape.no_change hash=%s", new_hash)
        return html

    run_date = pendulum.now("UTC").to_datetime_string()

    latest_path.write_text(html, encoding="utf-8")
    snapshot_path = RAW_DIR / f"agents_{run_date}.html"
    snapshot_path.write_text(html, encoding="utf-8")

    logger.info(
        "scrape.saved_snapshot hash=%s snapshot=%s latest=%s",
        new_hash,
        snapshot_path.name,
        latest_path.name,
    )

    return html


def parse_agents(soup: BeautifulSoup) -> list[dict]:
    logger.info("parse.start")

    cards = soup.find_all(class_="avatar-card")
    logger.info("parse.cards_found count=%d", len(cards))

    agents = []

    for i, card in enumerate(cards):
        try:
            data = {}

            data["rank"] = "S" if card.find(class_="rarity-S") else None
            if not data["rank"]:
                data["rank"] = "A" if card.find(class_="rarity-A") else None

            emp_name = card.find(class_="emp-name")
            data["name"] = emp_name.get_text().strip() if emp_name else None

            img = card.find("img", alt=lambda x: x and x == data["name"])
            data["img"] = (WIKI_BASE_URL + str(img["data-src"])) if img else None

            element_div = card.find(class_="element")
            element = (
                element_div.find("img", alt=lambda x: x and x.strip())
                if element_div
                else None
            )
            data["attribute"] = element["alt"] if element else None

            class_div = card.find(class_="class")
            clas = (
                class_div.find("img", alt=lambda x: x and x.strip())
                if class_div
                else None
            )
            data["speciality"] = clas["alt"] if clas else None

            agents.append(data)

        except Exception:
            logger.exception("parse.card_failed index=%d", i)

    logger.info(
        "parse.done parsed=%d success_rate=%.2f",
        len(agents),
        len(agents) / len(cards) if cards else 0,
    )

    return agents


def upsert_agent(con, agents: list[dict]):
    logger.info("db.agent_upsert.start rows=%d", len(agents))

    df = pd.DataFrame(agents)
    con.register("agent_tmp", df)

    try:
        con.execute("""
            INSERT INTO dim_agent (
                name, img, rank, attribute, speciality
            )
            SELECT name, img, rank, attribute, speciality
            FROM agent_tmp
            ON CONFLICT(name)
            DO UPDATE SET
                img = excluded.img,
                rank = excluded.rank,
                attribute = excluded.attribute,
                speciality = excluded.speciality
        """)
    except Exception:
        logger.exception("db.agent_upsert.failed")
        raise

    logger.info("db.agent_upsert.done")


def upsert_aliases(con, alias_map: dict):
    logger.info("db.alias_upsert.start")

    agent_names = get_agent_names(con)

    aliases_list = []
    for agent_name in agent_names:
        aliases = alias_map.get(agent_name) or []
        aliases.append(agent_name)
        aliases_list.extend([{"name": agent_name, "alias": alias} for alias in aliases])

    logger.info("db.alias_upsert.mapped rows=%d", len(aliases_list))

    alias_df = pd.DataFrame(aliases_list)
    con.register("alias_tmp", alias_df)

    try:
        con.execute("""
            INSERT INTO bridge_agent_alias
            SELECT *
            FROM alias_tmp
            ON CONFLICT DO NOTHING
        """)
    except Exception:
        logger.exception("db.alias_upsert.failed")
        raise

    logger.info("db.alias_upsert.done")


def scrape_and_load():
    logger.info("agents.start")

    try:
        with open(ALIASES_PATH) as f:
            alias_map = json.load(f)

        session = _make_session()

        html = scrape_wiki(session)

        # with open("data/raw/agents_latest.html", "r", encoding="utf-8") as f:
        #     html = f.read()

        soup = BeautifulSoup(html, "html.parser")
        agents = parse_agents(soup=soup)

        if not agents:
            logger.warning("agents.no_agents_parsed")
            return

        logger.info("agents.loaded_agents count=%d", len(agents))

        with get_db() as con:
            upsert_agent(con, agents)
            upsert_aliases(con, alias_map)

        logger.info("agents.success")

    except Exception:
        logger.exception("agents.failed")
        raise

    finally:
        logger.info("agents.end")
