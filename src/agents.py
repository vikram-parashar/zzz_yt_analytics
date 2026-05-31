import time
import json
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.utils import WORK_DIR, get_db, get_logger
from src.warehouse import get_agent_names

logger = get_logger(__name__)

WIKI_URL = "https://www.prydwen.gg/zenless/characters"
WIKI_BASE_URL = "https://www.prydwen.gg"

ALIASES_PATH = Path(WORK_DIR) / "data" / "aliases.json"

HTTP_MAX_RETRIES = 3
HTTP_RETRY_DELAY = 5


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


def _fetch_with_retry(
    session: requests.Session, url: str, max_retries: int = HTTP_MAX_RETRIES
) -> requests.Response:
    for attempt in range(max_retries + 1):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status and 400 <= status < 500 and status != 429:
                raise
            if attempt < max_retries:
                delay = HTTP_RETRY_DELAY * (2**attempt)
                logger.warning(
                    "HTTP %s on attempt %d/%d for %s — retrying in %ds",
                    status,
                    attempt + 1,
                    max_retries + 1,
                    url,
                    delay,
                )
                time.sleep(delay)
            else:
                raise
        except requests.exceptions.ConnectionError:
            if attempt < max_retries:
                delay = HTTP_RETRY_DELAY * (2**attempt)
                logger.warning(
                    "Connection error on attempt %d/%d for %s — retrying in %ds",
                    attempt + 1,
                    max_retries + 1,
                    url,
                    delay,
                )
                time.sleep(delay)
            else:
                raise


def scrape_wiki(session: requests.Session) -> str:
    """Scrape the character listing page. Returns raw HTML."""
    logger.info("scrape.start url=%s", WIKI_URL)
    start = time.perf_counter()

    resp = _fetch_with_retry(session, WIKI_URL)

    html = resp.text
    elapsed = time.perf_counter() - start

    logger.info(
        "scrape.fetched status=%s bytes=%s latency=%.2fs",
        resp.status_code,
        len(html),
        elapsed,
    )

    return html


def _parse_faction_from_detail(
    session: requests.Session, detail_url: str
) -> str | None:
    try:
        resp = _fetch_with_retry(session, detail_url)
        soup = BeautifulSoup(resp.text, "html.parser")

        intro = soup.find(class_="character-intro")
        if not intro:
            return None

        combined = intro.find(class_="combined")
        if combined:
            strong_tags = combined.find_all("strong")
            if strong_tags:
                return strong_tags[-1].get_text(strip=True)

        logger.debug("No faction <strong> found at %s", detail_url)
        return None

    except Exception:
        logger.exception("faction_scrape.failed url=%s", detail_url)
        return None


def parse_agents(soup: BeautifulSoup, session: requests.Session) -> list[dict]:
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

            link = card.find("a")
            data["href"] = (
                (WIKI_BASE_URL + str(link["href"]))
                if link and link.get("href")
                else None
            )

            img = card.find("img", alt=lambda x: x and x == data["name"])
            data["img"] = img["src"]

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

            data["faction"] = None
            if data["href"]:
                data["faction"] = _parse_faction_from_detail(session, data["href"])
                logger.debug(
                    "faction_scrape name=%s faction=%s",
                    data["name"],
                    data["faction"],
                )
                time.sleep(0.5)

            agents.append(data)

        except Exception:
            logger.exception("parse.card_failed index=%d", i)

    n_with_faction = sum(1 for a in agents if a.get("faction"))
    logger.info(
        "parse.done parsed=%d with_faction=%d success_rate=%.2f",
        len(agents),
        n_with_faction,
        len(agents) / len(cards) if cards else 0,
    )

    return agents


def upsert_agent(con, agents: list[dict]):
    logger.info("db.agent_upsert.start rows=%d", len(agents))

    df = pd.DataFrame(agents)
    if "href" in df.columns:
        df = df.drop(columns=["href"])

    con.register("agent_tmp", df)

    try:
        con.execute("""
            INSERT INTO dim_agent (
                name, img, rank, attribute, speciality, faction
            )
            SELECT name, img, rank, attribute, speciality, faction
            FROM agent_tmp
            ON CONFLICT(name)
            DO UPDATE SET
                img = excluded.img,
                rank = excluded.rank,
                attribute = excluded.attribute,
                speciality = excluded.speciality,
                faction = excluded.faction
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
        if agent_name not in aliases:
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

        soup = BeautifulSoup(html, "html.parser")
        agents = parse_agents(soup=soup, session=session)

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
