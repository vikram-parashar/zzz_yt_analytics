import time
import json
import re
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.utils import get_db, get_logger
from src.warehouse import get_agent_names, normalize_text

logger = get_logger(__name__)

WIKI_URL = "https://www.lootbar.com/blog/en/zenless-zone-zero-character-list.html"

ALIASES_PATH = Path("data/aliases.json")
FALLBACK_AGENTS_PATH = Path("data/agents_fallback.json")

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
    start = time.perf_counter()

    resp = _fetch_with_retry(session, WIKI_URL)
    elapsed = time.perf_counter() - start
    logger.info(
        "scrape.fetched status=%s bytes=%s latency=%.2fs",
        resp.status_code,
        len(resp.text),
        elapsed,
    )
    return resp.text


def _parse_rarity_from_img(td) -> str | None:
    img = td.find("img")
    if not img:
        return None
    alt = img.get("alt", "")
    if "Rank-S" in alt:
        return "S"
    if "Rank-A" in alt:
        return "A"
    return None


def _parse_rarity_from_text(text: str) -> str | None:
    m = re.search(r"\(([SA])-Rank\)", text)
    return m.group(1) if m else None


def _clean_name(raw: str) -> str:
    name = re.sub(r"\s*\([SA]-Rank\)", "", raw).strip()
    name = re.sub(r"\s*-\s*", " ", name)
    name = re.sub(r"\s+", " ", name)
    return name


def _parse_playable_tables(soup: BeautifulSoup) -> list[dict]:
    agents = []
    h2 = None
    for tag in soup.find_all("h2"):
        if "All Playable Characters" in tag.get_text():
            h2 = tag
            break

    if not h2:
        logger.warning("parse: 'All Playable Characters' h2 not found")
        return agents

    current_faction = None
    for sibling in h2.find_next_siblings():
        if sibling.name == "h2":
            break
        if sibling.name == "h3":
            current_faction = sibling.get_text(strip=True)
            faction_num_match = re.match(r"\d+\.\s*", current_faction)
            if faction_num_match:
                current_faction = current_faction[faction_num_match.end() :]
            continue
        if sibling.name == "table":
            rows = sibling.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue
                header_check = cells[0].get_text(strip=True)
                if header_check == "Name":
                    continue

                name_cell = cells[0]
                name_text = name_cell.get_text(strip=True)
                name = _clean_name(name_text)

                name_img = name_cell.find("img")
                img_src = name_img["src"] if name_img and name_img.get("src") else None

                rarity = _parse_rarity_from_img(cells[1])

                attr_cell = cells[2]
                attr_img = attr_cell.find("img")
                attribute = None
                if attr_img and attr_img.get("alt"):
                    attribute = re.sub(r"^Icon_", "", attr_img["alt"])
                if not attribute:
                    attribute = attr_cell.get_text(strip=True) or None

                spec_cell = cells[3]
                spec_img = spec_cell.find("img")
                speciality = None
                if spec_img and spec_img.get("alt"):
                    speciality = re.sub(r"^Icon_", spec_img["alt"])
                if not speciality:
                    speciality = spec_cell.get_text(strip=True) or None

                agents.append(
                    {
                        "name": name,
                        "img": img_src,
                        "rank": rarity,
                        "attribute": attribute,
                        "speciality": speciality,
                        "faction": current_faction,
                    }
                )

    return agents


def _parse_upcoming_tables(soup: BeautifulSoup, existing_names: set[str]) -> list[dict]:
    agents = []
    h2 = None
    for tag in soup.find_all("h2"):
        if "All Playable Characters" in tag.get_text():
            h2 = tag
            break

    if not h2:
        return agents

    _header_patterns = {"Agents", "Version", "Upcoming"}

    for sibling in h2.find_previous_siblings():
        if sibling.name == "table":
            rows = sibling.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                for cell in cells:
                    text = cell.get_text(strip=True)
                    if not text:
                        continue

                    name = _clean_name(text)
                    if any(name.startswith(p) for p in _header_patterns):
                        continue

                    if name in existing_names:
                        continue

                    rarity = _parse_rarity_from_text(text)

                    img_tag = cell.find("img")
                    img_src = img_tag["src"] if img_tag and img_tag.get("src") else None

                    agents.append(
                        {
                            "name": name,
                            "img": img_src,
                            "rank": rarity,
                            "attribute": None,
                            "speciality": None,
                            "faction": None,
                        }
                    )
                    existing_names.add(name)

    return agents


def parse_agents(soup: BeautifulSoup) -> list[dict]:
    playable = _parse_playable_tables(soup)

    existing_names = {a["name"] for a in playable}
    upcoming = _parse_upcoming_tables(soup, existing_names)

    agents = playable + upcoming
    n_with_faction = sum(1 for a in agents if a.get("faction"))
    logger.info(
        "parse.done total=%d with_faction=%d",
        len(agents),
        n_with_faction,
    )

    return agents


def upsert_agent(con, agents: list[dict]):
    df = pd.DataFrame(agents)
    for col in ["href"]:
        if col in df.columns:
            df = df.drop(columns=[col])

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


def upsert_aliases(con, alias_map: dict):
    agent_names = get_agent_names(con)

    aliases_list = []
    for agent_name in agent_names:
        raw_aliases = alias_map.get(agent_name) or []
        if not raw_aliases:
            logger.warning(f"{agent_name} does not have a alias")
            raw_aliases.append(agent_name)
        aliases_list.extend(
            [
                {"name": agent_name, "alias": normalize_text(alias)}
                for alias in raw_aliases
            ]
        )

    logger.info("db.alias_upsert.mapped rows=%d", len(aliases_list))

    alias_df = pd.DataFrame(aliases_list)
    con.register("alias_tmp", alias_df)

    try:
        con.execute("""
            truncate bridge_agent_alias
        """)
        con.execute("""
            INSERT INTO bridge_agent_alias
            SELECT *
            FROM alias_tmp
        """)
    except Exception:
        logger.exception("db.alias_upsert.failed")
        raise


def scrape_and_load():
    try:
        with open(ALIASES_PATH) as f:
            alias_map = json.load(f)

        session = _make_session()

        html = scrape_wiki(session)

        soup = BeautifulSoup(html, "html.parser")
        agents = parse_agents(soup=soup)

        if not agents:
            logger.warning("agents.no_agents_parsed — using fallback data")
            return

        logger.info("agents.loaded count=%d", len(agents))

        with get_db() as con:
            upsert_agent(con, agents)
            upsert_aliases(con, alias_map)

        logger.info("agents.success")

    except Exception:
        logger.exception("agents.failed")
        raise
