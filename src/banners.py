import re
import time
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.utils import get_db, get_logger

logger = get_logger(__name__)

BANNER_URL = "https://gamerant.com/zenless-zone-zero-zzz-current-next-past-banner-history-schedule/"

HTTP_MAX_RETRIES = 3
HTTP_RETRY_DELAY = 5

_VERSION_DATE_RE = re.compile(
    r"Version\s+([\d.]+)\s*:\s*"
    r"([A-Z][a-z]+ \d{1,2},? \d{4})\s*[–\-]\s*"
    r"([A-Z][a-z]+ \d{1,2},? \d{4})"
    r"(?:\s*\((Phase\s+\d+)\))?",
    re.I,
)
_VERSION_RE = re.compile(r"Version\s+([\d.]+)")


def _make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            ),
            "Connection": "keep-alive",
        }
    )
    return session


def _fetch_with_retry(
    session: requests.Session,
    url: str,
    max_retries: int = HTTP_MAX_RETRIES,
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


_NAME_SUFFIXES = (
    " First Rerun",
    " Rerun Banner",
    " Debut Banner",
    " Banner",
    " Rerun",
    " Debut",
)

_NAME_MAP = {"Jane Doe": "Jane"}


def _clean_agent_name(raw: str) -> str:
    name = raw.strip()
    for suffix in _NAME_SUFFIXES:
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    name = re.sub(r"\s*-\s+", " ", name).strip()
    return _NAME_MAP.get(name, name)


_SKIP_KEYWORDS = ("bangboo", "standard", "engine", "select")


def _parse_table(table) -> list[dict]:
    banners = []
    current_version = None
    current_start = None
    current_end = None

    for row in table.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if not cells:
            continue

        first = cells[0]

        if first.name == "th" and first.get("colspan"):
            (current_version, current_start, current_end) = _parse_version_header(
                first.get_text(" ", strip=True)
            )
            continue

        if first.name == "td":
            continue

        if first.name == "th" and not first.get("colspan") and current_version:
            raw_name = first.get_text(" ", strip=True)

            if any(kw in raw_name.lower() for kw in _SKIP_KEYWORDS):
                continue
            agent_name = _clean_agent_name(raw_name)

            if current_version and agent_name and current_start and current_end:
                banners.append(
                    {
                        "version": current_version,
                        "agent_name": agent_name,
                        "banner_start": current_start,
                        "banner_end": current_end,
                    }
                )

    return banners


def _parse_version_header(text):
    m = _VERSION_DATE_RE.search(text)
    if m:
        version = m.group(1)

        try:
            start_date = datetime.strptime(m.group(2), "%B %d, %Y").date().isoformat()

            end_date = datetime.strptime(m.group(3), "%B %d, %Y").date().isoformat()

            return version, start_date, end_date

        except Exception:
            return version, None, None

    m = _VERSION_RE.search(text)
    if m:
        return m.group(1), None, None

    return None, None, None


def _history_tables(soup):
    history_h2 = None

    for h2 in soup.find_all("h2"):
        text = h2.get_text(" ", strip=True).lower()

        if "zzz banner history" in text:
            history_h2 = h2
            break

    if not history_h2:
        return []

    tables = []

    node = history_h2.find_next_sibling()

    while node:
        if getattr(node, "name", None) == "h2":
            break

        tables.extend(node.find_all("table"))

        node = node.find_next_sibling()

    return tables


def _parse_current_banner_table(table) -> list[dict]:
    rows = table.find_all("tr")

    if len(rows) < 2:
        return []

    header_text = rows[0].get_text(" ", strip=True)

    version, start, end = _parse_version_header(header_text)

    if not version:
        return []

    name_cells = rows[1].find_all("th")

    banners = []

    for cell in name_cells:
        raw_name = cell.get_text(" ", strip=True)

        if not raw_name:
            continue

        agent_name = _clean_agent_name(raw_name)

        if version and agent_name and start and end:
            banners.append(
                {
                    "version": version,
                    "agent_name": agent_name,
                    "banner_start": start,
                    "banner_end": end,
                }
            )

    return banners


def parse_banners(soup: BeautifulSoup) -> list[dict]:
    banners = []
    seen = set()

    first_table = soup.find("table")

    if first_table:
        for b in _parse_current_banner_table(first_table):
            key = (b["version"], b["agent_name"])

            if key not in seen:
                seen.add(key)
                banners.append(b)

    for table in _history_tables(soup):
        for b in _parse_table(table):
            key = (b["version"], b["agent_name"])

            if key not in seen:
                seen.add(key)
                banners.append(b)

    return banners


def upsert_banners(con, banners: list[dict]):
    if not banners:
        logger.info("banners.upsert: No banners to write")
        return
    df = pd.DataFrame(banners)
    con.register("banner_tmp", df)
    try:
        con.execute(
            """
            INSERT INTO dim_patch (version, agent_name, banner_start, banner_end)
            SELECT version, agent_name, banner_start, banner_end
            FROM banner_tmp
            ON CONFLICT (version, agent_name)
            DO UPDATE SET
                banner_start = excluded.banner_start,
                banner_end   = excluded.banner_end
            """
        )
    except Exception:
        logger.exception("banners.upsert.failed")
        raise
    logger.info("banners.upsert.done rows=%d", len(banners))


def scrape_banners():
    try:
        session = _make_session()
        start = time.perf_counter()
        resp = _fetch_with_retry(session, BANNER_URL)
        html = resp.text
        elapsed = time.perf_counter() - start
        logger.info(
            "banners.fetch.done status=%s bytes=%s latency=%.2fs",
            resp.status_code,
            len(html),
            elapsed,
        )
        soup = BeautifulSoup(html, "html.parser")
        banners = parse_banners(soup)
        if not banners:
            logger.warning("banners.no_banners_parsed")
            return
        with get_db() as con:
            upsert_banners(con, banners)
        logger.info("banners.success")
    except Exception:
        logger.exception("banners.failed")
        raise
