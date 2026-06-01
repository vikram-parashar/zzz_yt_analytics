import asyncio
import re
import time

import pandas as pd
import pendulum
import requests
from bs4 import BeautifulSoup

from src.utils import get_db, get_logger
from src.warehouse import ensure_dim_patch_schema, TABLE_DDL

logger = get_logger(__name__)

BANNER_URL = "https://game8.co/games/Zenless-Zone-Zero/archives/435687"

HTTP_MAX_RETRIES = 3
HTTP_RETRY_DELAY = 5


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


def _is_waf_challenge(html: str) -> bool:
    markers = ("awsWafCookieDomainList", "AwsWafIntegration", "challenge.js")
    return any(m in html for m in markers)


async def _fetch_with_playwright(url: str) -> str:
    from playwright.async_api import async_playwright

    logger.info("banners.fetch.playwright.start url=%s", url)
    start = time.perf_counter()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle", timeout=60_000)
        html = await page.content()
        await browser.close()

    elapsed = time.perf_counter() - start
    logger.info(
        "banners.fetch.playwright.done bytes=%s latency=%.2fs",
        len(html),
        elapsed,
    )
    return html


def fetch_banner_page() -> str:
    try:
        session = _make_session()
        logger.info("banners.fetch.requests.start url=%s", BANNER_URL)
        start = time.perf_counter()

        resp = _fetch_with_retry(session, BANNER_URL)
        html = resp.text

        elapsed = time.perf_counter() - start
        logger.info(
            "banners.fetch.requests.done status=%s bytes=%s latency=%.2fs",
            resp.status_code,
            len(html),
            elapsed,
        )

        if not _is_waf_challenge(html):
            return html

        logger.info("banners.fetch: requests got WAF challenge — trying Playwright")
    except Exception as exc:
        logger.info("banners.fetch: requests failed (%s) — trying Playwright", exc)

    try:
        html = asyncio.run(_fetch_with_playwright(BANNER_URL))
        if _is_waf_challenge(html):
            raise RuntimeError("Playwright also received WAF challenge page")
        return html
    except ImportError:
        logger.error(
            "banners.fetch: Playwright is not installed. "
            "Install it with: pip install playwright && playwright install chromium"
        )
        raise
    except Exception:
        logger.exception("banners.fetch: Playwright fetch failed")
        raise


_DATE_RE = re.compile(r"(\d{1,2}/\d{1,2}(?:/\d{2,4})?)\s*-\s*(\d{1,2}/\d{1,2}/\d{2,4})")
_PHASE_RE = re.compile(r"\(Phase\s*(\d+)\)", re.IGNORECASE)


def _parse_date_range(date_str: str) -> tuple[str, str] | None:
    cleaned = _PHASE_RE.sub("", date_str).strip()
    match = _DATE_RE.search(cleaned)
    if not match:
        return None

    start_raw, end_raw = match.groups()

    end_parts = end_raw.split("/")
    end_month, end_day, end_year = (
        int(end_parts[0]),
        int(end_parts[1]),
        int(end_parts[2]),
    )
    if end_year < 100:
        end_year += 2000

    start_parts = start_raw.split("/")
    start_month, start_day = int(start_parts[0]), int(start_parts[1])

    if len(start_parts) == 3:
        start_year = int(start_parts[2])
        if start_year < 100:
            start_year += 2000
    else:
        start_year = end_year
        if start_month > end_month:
            start_year -= 1

    start_date = f"{start_year:04d}-{start_month:02d}-{start_day:02d}"
    end_date = f"{end_year:04d}-{end_month:02d}-{end_day:02d}"

    return start_date, end_date


def _clean_agent_name(raw_name: str) -> str:
    """Extract the agent name from a banner label.

    Examples::

        "Promeia Banner"       -> "Promeia"
        "Lucia Rerun Banner"   -> "Lucia"
        "Astra Yao Rerun Banner" -> "Astra Yao"
        "Soldier 0 - Anby Banner" -> "Soldier 0 - Anby"
    """
    name = raw_name.strip()
    for suffix in (
        " Rerun Banner",
        " Debut Banner",
        " Banner",
        " Rerun",
        " Debut",
    ):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    return name.strip()


def parse_banner_current(soup: BeautifulSoup) -> list[dict]:
    h3 = soup.find(
        lambda tag: tag.name == "h3" and "Banner Schedule" in tag.get_text(strip=True)
    )

    if not h3:
        logger.warning("banner_current: h3 not found")
        return []

    ul = h3.find_next("ul")
    if not ul:
        logger.warning("banner_current: ul not found")
        return []

    banners = []

    for li in ul.find_all("li", recursive=False):
        text = li.get_text(" ", strip=True)

        lower = text.lower()
        if any(keyword in lower for keyword in ("bangboo", "standard", "engine")):
            continue

        link = li.find("a")
        if not link:
            continue

        raw_name = link.get_text(strip=True)
        agent_name = _clean_agent_name(raw_name)

        if agent_name == "Orphie":
            agent_name = "Orphie & Magus"
        elif agent_name == "Orphie and Magus":
            agent_name = "Orphie & Magus"
        elif agent_name == "Soldier 0 - Anby":
            agent_name = "Anby: Soldier 0"

        if "=" not in text:
            continue

        date_part = text.split("=", 1)[1].strip()

        if date_part.lower() == "permanent":
            continue

        try:
            start_str, end_str = [x.strip() for x in date_part.split("-", 1)]

            start_date = pendulum.from_format(
                start_str,
                "MMMM D, YYYY",
            ).to_date_string()

            end_date = pendulum.from_format(
                end_str,
                "MMMM D, YYYY",
            ).to_date_string()
            version_link = soup.find("a", string=lambda s: s and "Version" in s)

            version = None
            if version_link:
                version = version_link.get_text(strip=True).split()[1]

        except Exception:
            logger.warning(
                "banner_current: failed to parse date for %s: %s",
                agent_name,
                date_part,
            )
            continue

        banners.append(
            {
                "version": version,
                "agent_name": agent_name,
                "banner_start": start_date,
                "banner_end": end_date,
            }
        )

    logger.info(
        "banner_current.parse.done parsed=%d",
        len(banners),
    )

    return banners


def parse_banners(soup: BeautifulSoup) -> list[dict]:
    target_th = soup.find(
        "th", string=lambda t: t and "All Agent and W-Engine Banners" in t
    )
    if not target_th:
        logger.warning("banners.parse: Could not find banner table header")
        return []

    table = target_th.find_parent("table")
    if not table:
        logger.warning("banners.parse: Could not find parent table")
        return []

    tbody = table.find("tbody") or table

    rows = tbody.find_all("tr")
    if not rows:
        logger.warning("banners.parse: No rows found in table body")
        return []

    banners: list[dict] = []
    current_version: str | None = None
    version_rowspan_remaining: int = 0

    for row_idx, row in enumerate(rows):
        cells = row.find_all(["th", "td"])

        if row_idx == 0 and cells and cells[0].name == "th":
            header_text = cells[0].get_text(strip=True)
            if header_text == "Ver.":
                continue

        first_cell = cells[0]
        if first_cell.name == "th":
            version_link = first_cell.find("a")
            current_version = (
                version_link.get_text(strip=True)
                if version_link
                else first_cell.get_text(strip=True)
            )
            version_rowspan_remaining = int(first_cell.get("rowspan", 1)) - 1
            td_cells = [c for c in cells[1:] if c.name == "td"]
        elif version_rowspan_remaining > 0:
            td_cells = [c for c in cells if c.name == "td"]
            version_rowspan_remaining -= 1
        else:
            continue

        if not td_cells:
            continue

        agent_td = td_cells[0]

        link_tag = agent_td.find("a", class_="a-link")
        if link_tag:
            raw_name = link_tag.get_text(strip=True)
        else:
            img = agent_td.find("img")
            raw_name = img.get("alt", "") if img else agent_td.get_text(strip=True)

        agent_name = _clean_agent_name(raw_name)

        if not agent_name:
            logger.debug("banners.parse: Skipping empty agent name at row %d", row_idx)
            continue
        if "screening" in agent_name:
            continue
        elif agent_name.strip() == "Orphie":
            agent_name = "Orphie & Magus"
        elif agent_name.strip() == "Soldier 0 - Anby":
            agent_name = "Anby: Soldier 0"

        cell_text = agent_td.get_text(separator=" ", strip=True)
        date_range = _parse_date_range(cell_text)

        if not date_range:
            logger.warning(
                "banners.parse: Could not parse date for %s (v%s) at row %d: %s",
                agent_name,
                current_version,
                row_idx,
                cell_text,
            )
            continue

        banner_start, banner_end = date_range

        banners.append(
            {
                "version": current_version,
                "agent_name": agent_name,
                "banner_start": banner_start,
                "banner_end": banner_end,
            }
        )

    logger.info("banners.parse.done parsed=%d", len(banners))
    return banners


def upsert_banners(con, banners: list[dict]):
    if not banners:
        logger.info("banners.upsert: No banners to write")
        return

    ensure_dim_patch_schema(con)
    con.execute(TABLE_DDL["dim_patch"])

    df = pd.DataFrame(banners)
    con.register("banner_tmp", df)

    try:
        con.execute(
            """
            INSERT INTO dim_patch (version, agent_name, banner_start, banner_end )
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
    logger.info("banners.start")

    try:
        html = fetch_banner_page()
        soup = BeautifulSoup(html, "html.parser")

        current_banners = parse_banner_current(soup)
        if not current_banners:
            logger.warning("banners.no_banners_parsed")
            return

        banners = parse_banners(soup)

        if not banners:
            logger.warning("banners.no_banners_parsed")
            return

        logger.info("banners.parsed count=%d", len(banners) + len(current_banners))

        with get_db() as con:
            upsert_banners(con, banners)
            upsert_banners(con, current_banners)

        logger.info("banners.success")

    except Exception:
        logger.exception("banners.failed")
        raise

    finally:
        logger.info("banners.end")
