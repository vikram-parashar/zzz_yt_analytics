import hashlib
from pathlib import Path
import requests
import pendulum
import utils

logger = utils.get_logger(__name__)
RAW_DIR = Path("data/raw")
URL = "https://zenless-zone-zero.fandom.com/wiki/Agent"


# requests.get () doesn't work without it in airflow, but works as script
def get_session():
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        }
    )
    return session


def fetch_agents_page() -> str:
    logger.info("Fetching agents page from wiki")
    session = get_session()
    res = session.get(URL)
    res.raise_for_status()
    logger.info("Page fetched | status=%s", res.status_code)
    return res.text


def save_raw(html: str, run_date: str):
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    latest_path = RAW_DIR / "agents_latest.html"
    dated_path = RAW_DIR / f"agents_{run_date}.html"

    new_hash = hashlib.md5(html.encode()).hexdigest()
    old_hash = (
        hashlib.md5(latest_path.read_bytes()).hexdigest()
        if latest_path.exists()
        else None
    )

    if new_hash == old_hash:
        logger.info("No change detected in agents page")
        return

    latest_path.write_text(html, encoding="utf-8")
    dated_path.write_text(html, encoding="utf-8")

    logger.info("Saved new snapshot | hash=%s", new_hash)


# main
def pull_agents_html(**context):  # Airflow can pass context
    run_date = pendulum.now("UTC").to_datetime_string()
    logger.info("Agents extraction started | run_date=%s", run_date)

    html = fetch_agents_page()
    save_raw(html, run_date)

    logger.info("Agents extraction finished")


if __name__ == "__main__":
    pull_agents_html()
