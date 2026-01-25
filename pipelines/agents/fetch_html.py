import logging
import requests
import pendulum
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

url = "https://zenless-zone-zero.fandom.com/wiki/Agent/List"
agent_dir = "data/agents"
html_path = f"{agent_dir}/raw_agents_{pendulum.now().to_date_string()}.html"

try:
    logger.info("agent/fetch_html.py started")
    logger.info(f"Requesting URL: {url}")

    res = requests.get(url, timeout=30)
    logger.info(f"Response status code: {res.status_code}")

    if res.status_code != 200:
        logger.error(f"Request failed with status {res.status_code}")
        res.raise_for_status()

    os.makedirs(agent_dir, exist_ok=True)

    with open(html_path, "w") as f:
        f.write(res.text)

    logger.info(f"HTML saved successfully to {html_path}")
    logger.info("Scraping job completed successfully")

except requests.exceptions.RequestException as e:
    logger.exception(f"Network-related error occurred: {e}")

except OSError as e:
    logger.exception(f"File system error occurred: {e}")

except Exception as e:
    logger.exception(f"Unexpected error occurred: {e}")
