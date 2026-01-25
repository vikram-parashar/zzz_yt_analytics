import os
import logging
from bs4 import BeautifulSoup
import pendulum
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

"""
Paths
"""
agent_dir = "data/agents"
html_path = f"{agent_dir}/raw_agents_{pendulum.now().to_date_string()}.html"
cleaned_path = f"{agent_dir}/clean_agent.csv"

"""
Load HTML
"""
if not os.path.exists(html_path):
    logger.error(f"No HTML file found for today: {html_path}")
    raise FileNotFoundError(f"No HTML file found for today: {html_path}")

logger.info("agent/parse.html started")
logger.info(f"Reading HTML file: {html_path}")
with open(html_path, "r", encoding="utf-8") as f:
    html = f.read()
    if not html.strip():
        logger.warning("HTML file is empty, using placeholder content")
        html = "<html></html>"

"""
Parse HTML
"""
soup = BeautifulSoup(html, "html.parser")

"""
Parse playable agents
"""
try:
    playable_agents = soup.table.tbody.find_all("tr")[1:]
except AttributeError:
    logger.error("Could not find the playable agents table in the HTML")
    playable_agents = []

logger.info(f"Total playable agents found: {len(playable_agents)}")
agents = []

for agent_row in playable_agents:
    agent_tds = agent_row.find_all("td")
    try:
        agents.append(
            {
                "icon": agent_tds[0].img.get("data-src") or agent_tds[0].img.get("src"),
                "name": agent_tds[1].a.text,
                "rank": agent_tds[2].span["title"].split(" ")[1].strip(),
                "attribute": agent_tds[3].a["title"],
                "speciality": agent_tds[4].a["title"],
                "faction": agent_tds[6].a["title"],
                "release_date": list(agent_tds[7].stripped_strings)[0],
                "release_version": list(agent_tds[7].stripped_strings)[3].split(" ")[1],
            }
        )
    except Exception as e:
        logger.exception(f"Failed to parse agent row: {agent_row}")
agents_df = pd.DataFrame(agents)

# Convert release_date to proper date
agents_df["release_date"] = agents_df["release_date"].map(
    lambda x: pendulum.from_format(x, "MMMM DD, YYYY").date()
)

"""
Parse upcoming agents
"""
agents = []
try:
    upcoming_agents = soup.find_all("table")[1].tbody.find_all("tr")[1:]
except Exception:
    logger.warning("Could not find upcoming agents table")
    upcoming_agents = []

logger.info(f"Total upcoming agents found: {len(upcoming_agents)}")

for agent_row in upcoming_agents:
    agent_tds = agent_row.find_all("td")
    try:
        agents.append(
            {
                "icon": agent_tds[0].img.get("data-src") or agent_tds[0].img.get("src"),
                "name": agent_tds[1].a.text,
                "rank": None,
                "attribute": agent_tds[3].a["title"],
                "speciality": agent_tds[4].a["title"],
                "faction": agent_tds[6].a["title"],
                "release_date": None,
                "release_version": None,
            }
        )
    except Exception as e:
        logger.exception(f"Failed to parse upcoming agent row: {agent_row}")

agents_df_new = pd.DataFrame(agents)
agents_df = pd.concat([agents_df, agents_df_new], ignore_index=True)

"""
Clean data
"""
agents_df = agents_df[agents_df["icon"].str.startswith("http")]
agents_df = agents_df.drop_duplicates(subset=["name"])

# Fix specific faction naming
agents_df.loc[agents_df["name"] == "Jane Doe", "faction"] = (
    "Criminal Investigation Special Response Team"
)

# Save cleaned CSV
agents_df.to_csv(cleaned_path, index=False)
logger.info(f"Saved cleaned agents CSV to {cleaned_path}")
logger.info(f"Total agents saved: {len(agents_df)}")
