from bs4 import BeautifulSoup
import pendulum
import pandas as pd
from pathlib import Path
import utils

logger = utils.get_logger(__name__)
RAW_DIR = Path("data/raw")
STAGE_DIR = Path("data/stage")


def load_html():
    html_path = RAW_DIR / "agents_latest.html"
    logger.info(f"Reading HTML file: {html_path}")
    return html_path.read_text()


def get_playable_agents(soup: BeautifulSoup):
    # second table on page [1]
    try:
        playable_agents = soup.find_all("table")[1].tbody.find_all("tr")[1:]
    except AttributeError:
        logger.error("Could not find the playable agents table in the HTML")
        playable_agents = []

    logger.info(f"Total playable agents found: {len(playable_agents)}")
    agents = []

    # print(playable_agents[0].find_all("td")[0])
    for agent_row in playable_agents:
        agent_tds = agent_row.find_all("td")

        try:
            agents.append(
                {
                    "icon": agent_tds[0].img.get("data-src")
                    or agent_tds[0].img.get("src"),
                    "name": agent_tds[1].a.text,
                    "rank": agent_tds[2].span["title"].split(" ")[1].strip(),
                    "attribute": agent_tds[3].a["title"],
                    "speciality": agent_tds[4].a["title"],
                    "faction": agent_tds[6].a["title"],
                    "release_date": list(agent_tds[7].stripped_strings)[0],
                    "release_version": list(agent_tds[7].stripped_strings)[3].split(
                        " "
                    )[1],
                }
            )
        except Exception as e:
            logger.exception(f"Failed to parse agent row: {agent_row}")

    agents_df = pd.DataFrame(agents)

    # Convert release_date to proper date
    agents_df["release_date"] = agents_df["release_date"].map(
        lambda x: pendulum.from_format(x, "MMMM DD, YYYY").date()
    )

    # Fix specific faction naming
    agents_df.loc[agents_df["name"] == "Jane Doe", "faction"] = (
        "Criminal Investigation Special Response Team"
    )

    return agents_df


def get_upcoming_agents(soup: BeautifulSoup):
    # third table on page [2]
    agents = []
    try:
        upcoming_agents = soup.find_all("table")[2].tbody.find_all("tr")[1:]
    except Exception:
        logger.warning("Could not find upcoming agents table")
        upcoming_agents = []

    logger.info(f"Total upcoming agents found: {len(upcoming_agents)}")

    for agent_row in upcoming_agents:
        agent_tds = agent_row.find_all("td")
        try:
            agents.append(
                {
                    "icon": agent_tds[0].img.get("data-src")
                    or agent_tds[0].img.get("src"),
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

    return pd.DataFrame(agents)


def save_df(df):
    STAGE_DIR.mkdir(parents=True, exist_ok=True)
    stage_path = STAGE_DIR / "agents.csv"

    df.to_csv(stage_path, index=False)

    logger.info(f"Saved cleaned agents CSV to {stage_path}")
    logger.info(f"Total agents saved: {len(df)}")


# main
def parse_agents_func():
    html = load_html()
    soup = BeautifulSoup(html, "html.parser")

    playable_agents_df = get_playable_agents(soup)
    upcoming_agents_df = get_upcoming_agents(soup)

    agents_df = pd.concat([playable_agents_df, upcoming_agents_df], ignore_index=True)

    save_df(agents_df)


if __name__ == "__main__":
    parse_agents_func()
