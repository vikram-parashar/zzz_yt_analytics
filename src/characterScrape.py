import os
from bs4 import BeautifulSoup
import requests
import pandas as pd


def main():
    url = "https://zenless-zone-zero.fandom.com/wiki/Agent/List"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    agents = []
    playable_agents = soup.table.tbody.find_all("tr")[1:]
    for agent_row in playable_agents:
        agent_tds = agent_row.find_all("td")
        agents.append(
            {
                "icon": agent_tds[0].img.get("data-src") or agent_tds[0].img.get("src"),
                "name": agent_tds[1].a.text,
                "rank": agent_tds[2].span["title"].split(" ")[1].strip(),
                "attribute": agent_tds[3].a["title"],
                "speciality": agent_tds[4].a["title"],
                "attack_type": agent_tds[5]
                .find("span", class_="zzw-icon-subtitle-text")
                .text,
                "faction": agent_tds[6].a["title"],
                "release_date": list(agent_tds[7].stripped_strings)[0],
                "release_version": list(agent_tds[7].stripped_strings)[3].split(" ")[1],
            }
        )

    upcoming_agents = soup.find_all("table")[1].tbody.find_all("tr")[1:]
    for agent_row in upcoming_agents:
        agent_tds = agent_row.find_all("td")
        agents.append(
            {
                "icon": agent_tds[0].img.get("data-src") or agent_tds[0].img.get("src"),
                "name": agent_tds[1].a.text,
                "rank": None,
                "attribute": agent_tds[3].a["title"],
                "speciality": agent_tds[4].a["title"],
                "attack_type": None,
                "faction": agent_tds[6].a["title"],
                "release_date": None,
                "release_version": None,
            }
        )

    agents_df = pd.DataFrame(agents)
    agents_df.head()

    data_path = "../data"
    if not os.path.exists(data_path):
        os.mkdir(data_path)
    agents_df.to_csv(f"{data_path}/all_agents.csv", index=False)


if __name__ == "__main__":
    main()
