import requests
import os
import pendulum
import pandas as pd
from bs4 import BeautifulSoup

url = "https://zenless-zone-zero.fandom.com/wiki/Agent/List"
res = requests.get(url)
soup = BeautifulSoup(res.text, "html.parser")

playable_agents = soup.table.tbody.find_all("tr")[1:]
agents = []
print("total playable agents", len(playable_agents))
for agent_row in playable_agents:
    agent_tds = agent_row.find_all("td")
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
agents_df = pd.DataFrame(agents)

agents_df["release_date"] = agents_df["release_date"].map(
    lambda x: pendulum.from_format(x, "MMMM DD, YYYY").date()
)

agents = []
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
            "faction": agent_tds[6].a["title"],
            "release_date": None,
            "release_version": None,
        }
    )
agents_df_new = pd.DataFrame(agents)
agents_df = pd.concat([agents_df, agents_df_new], ignore_index=True)
agents_df = agents_df[agents_df["icon"].str.startswith("http")]
agents_df = agents_df.drop_duplicates(subset=["name"])
# was 'Criminal Investigation Special Response Team (Jhon Doe)'
agents_df.loc[agents_df["name"] == "Jane Doe", "faction"] = (
    "Criminal Investigation Special Response Team"
)

data_path = "../data"
if not os.path.exists(data_path):
    os.mkdir(data_path)
agents_df.to_csv(f"{data_path}/all_agents.csv", index=False)
