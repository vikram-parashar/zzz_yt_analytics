import duckdb
from tqdm import tqdm
import utils
import pandas as pd

config = utils.load_config()


def insert_df(df: pd.DataFrame, con: duckdb.DuckDBPyConnection):
    if df.empty:
        return
    con.register("df_tmp", df)
    con.execute("""
    INSERT INTO bridge_video_agent (
        video_id,agent_name,confidence
    )
    SELECT 
        video_id,agent_name,confidence
    FROM df_tmp
    ON CONFLICT (video_id,agent_name) DO NOTHING
    """)


def build_video_agent_bridge_func(con):
    agent_aliases = con.sql("""
    SELECT * FROM bridge_agent_alias
    """).df()

    videos = con.sql("""
    SELECT video_id,title,description,tags FROM dim_video
    """).df()

    weights = {"title": 0.6, "tags": 0.3, "description": 0.1}
    res = []
    for _, video_row in tqdm(videos.iterrows()):
        confidence_scores = {}
        for _, alias_row in agent_aliases.iterrows():
            score = 0
            if alias_row["alias"].lower() in video_row["title"].lower():
                score += weights["title"]
            if alias_row["alias"].lower() in video_row["description"].lower():
                score += weights["description"]
            if alias_row["alias"].lower() in " ".join(video_row["tags"]).lower():
                score += weights["tags"]

            agent_name = alias_row["name"]
            if agent_name in confidence_scores:
                confidence_scores[agent_name] = max(
                    confidence_scores[agent_name], score
                )
            else:
                confidence_scores[agent_name] = score
        for agent_name in confidence_scores:
            if confidence_scores[agent_name] > 0:
                res.append(
                    {
                        "video_id": video_row["video_id"],
                        "agent_name": agent_name,
                        "confidence": confidence_scores[agent_name],
                    }
                )
        res_df = pd.DataFrame(res)
        insert_df(res_df, con)


if __name__ == "__main__":
    con = duckdb.connect(config["db"]["path"])
    build_video_agent_bridge_func(con)
