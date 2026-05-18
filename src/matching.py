import pandas as pd
from tqdm import tqdm

from src.utils import get_logger, get_db

logger = get_logger(__name__)

# Weights for where an alias match is found.
# Title match is strongest signal, tags second, description weakest.
MATCH_WEIGHTS = {"title": 0.6, "tags": 0.3, "description": 0.1}


def _compute_confidence(
    video_row: pd.Series, aliases: pd.DataFrame
) -> dict[str, float]:
    scores: dict[str, float] = {}
    title = video_row["title"].lower()
    description = (video_row["description"] or "").lower()
    tags_text = " ".join(video_row["tags"]).lower()

    for _, alias_row in aliases.iterrows():
        alias = alias_row["alias"].lower()
        score = 0.0
        if alias in title:
            score += MATCH_WEIGHTS["title"]
        if alias in description:
            score += MATCH_WEIGHTS["description"]
        if alias in tags_text:
            score += MATCH_WEIGHTS["tags"]

        if score > 0:
            agent = alias_row["name"]
            # Keep the highest score if multiple aliases match
            scores[agent] = max(scores.get(agent, 0), score)

    return scores


def match_videos_to_agents(con=None):
    should_close = False
    if con is None:
        con_cm = get_db()
        con = con_cm.__enter__()
        should_close = True

    try:
        aliases = con.sql("SELECT * FROM bridge_agent_alias").df()
        videos = con.sql(
            "SELECT video_id, title, description, tags FROM dim_video"
        ).df()

        logger.info(f"Matching {len(videos)} videos against {len(aliases)} aliases")

        results = []
        for _, video in tqdm(videos.iterrows(), total=len(videos), desc="Matching"):
            scores = _compute_confidence(video, aliases)
            for agent, confidence in scores.items():
                results.append(
                    {
                        "video_id": video["video_id"],
                        "agent_name": agent,
                        "confidence": confidence,
                    }
                )

        if results:
            df = pd.DataFrame(results)
            con.register("match_tmp", df)
            con.execute("""
                INSERT INTO bridge_video_agent (video_id, agent_name, confidence)
                SELECT video_id, agent_name, confidence
                FROM match_tmp
                ON CONFLICT (video_id, agent_name) DO NOTHING
            """)
            logger.info(f"Wrote {len(results)} video-agent associations")
        else:
            logger.info("No video-agent matches found")

    finally:
        if should_close:
            con_cm.__exit__(None, None, None)
