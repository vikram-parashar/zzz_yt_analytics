import pandas as pd
from tqdm import tqdm

from src.utils import get_logger, get_db

logger = get_logger(__name__)
import re
import math

def _word_boundary_match(text: str, term: str) -> bool:
    if any("\u4e00" <= c <= "\u9fff" for c in term):
        return term in text

    pattern = rf"\b{re.escape(term)}\b"
    return bool(re.search(pattern, text, re.IGNORECASE))


def _title_position_weight(title: str, match_start: int) -> float:
    words_before = len(title[:match_start].split())
    if words_before <= 3:
        return 1.0
    elif words_before <= 6:
        return 0.8
    else:
        return 0.5


def _score_tag_match(tags: list[str], alias: str) -> float:
    if not tags:
        return 0.0

    matched = sum(1 for t in tags if _word_boundary_match(t, alias))
    if matched == 0:
        return 0.0

    total_tags = len(tags)
    raw_fraction = matched / total_tags

    return math.sqrt(raw_fraction)


MATCH_WEIGHTS = {
    "title": 0.55,
    "tags": 0.30,
    "description": 0.15,
}


def _compute_confidence(
    video_row: pd.Series, aliases: pd.DataFrame
) -> dict[str, float]:
    scores: dict[str, float] = {}

    title = video_row["title"]
    title_lower = title.lower()
    description = (video_row["description"] or "").lower()
    tags = video_row["tags"] if isinstance(video_row["tags"], list) else []

    for _, alias_row in aliases.iterrows():
        alias = alias_row["alias"]
        alias_lower = alias.lower()
        canonical = alias_row["name"]

        component_score = 0.0

        if _word_boundary_match(title_lower, alias_lower):
            match = re.search(alias_lower, title_lower, re.IGNORECASE)
            if match:
                pos_weight = _title_position_weight(title, match.start())
            else:
                pos_weight = 0.8
            component_score += MATCH_WEIGHTS["title"] * pos_weight

        tag_score = _score_tag_match(tags, alias_lower)
        component_score += MATCH_WEIGHTS["tags"] * tag_score

        if _word_boundary_match(description, alias_lower):
            component_score += MATCH_WEIGHTS["description"]

        scores[canonical] = max(scores.get(canonical, 0), component_score)

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
                if confidence > 0:
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

        con.execute("""
            delete from bridge_video_agent
            where agent_name='Billy' and
            video_id in (select video_id from bridge_video_agent where agent_name='Billy - Starlight')
        """)
        con.execute("""
            delete from bridge_video_agent
            where agent_name='Anby' and
            video_id in (select video_id from bridge_video_agent where agent_name='Anby: Soldier 0')
        """)

    finally:
        if should_close:
            con_cm.__exit__(None, None, None)
