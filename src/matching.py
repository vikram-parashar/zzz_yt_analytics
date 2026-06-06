import math
import re

import pandas as pd

from src.utils import get_logger, get_db
from src.warehouse import update_attribution_weights

logger = get_logger(__name__)

MATCH_WEIGHTS = {
    "title": 10,
    "tags": 5,
    "description": 2,
}

_STRIP_RE = re.compile(r"[_/\-|]")
_OWNERSHIP_RE = re.compile(r"'s\b", re.IGNORECASE)
_MULTISPACE_RE = re.compile(r"\s+")


def normalize(text: str) -> str:
    if not text:
        return ""

    text = _STRIP_RE.sub(" ", text)
    text = _OWNERSHIP_RE.sub("", text)
    text = _MULTISPACE_RE.sub(" ", text).strip()

    return text.lower()


def _word_match(text: str, term: str) -> bool:
    if any("\u4e00" <= c <= "\u9fff" for c in term):
        return term in text

    return bool(
        re.search(
            rf"\b{re.escape(term)}\b",
            text,
            re.IGNORECASE,
        )
    )


def _compute_confidence(
    video_row: pd.Series,
    aliases_df: pd.DataFrame,
) -> dict[str, float]:
    results = {}

    title = normalize(str(video_row.get("title", "")))
    description = normalize(str(video_row.get("description", "")))

    tags_raw = video_row.get("tags")
    tags = tags_raw if isinstance(tags_raw, list) else []
    tags = [normalize(str(t)) for t in tags]

    tag_matched_agents = set()

    for _, row in aliases_df.iterrows():
        alias = normalize(str(row["alias"]))
        agent = row["name"]

        if not alias:
            continue

        if any(_word_match(tag, alias) for tag in tags):
            tag_matched_agents.add(agent)

    num_tag_agents = len(tag_matched_agents)
    tag_multiplier = (
        1.0 if num_tag_agents <= 1 else 1.0 / (1 + math.log(num_tag_agents))
    )

    for _, row in aliases_df.iterrows():
        alias = normalize(str(row["alias"]))
        agent = row["name"]

        if not alias:
            continue

        score = 0.0

        if _word_match(title, alias):
            score += MATCH_WEIGHTS["title"]

        if _word_match(description, alias):
            score += MATCH_WEIGHTS["description"]

        if any(_word_match(tag, alias) for tag in tags):
            score += MATCH_WEIGHTS["tags"] * tag_multiplier

        if score == 0:
            continue

        alias_len = len(alias)

        current = results.get(agent)

        if (
            current is None
            or score > current["score"]
            or (score == current["score"] and alias_len > current["alias_len"])
        ):
            results[agent] = {
                "score": score,
                "alias_len": alias_len,
            }

    return {agent: data["score"] for agent, data in results.items()}


def match_videos(con, video_ids: list[str] | None = None):
    aliases_df = con.sql("""
        SELECT name, alias
        FROM bridge_agent_alias
    """).df()

    if aliases_df.empty:
        logger.warning("No aliases found — skipping matching")
        return

    if video_ids:
        placeholders = ", ".join(["?"] * len(video_ids))

        videos_df = con.execute(
            f"""
            SELECT
                video_id,
                title,
                description,
                tags
            FROM dim_video
            WHERE video_id IN ({placeholders})
            """,
            video_ids,
        ).df()
    else:
        videos_df = con.sql("""
            SELECT
                video_id,
                title,
                description,
                tags
            FROM dim_video
        """).df()

    if videos_df.empty:
        logger.info("No videos to match")
        return

    logger.info(f"Matching {len(videos_df)} videos against {len(aliases_df)} aliases")

    results = []

    for _, video in videos_df.iterrows():
        scores = _compute_confidence(video, aliases_df)

        for agent, confidence in scores.items():
            results.append(
                {
                    "video_id": video["video_id"],
                    "agent_name": agent,
                    "confidence": confidence,
                }
            )

    if video_ids:
        placeholders = ", ".join(["?"] * len(video_ids))

        con.execute(
            f"""
            DELETE FROM bridge_video_agent
            WHERE video_id IN ({placeholders})
            """,
            video_ids,
        )

    if results:
        results_df = pd.DataFrame(results)

        con.register("match_tmp", results_df)

        con.execute("""
            INSERT INTO bridge_video_agent (
                video_id,
                agent_name,
                confidence
            )
            SELECT
                video_id,
                agent_name,
                confidence
            FROM match_tmp
            ON CONFLICT (video_id, agent_name)
            DO UPDATE SET
                confidence = excluded.confidence
        """)

        logger.info(f"Wrote {len(results)} video-agent associations")
    else:
        logger.info("No video-agent matches found")

    update_attribution_weights(con)


def match_all_videos(con=None):
    if con is not None:
        match_videos(con)
    else:
        with get_db() as con:
            match_videos(con)
