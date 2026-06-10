from collections import defaultdict
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


def _word_match(text: str, term: str) -> int:
    term = term.lower().strip()
    if not term:
        return 0

    text = text.lower()

    cnt = (len(text) - len(text.replace(term, ""))) // len(term)

    if " " in term:
        no_space_term = term.replace(" ", "")
        if no_space_term:
            cnt += (len(text) - len(text.replace(no_space_term, ""))) // len(
                no_space_term
            )

    return cnt


def _compute_confidence(video_row, aliases) -> dict[str, int]:
    results = defaultdict(int)

    title = normalize(str(video_row.title))
    description = normalize(str(video_row.description))

    tags_raw = video_row.tags
    tags = tags_raw if isinstance(tags_raw, list) else []
    tags_text = normalize(" ".join(tags))

    for agent, alias in aliases:
        score = 0

        score += MATCH_WEIGHTS["title"] * _word_match(title, alias)
        score += MATCH_WEIGHTS["description"] * _word_match(description, alias)
        score += MATCH_WEIGHTS["tags"] * _word_match(tags_text, alias)

        if score == 0:
            continue

        results[agent] += score
    return results


def match_videos(con, videos_df: pd.DataFrame | None = None):
    aliases_df = con.sql("""
        SELECT name, alias
        FROM bridge_agent_alias
    """).df()

    if aliases_df.empty:
        logger.warning("No aliases found — skipping matching")
        return
    aliases = list(aliases_df[["name", "alias"]].itertuples(index=False, name=None))

    if videos_df is None:
        videos_df = con.sql("""
            SELECT
                video_id,
                title,
                description,
                tags
            FROM dim_video
        """).df()

    if videos_df is None:
        logger.warning("cannot fetch dim_video")
        return

    if videos_df.empty:
        logger.info("No videos to match")
        return

    results = []

    for video in videos_df.itertuples(index=False):
        scores = _compute_confidence(video, aliases)

        for agent, confidence in scores.items():
            results.append(
                {
                    "video_id": video.video_id,
                    "agent_name": agent,
                    "confidence": confidence,
                }
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


def match_all_videos():
    with get_db() as con:
        match_videos(con)
