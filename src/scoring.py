import json
from pathlib import Path

import pandas as pd

from src.utils import get_logger, WORK_DIR

logger = get_logger(__name__)

CONFIG_PATH = Path(WORK_DIR) / "data" / "scoring_config.json"


def load_scoring_config(path: Path | None = None) -> dict:
    path = path or CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"Scoring config not found: {path}")
    with open(path) as f:
        config = json.load(f)
    logger.info(f"Loaded scoring config from {path}")
    return config


def compute_video_score(
    title: str,
    description: str,
    tags: list[str],
    relevant_keywords: dict[str, float],
    irrelevant_keywords: dict[str, float],
    title_weight: float = 1.5,
    tags_weight: float = 1.0,
    description_weight: float = 0.5,
) -> float:
    title_lower = title.lower()
    desc_lower = (description or "").lower()
    tags_text = " ".join(tags).lower() if tags else ""

    score = 0.0

    for keyword, weight in relevant_keywords.items():
        kw_lower = keyword.lower()
        if kw_lower in title_lower:
            score += weight * title_weight
        if kw_lower in tags_text:
            score += weight * tags_weight
        if kw_lower in desc_lower:
            score += weight * description_weight

    for keyword, penalty in irrelevant_keywords.items():
        kw_lower = keyword.lower()
        abs_penalty = abs(penalty)
        if kw_lower in title_lower:
            score -= abs_penalty * title_weight
        if kw_lower in tags_text:
            score -= abs_penalty * tags_weight
        if kw_lower in desc_lower:
            score -= abs_penalty * description_weight

    return score


def score_videos(
    df: pd.DataFrame,
    config: dict | None = None,
) -> pd.DataFrame:
    if config is None:
        config = load_scoring_config()

    relevant_kw = config.get("relevant_keywords", {})
    irrelevant_kw = config.get("irrelevant_keywords", {})
    scoring_params = config.get("scoring", {})
    min_score = scoring_params.get("min_score_to_include", 1.0)
    title_w = scoring_params.get("title_weight", 1.5)
    tags_w = scoring_params.get("tags_weight", 1.0)
    desc_w = scoring_params.get("description_weight", 0.5)

    scores = []
    for _, row in df.iterrows():
        tags = row.get("tags", [])
        if not isinstance(tags, list):
            tags = []
        score = compute_video_score(
            title=row["title"],
            description=row.get("description", "") or "",
            tags=tags,
            relevant_keywords=relevant_kw,
            irrelevant_keywords=irrelevant_kw,
            title_weight=title_w,
            tags_weight=tags_w,
            description_weight=desc_w,
        )
        scores.append(score)

    df = df.copy()
    df["relevance_score"] = scores
    df["is_relevant"] = df["relevance_score"] >= min_score

    n_relevant = df["is_relevant"].sum()
    n_total = len(df)
    logger.info(
        f"Scored {n_total} videos: {n_relevant} relevant "
        f"({n_relevant / n_total * 100:.1f}%) with min_score={min_score}"
    )

    return df


def score_existing_videos(con, config: dict | None = None) -> int:
    if config is None:
        config = load_scoring_config()

    try:
        df = con.sql(
            "SELECT video_id, title, description, tags, relevance_score FROM dim_video"
        ).to_df()
    except Exception:
        logger.exception("Failed to fetch videos for scoring")
        return 0

    if df.empty:
        logger.info("No videos to score")
        return 0

    unscored = df[df["relevance_score"] == 0.0]
    if unscored.empty:
        logger.info("All videos already scored")
        return 0

    logger.info(f"Scoring {len(unscored)} unscored videos out of {len(df)} total")
    scored_df = score_videos(unscored, config)

    update_df = scored_df[["video_id", "relevance_score", "is_relevant"]]
    from src.warehouse import update_video_scores

    update_video_scores(con, update_df)

    return len(unscored)
