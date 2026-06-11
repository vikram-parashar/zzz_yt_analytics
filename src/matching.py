from src.utils import get_db, get_logger
from src.warehouse import update_attribution_weights

logger = get_logger(__name__)

MATCH_WEIGHTS = {
    "title": 10,
    "tags": 5,
    "description": 2,
}


def _do_match(con, incremental: bool):
    video_filter = (
        " WHERE v.is_matched = FALSE OR v.is_matched IS NULL " if incremental else ""
    )

    sql = f"""
    INSERT INTO bridge_video_agent (
        video_id,
        agent_name,
        confidence
    )
    WITH aliases AS (
        SELECT
            name,
            alias,
            '\\b' || regexp_escape(alias) || '\\b' AS pattern
        FROM bridge_agent_alias
    ),

    scored AS (
        SELECT
            v.video_id,
            a.name AS agent_name,

            (
                CASE WHEN regexp_matches(v.title, a.pattern) THEN 10 ELSE 0 END
                +
                CASE WHEN regexp_matches(v.description, a.pattern) THEN 2 ELSE 0 END
                +
                CASE WHEN regexp_matches(array_to_string(v.tags, ' '), a.pattern) THEN 5 ELSE 0 END
            ) AS confidence

        FROM dim_video v
        CROSS JOIN aliases a
        {video_filter}
    )

    SELECT
        video_id,
        agent_name,
        confidence
    FROM scored
    WHERE confidence > 0

    ON CONFLICT (video_id, agent_name)
    DO UPDATE SET
        confidence = excluded.confidence
    """

    con.execute(sql)

    update_attribution_weights(con)


def match_remaining():
    with get_db() as con:
        videos = con.sql("""
            SELECT COUNT(*)
            FROM dim_video
            WHERE is_matched = FALSE
               OR is_matched IS NULL
        """).fetchone()[0]

        if videos == 0:
            logger.info("No unmatched videos found")
            return

        logger.info(f"Matching {videos} unmatched videos")

        _do_match(con, incremental=True)

        con.execute("""
            UPDATE dim_video
            SET is_matched = TRUE
            WHERE is_matched = FALSE
               OR is_matched IS NULL
        """)

        logger.info(f"match_remaining complete: {videos} videos processed ")


def match_all():
    with get_db() as con:
        videos = con.sql("""
            SELECT COUNT(*)
            FROM dim_video
        """).fetchone()[0]

        if videos == 0:
            logger.info("No videos found")
            return

        logger.info(f"Re-matching all {videos} videos")

        con.execute("DELETE FROM bridge_video_agent")

        _do_match(con, incremental=False)

        con.execute("""
            UPDATE dim_video
            SET is_matched = TRUE
        """)

        logger.info(f"match_all complete: {videos} videos processed ")
