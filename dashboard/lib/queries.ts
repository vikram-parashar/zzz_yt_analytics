export const AGENT_STATS_QUERY = `
  SELECT
      a.name, a.img, a.rank, a.attribute, a.speciality, a.faction, a.release_date,
      fad.video_count,
      fad.attributed_views AS total_views,
      fad.attributed_likes AS total_likes,
      fad.attributed_comments AS total_comments,
      (p.agent_name IS NOT NULL) AS on_banner
  FROM dim_agent a
  LEFT JOIN (
      SELECT DISTINCT ON (agent_name)
          *
      FROM fact_agent_daily
      ORDER BY agent_name, snapshot_date DESC
  ) fad
      ON a.name = fad.agent_name
  LEFT JOIN (
      SELECT *
      FROM dim_patch
      WHERE now() BETWEEN banner_start AND banner_end
  ) p
      ON a.name = p.agent_name;
`;
export const DIM_PATCH_QUERY = `
WITH distinct_runs AS (
    SELECT DISTINCT
        banner_start,
        banner_end,
        agent_name
    FROM dim_patch
),
agent_history AS (
    SELECT
        banner_start,
        banner_end,
        agent_name,
        MIN(banner_start) OVER (
            PARTITION BY agent_name
        ) AS first_seen_date
    FROM distinct_runs
)
SELECT
    banner_start,
    banner_end,
    array_to_string(
        ARRAY_AGG(
            agent_name
            ORDER BY first_seen_date desc, agent_name
        ),
        '/'
    ) AS label
FROM agent_history
GROUP BY banner_start, banner_end
having banner_start>='2026-07-01'
ORDER BY banner_start
`;
export function agentLookupQuery(agentName: string): string {
  const safe = agentName.replace(/'/g, "''");
  return `
    SELECT
      a.name, a.img, a.rank, a.attribute, a.speciality, a.faction, a.release_date,
      fad.video_count,
      fad.attributed_views AS total_views,
      fad.attributed_likes AS total_likes,
      fad.attributed_comments AS total_comments,
      (p.agent_name IS NOT NULL) AS on_banner
  FROM (SELECT * FROM dim_agent WHERE name='${safe}') a
  LEFT JOIN (
      SELECT DISTINCT ON (agent_name)
          *
      FROM fact_agent_daily
      ORDER BY agent_name, snapshot_date DESC
  ) fad
      ON a.name = fad.agent_name
  LEFT JOIN (
      SELECT *
      FROM dim_patch
      WHERE now() BETWEEN banner_start AND banner_end
  ) p
      ON a.name = p.agent_name;
  `;
}
export const AGENT_NAMES_QUERY = `
  SELECT name, img, rank, attribute, speciality, faction FROM dim_agent
`;
export function topAgentsTimelineQuery(startDate: string, endDate: string): string {
  const sd = startDate.replace(/'/g, "''");
  const ed = endDate.replace(/'/g, "''");
  return `
    WITH _top_agents AS (
      SELECT b.agent_name
      FROM bridge_video_agent b
      JOIN dim_video v ON v.video_id = b.video_id
      WHERE v.published_at >= '${sd}' AND v.published_at <= '${ed}'
      GROUP BY b.agent_name
      ORDER BY SUM(b.confidence) DESC
      LIMIT 5
    )
    SELECT
      b.agent_name,
      TO_CHAR(v.published_at, 'YYYY-MM') AS month,
      COUNT(*) AS video_count
    FROM (SELECT * FROM bridge_video_agent WHERE attribution_weight >= 0.2 AND agent_name IN (SELECT agent_name FROM _top_agents)) AS b
    JOIN dim_video v ON v.video_id = b.video_id
    WHERE v.published_at >= '${sd}' AND v.published_at <= '${ed}'
    GROUP BY b.agent_name, TO_CHAR(v.published_at, 'YYYY-MM')
    ORDER BY b.agent_name, month
  `;
}
export const bannerAgentGainQuery = (bannerStart: string, bannerEnd: string) => `
WITH lo AS (
    SELECT DISTINCT ON (agent_name)
        agent_name,
        attributed_views AS lo_views
    FROM fact_agent_daily
    WHERE snapshot_date >= DATE '${bannerStart}' - INTERVAL '7 days'
    ORDER BY agent_name, snapshot_date ASC
),
hi AS (
    SELECT DISTINCT ON (agent_name)
        agent_name,
        attributed_views AS hi_views
    FROM fact_agent_daily
    WHERE snapshot_date <= DATE '${bannerEnd}'
    ORDER BY agent_name, snapshot_date DESC
)
SELECT
    hi.agent_name,
    hi.hi_views - COALESCE(lo.lo_views, 0) AS view_gain
FROM hi
LEFT JOIN lo USING (agent_name)
WHERE hi.hi_views - COALESCE(lo.lo_views, 0) > 0
ORDER BY view_gain DESC
LIMIT 5
`;
export function risingCreatorsQuery(
  timeRange: 'week' | 'month' | 'year'
): string {
  const interval = timeRange === 'week' ? '7 days' : timeRange === 'month' ? '1 month' : '1 year';
  return `
        WITH daily_ranked AS (
        SELECT
            channel_id,
            subscriber_count,
            view_count,
            ROW_NUMBER() OVER (
                PARTITION BY channel_id
                ORDER BY snapshot_date ASC
            ) AS rn_first,
            ROW_NUMBER() OVER (
                PARTITION BY channel_id
                ORDER BY snapshot_date DESC
            ) AS rn_last
        FROM fact_channel_daily
        WHERE snapshot_date >= CURRENT_DATE - INTERVAL '${interval}'
    ),
    channel_growth AS (
        SELECT
            channel_id,
            MAX(subscriber_count) FILTER (WHERE rn_first = 1) AS start_subs,
            MAX(subscriber_count) FILTER (WHERE rn_last = 1)  AS end_subs,
            MAX(view_count) FILTER (WHERE rn_first = 1) AS start_views,
            MAX(view_count) FILTER (WHERE rn_last = 1)  AS end_views
        FROM daily_ranked
        GROUP BY channel_id
    ),
    video_counts AS (
        SELECT
            channel_id,
            COUNT(*) AS video_cnt
        FROM dim_video
        GROUP BY channel_id
    ),
    total_videos AS (
    SELECT DISTINCT ON (channel_id)
      channel_id,
      video_count,
      snapshot_date
    FROM fact_channel_daily
    ORDER BY channel_id, snapshot_date DESC
    )
    SELECT
        c.channel_id,
        c.channel_name,
        c.thumbnail,
        vc.video_cnt AS videos_collected,
        tv.video_count AS total_videos,
        CASE
            WHEN ch.end_views IS NULL
              OR ch.start_views IS NULL
              OR ch.end_subs IS NULL
              OR ch.start_subs IS NULL
              OR ch.end_subs <= ch.start_subs
            THEN NULL
            ELSE
                ROUND((ch.end_views - ch.start_views)
                / NULLIF(ch.end_subs - ch.start_subs, 0),0)
        END AS views_per_new_sub
    FROM dim_channel c
    JOIN channel_growth ch USING (channel_id)
    JOIN video_counts vc USING (channel_id)
    JOIN total_videos tv USING (channel_id)
    ORDER BY views_per_new_sub DESC NULLS LAST
    LIMIT 50
  `;
}
export function agentVideoTimelineQuery(agentName: string, startDate: string, endDate: string): string {
  const safe = agentName.replace(/'/g, "''");
  const sd = startDate.replace(/'/g, "''");
  const ed = endDate.replace(/'/g, "''");
  return `
    SELECT
        TO_CHAR(dv.published_at, 'YYYY-MM') AS month,
        COUNT(*) AS video_cnt
    FROM bridge_video_agent bva
    JOIN dim_video dv
        ON bva.video_id = dv.video_id
    WHERE bva.agent_name = '${safe}'
      AND dv.published_at BETWEEN '${sd}' AND '${ed}'
    GROUP BY 1
    ORDER BY 1;
  `;
}
export function agentBannersQuery(agentName: string): string {
  const safe = agentName.replace(/'/g, "''");
  return `SELECT version, CAST(banner_start AS VARCHAR) AS banner_start, CAST(banner_end AS VARCHAR) AS banner_end FROM dim_patch WHERE agent_name = '${safe}' ORDER BY banner_start`;
}
export function agentEngagementTrendQuery(agentName: string): string {
  const safe = agentName.replace(/'/g, "''");
  return `
    SELECT
      CAST(snapshot_date AS VARCHAR) AS date,
      attributed_views AS views,
      attributed_likes AS likes
    FROM fact_agent_daily
    WHERE agent_name = '${safe}'
      AND snapshot_date >= NOW() - INTERVAL '30 days'
    ORDER BY snapshot_date
  `;
}
export function agentMostLikedVideoQuery(agentName: string): string {
  const safe = agentName.replace(/'/g, "''");
  return `
    SELECT
        dv.video_id,
        dv.title,
        dv.published_at,
        dv.latest_view_count AS view_count,
        dv.latest_like_count AS like_count,
        bva.attribution_weight*(
            1000 * (
                SELECT CAST(SUM(latest_like_count) AS DOUBLE PRECISION)
                       / NULLIF(SUM(latest_view_count), 0)
                FROM dim_video
            )
            + dv.latest_like_count
        )
        /
        (1000 + dv.latest_view_count) AS like_rate
    FROM bridge_video_agent bva
    LEFT JOIN dim_video dv
        ON bva.video_id = dv.video_id
    WHERE bva.agent_name = '${safe}'
    ORDER BY like_rate DESC
    LIMIT 50;
  `;
}
export function agentMostViewedOnQuery(agentName: string): string {
  const safe = agentName.replace(/'/g, "''");
  return `
    SELECT
        dv.channel_id,
        dc.channel_name,
        FLOOR(SUM(dv.latest_view_count * bva.attribution_weight)) AS total_views
    FROM bridge_video_agent bva
    LEFT JOIN dim_video dv
        ON bva.video_id = dv.video_id
    LEFT JOIN dim_channel dc
        ON dc.channel_id = dv.channel_id
    WHERE bva.agent_name = '${safe}'
    GROUP BY
        dv.channel_id,
        dc.channel_name
    ORDER BY
        total_views DESC
    LIMIT 50
  `;
}
export function agentCoOccurringQuery(agentName: string): string {
  const safe = agentName.replace(/'/g, "''");
  return `
    SELECT bva2.agent_name, COUNT(DISTINCT bva1.video_id) AS co_video_count
    FROM bridge_video_agent bva1
    JOIN bridge_video_agent bva2 ON bva1.video_id = bva2.video_id AND bva2.agent_name != '${safe}'
    WHERE bva1.agent_name = '${safe}'
    GROUP BY bva2.agent_name
    ORDER BY co_video_count DESC
    LIMIT 10
  `;
}