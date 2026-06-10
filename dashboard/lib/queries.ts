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
  SELECT DISTINCT version, agent_name AS banner_agent,
    banner_start::VARCHAR AS banner_start, banner_end::VARCHAR AS banner_end
  FROM dim_patch
  ORDER BY banner_start
`;
export const FACT_MIN_DATE_QUERY = `
  SELECT MIN(snapshot_date)::VARCHAR AS mn FROM fact_agent_daily
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
  FROM (select * from dim_agent where name='${safe}') a
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
      strftime(v.published_at, '%Y-%m') AS month,
      COUNT(*) AS video_count
    FROM (SELECT * FROM bridge_video_agent WHERE attribution_weight >= 0.2 AND agent_name IN (SELECT agent_name FROM _top_agents)) AS b
    JOIN dim_video v ON v.video_id = b.video_id
    WHERE v.published_at >= '${sd}' AND v.published_at <= '${ed}'
    GROUP BY b.agent_name, strftime(v.published_at, '%Y-%m')
    ORDER BY b.agent_name, month
  `;
}
export function bannerAgentGainQuery(selectedVersion: string): string {
  const safe = selectedVersion.replace(/'/g, "''");
  return `
    WITH patch AS (
      SELECT
        version,
        banner_start::DATE AS banner_start,
        banner_end::DATE AS banner_end
      FROM dim_patch
      WHERE version = '${safe}'
    ),
    start_date AS (
      SELECT
        MIN(fad.snapshot_date)::DATE AS start_snap
      FROM fact_agent_daily fad
      CROSS JOIN patch p
      WHERE fad.snapshot_date >= p.banner_start - INTERVAL '7 day'
    ),
    end_date AS (
      SELECT
        MAX(fad.snapshot_date)::DATE AS end_snap
      FROM fact_agent_daily fad
      CROSS JOIN patch p
      WHERE fad.snapshot_date <= LEAST(CURRENT_DATE, p.banner_end)
    ),
    start_snap AS (
      SELECT
        fad.agent_name,
        fad.attributed_views AS start_views
      FROM fact_agent_daily fad
      CROSS JOIN start_date sd
      WHERE fad.snapshot_date::DATE = sd.start_snap
    ),
    end_snap AS (
      SELECT
        fad.agent_name,
        fad.attributed_views AS end_views
      FROM fact_agent_daily fad
      CROSS JOIN end_date ed
      WHERE fad.snapshot_date::DATE = ed.end_snap
    ),
    ranked AS (
      SELECT
        ss.agent_name,
        COALESCE(es.end_views, 0) - COALESCE(ss.start_views, 0) AS view_gain,
        ROW_NUMBER() OVER (
          ORDER BY COALESCE(es.end_views, 0) - COALESCE(ss.start_views, 0) DESC
        ) AS rn
      FROM start_snap ss
      LEFT JOIN end_snap es
        ON ss.agent_name = es.agent_name
    )
    SELECT
      agent_name,
      view_gain
    FROM ranked
    WHERE rn <= 5
    ORDER BY view_gain DESC
  `;
}
export function risingCreatorsQuery(_timeRange: 'week' | 'month' | 'year'): string {
  return `
    WITH channel_growth AS (
      SELECT
        channel_id,
        arg_min(subscriber_count, snapshot_date) AS start_subs,
        arg_max(subscriber_count, snapshot_date) AS end_subs,
        arg_min(view_count, snapshot_date) AS start_views,
        arg_max(view_count, snapshot_date) AS end_views,
        arg_min(video_count, snapshot_date) AS start_videos,
        arg_max(video_count, snapshot_date) AS end_videos
      FROM fact_channel_daily
      WHERE snapshot_date >= CURRENT_DATE - INTERVAL '1 year'
      GROUP BY channel_id
    ),
    video_counts AS (
      SELECT
        channel_id,
        COUNT(*) AS video_cnt
      FROM dim_video
      GROUP BY channel_id
    )
    SELECT
      cg.channel_id,
      c.channel_name,
      c.thumbnail,
      vc.video_cnt AS videos_collected,
      ROUND(100.0 * (end_subs - start_subs) / NULLIF(start_subs, 0), 2) AS sub_growth,
      ROUND(100.0 * (end_views - start_views) / NULLIF(start_views, 0), 2) AS view_growth,
      ROUND(100.0 * (end_videos - start_videos) / NULLIF(start_videos, 0), 2) AS video_cnt_growth,
      vc.video_cnt * (
        0.5 * (100.0 * (end_subs - start_subs) / NULLIF(start_subs, 0))
        + 0.3 * (100.0 * (end_views - start_views) / NULLIF(start_views, 0))
        + 0.2 * (100.0 * (end_videos - start_videos) / NULLIF(start_videos, 0))
      ) AS score
    FROM channel_growth cg
    JOIN dim_channel c USING (channel_id)
    JOIN video_counts vc USING (channel_id)
    ORDER BY score DESC
  `;
}
export function agentVideoTimelineQuery(agentName: string, startDate: string, endDate: string): string {
  const safe = agentName.replace(/'/g, "''");
  const sd = startDate.replace(/'/g, "''");
  const ed = endDate.replace(/'/g, "''");
  return `
    SELECT
        strftime(dv.published_at, '%Y-%m') AS month,
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
  return `SELECT version, banner_start::VARCHAR AS banner_start, banner_end::VARCHAR AS banner_end FROM dim_patch WHERE agent_name = '${safe}' ORDER BY banner_start`;
}
export function agentEngagementTrendQuery(agentName: string): string {
  const safe = agentName.replace(/'/g, "''");
  return `
    SELECT
      snapshot_date::VARCHAR AS date,
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
                SELECT SUM(latest_like_count)::DOUBLE
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