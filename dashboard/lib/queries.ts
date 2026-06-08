export const AGENT_STATS_QUERY = `
  WITH video_conf_total AS (
    SELECT video_id, SUM(confidence) AS total_conf FROM bridge_video_agent GROUP BY video_id
  ),
  latest_fvd AS (
    SELECT fvd.video_id, fvd.view_count, fvd.like_count, fvd.comment_count, fvd.snapshot_date
    FROM fact_video_daily fvd
    JOIN (
      SELECT video_id, MAX(snapshot_date) AS max_date FROM fact_video_daily GROUP BY video_id
    ) fvd_max ON fvd.video_id = fvd_max.video_id AND fvd.snapshot_date = fvd_max.max_date
  )
  SELECT
    a.name, a.img, a.rank, a.attribute, a.speciality, a.faction, a.release_date,
    COALESCE(vd.video_count, 0) AS video_count,
    COALESCE(vd.total_views, 0) AS total_views,
    COALESCE(vd.total_likes, 0) AS total_likes,
    COALESCE(vd.total_comments, 0) AS total_comments,
    COALESCE(vd.latest_view_count, 0) AS latest_view_count,
    CASE WHEN p.agent_name IS NOT NULL THEN true ELSE false END AS on_banner
  FROM dim_agent a
  LEFT JOIN (
    SELECT
      bva.agent_name,
      COUNT(DISTINCT bva.video_id) AS video_count,
      SUM(lf.view_count * bva.confidence / vct.total_conf) AS total_views,
      SUM(lf.like_count * bva.confidence / vct.total_conf) AS total_likes,
      SUM(lf.comment_count * bva.confidence / vct.total_conf) AS total_comments,
      SUM(lf.view_count * bva.confidence / vct.total_conf) AS latest_view_count
    FROM bridge_video_agent bva
    JOIN video_conf_total vct ON bva.video_id = vct.video_id
    JOIN latest_fvd lf ON bva.video_id = lf.video_id
    GROUP BY bva.agent_name
  ) vd ON a.name = vd.agent_name
  LEFT JOIN dim_patch p ON a.name = p.agent_name
    AND CURRENT_DATE >= p.banner_start AND CURRENT_DATE <= p.banner_end
  ORDER BY total_views DESC
`;
export const DIM_PATCH_QUERY = `
  SELECT DISTINCT version, agent_name AS banner_agent,
    banner_start::VARCHAR AS banner_start, banner_end::VARCHAR AS banner_end
  FROM dim_patch
  ORDER BY banner_start
`;
export function topAgentsTimelineQuery(startDate: string, endDate: string): string {
  return `
    WITH _top_agents AS (
      SELECT b.agent_name
      FROM bridge_video_agent b
      JOIN dim_video v ON v.video_id = b.video_id
      WHERE v.published_at >= '${startDate}' AND v.published_at <= '${endDate}'
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
    WHERE v.published_at >= '${startDate}' AND v.published_at <= '${endDate}'
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
  return `
    SELECT
      strftime(dv.published_at, '%Y-%m') AS month,
      COUNT(*) AS video_cnt
    FROM bridge_video_agent bva
    JOIN dim_video dv ON bva.video_id = dv.video_id
    WHERE bva.agent_name = '${safe}'
      AND dv.published_at >= '${startDate}'
      AND dv.published_at <= '${endDate}'
    GROUP BY strftime(dv.published_at, '%Y-%m')
    ORDER BY month
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
export function agentMostLikedVideoQuery(agentName: string, limit: number = 50): string {
  const safe = agentName.replace(/'/g, "''");
  return `
    WITH latest_fvd AS (
      SELECT fvd.video_id, fvd.view_count, fvd.like_count
      FROM fact_video_daily fvd
      JOIN (SELECT video_id, MAX(snapshot_date) AS max_date FROM fact_video_daily GROUP BY video_id) mx
        ON fvd.video_id = mx.video_id AND fvd.snapshot_date = mx.max_date
    ),
    global_rate AS (
      SELECT COALESCE(SUM(like_count), 0) AS total_likes, COALESCE(SUM(view_count), 0) AS total_views
      FROM latest_fvd
    )
    SELECT
      dv.video_id, dv.title, dv.published_at,
      lf.view_count, lf.like_count,
      CASE WHEN lf.view_count > 0
        THEN (1000 * COALESCE(gr.total_likes * 1.0 / NULLIF(gr.total_views, 0), 0) + lf.like_count)
             / (1000 + lf.view_count)
        ELSE 0
      END AS like_score
    FROM bridge_video_agent bva
    JOIN dim_video dv ON bva.video_id = dv.video_id
    JOIN latest_fvd lf ON bva.video_id = lf.video_id
    CROSS JOIN global_rate gr
    WHERE bva.agent_name = '${safe}' AND lf.view_count > 0
    ORDER BY like_score DESC
    LIMIT ${limit}
  `;
}
export function agentMostViewedOnQuery(agentName: string): string {
  const safe = agentName.replace(/'/g, "''");
  return `
    WITH video_conf_total AS (
      SELECT video_id, SUM(confidence) AS total_conf FROM bridge_video_agent GROUP BY video_id
    ),
    latest_fvd AS (
      SELECT fvd.video_id, fvd.view_count
      FROM fact_video_daily fvd
      JOIN (SELECT video_id, MAX(snapshot_date) AS max_date FROM fact_video_daily GROUP BY video_id) mx
        ON fvd.video_id = mx.video_id AND fvd.snapshot_date = mx.max_date
    )
    SELECT dc.channel_id, dc.channel_name,
      COALESCE(SUM(lf.view_count * bva.confidence / vct.total_conf), 0) AS total_views
    FROM dim_channel dc
    JOIN dim_video dv ON dc.channel_id = dv.channel_id
    JOIN bridge_video_agent bva ON dv.video_id = bva.video_id AND bva.agent_name = '${safe}'
    JOIN video_conf_total vct ON bva.video_id = vct.video_id
    LEFT JOIN latest_fvd lf ON dv.video_id = lf.video_id
    GROUP BY dc.channel_id, dc.channel_name
    ORDER BY total_views DESC
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