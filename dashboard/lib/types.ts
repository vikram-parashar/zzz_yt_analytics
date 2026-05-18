export interface DashboardData {
  last_updated: string;
  pipeline_status: string;
  summary: {
    total_agents: number;
    total_videos: number;
    total_channels: number;
    total_facts: number;
  };
  top_agents: TopAgent[];
  daily_views_trend: DailyViews[];
}

export interface TopAgent {
  name: string;
  rank: string;
  attribute: string;
  total_views: number;
  total_likes: number;
  video_count: number;
}

export interface DailyViews {
  date: string;
  views: number;
}

