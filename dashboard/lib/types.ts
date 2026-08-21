export interface AgentStats {
  name: string; img: string; rank: string; attribute: string;
  speciality: string; faction: string; release_date: string;
  video_count: number; total_views: number; total_likes: number;
  total_comments: number;
  on_banner: boolean;
}
export interface BannerAgentGain {
  agent_name: string; version: string; banner_agent: string;
  banner_start: string; banner_end: string; view_gain: number;
}
export interface RisingCreator {
  channel_id: string;
  channel_name: string;
  thumbnail: string | null;
  videos_collected: number | null;
  sub_growth: number | null;
  view_growth: number | null;
  score: number | null;
}
export interface AgentBannerPeriod {
  version: string; banner_start: string; banner_end: string;
}
export interface PatchEngagement {
  version: string; patch_agent: string; banner_start: string; banner_end: string;
  video_count: number; total_views: number; total_likes: number;
  total_daily_velocity: number;
}
export interface AgentMostViewedOn {
  channel_id: string; channel_name: string; total_views: number;
}
export interface MostLikedVideo {
  video_id: string; title: string; published_at: string;
  view_count: number; like_count: number; like_score: number;
}
export interface CoOccurringAgent {
  agent_name: string; co_video_count: number;
}
export type SortField = 'total_views' | 'video_count' | 'total_likes' | 'total_comments' | 'name' | 'rank';
export type SortDir = 'asc' | 'desc';