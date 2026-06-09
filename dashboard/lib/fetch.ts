import { query } from './db';
import {
  AGENT_STATS_QUERY,
  DIM_PATCH_QUERY,
  FACT_MIN_DATE_QUERY,
  topAgentsTimelineQuery,
  bannerAgentGainQuery,
  risingCreatorsQuery,
  agentVideoTimelineQuery,
  agentBannersQuery,
  agentEngagementTrendQuery,
  agentMostLikedVideoQuery,
  agentMostViewedOnQuery,
  agentCoOccurringQuery,
} from './queries';
import type {
  AgentStats,
  RisingCreator,
  AgentBannerPeriod,
  AgentMostViewedOn,
  MostLikedVideo,
  CoOccurringAgent,
} from './types';
export interface DimPatch {
  version: string;
  banner_agent: string;
  banner_start: string;
  banner_end: string;
}
export interface BannerGainRow {
  agent_name: string;
  view_gain: number | null;
}
export interface TimelineRow {
  agent_name: string;
  month: string;
  video_count: number;
}
export interface VideoTimelineRow {
  month: string;
  video_cnt: number;
}
export interface EngagementRow {
  date: string;
  views: number;
  likes: number;
}
export async function fetchAgentStats(): Promise<AgentStats[]> {
  return query<AgentStats>(AGENT_STATS_QUERY);
}
export async function fetchDimPatch(): Promise<DimPatch[]> {
  return query<DimPatch>(DIM_PATCH_QUERY);
}
export async function fetchFactMinDate(): Promise<string | null> {
  const rows = await query<{ mn: string | null }>(FACT_MIN_DATE_QUERY);
  return rows[0]?.mn ?? null;
}
export async function fetchTopAgentsTimeline(startDate: string, endDate: string): Promise<TimelineRow[]> {
  return query<TimelineRow>(topAgentsTimelineQuery(startDate, endDate));
}
export async function fetchBannerGain(selectedVersion: string): Promise<BannerGainRow[]> {
  return query<BannerGainRow>(bannerAgentGainQuery(selectedVersion));
}
export async function fetchRisingCreators(timeRange: 'week' | 'month' | 'year'): Promise<RisingCreator[]> {
  return query<RisingCreator>(risingCreatorsQuery(timeRange));
}
export async function fetchAgentVideoTimeline(agentName: string, startDate: string, endDate: string): Promise<VideoTimelineRow[]> {
  return query<VideoTimelineRow>(agentVideoTimelineQuery(agentName, startDate, endDate));
}
export async function fetchAgentBanners(agentName: string): Promise<AgentBannerPeriod[]> {
  return query<AgentBannerPeriod>(agentBannersQuery(agentName));
}
export async function fetchAgentEngagement(agentName: string): Promise<EngagementRow[]> {
  return query<EngagementRow>(agentEngagementTrendQuery(agentName));
}
export async function fetchAgentMostLiked(agentName: string, limit: number = 50): Promise<MostLikedVideo[]> {
  return query<MostLikedVideo>(agentMostLikedVideoQuery(agentName, limit));
}
export async function fetchAgentMostViewedOn(agentName: string): Promise<AgentMostViewedOn[]> {
  return query<AgentMostViewedOn>(agentMostViewedOnQuery(agentName));
}
export async function fetchAgentCoOccurring(agentName: string): Promise<CoOccurringAgent[]> {
  return query<CoOccurringAgent>(agentCoOccurringQuery(agentName));
}