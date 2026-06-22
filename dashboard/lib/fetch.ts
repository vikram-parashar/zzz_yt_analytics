import { query } from './db';
import {
  AGENT_STATS_QUERY,
  AGENT_NAMES_QUERY,
  DIM_PATCH_QUERY,
  FACT_MIN_DATE_QUERY,
  agentLookupQuery,
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
export interface FetchResult<T> {
  data: T;
  durationSec: number;
}
export async function fetchAgentStats(): Promise<FetchResult<AgentStats[]>> {
  const { rows, durationSec } = await query<AgentStats>(AGENT_STATS_QUERY);
  return { data: rows, durationSec };
}
export async function fetchAgentLookup(agentName: string): Promise<FetchResult<AgentStats | null>> {
  const { rows, durationSec } = await query<AgentStats>(agentLookupQuery(agentName));
  return { data: rows[0] ?? null, durationSec };
}
export interface AgentNameRow {
  name: string;
  img: string;
  rank: string;
  attribute: string;
  speciality: string;
  faction: string;
}
export async function fetchAgentNames(): Promise<FetchResult<AgentNameRow[]>> {
  const { rows, durationSec } = await query<AgentNameRow>(AGENT_NAMES_QUERY);
  return { data: rows, durationSec };
}
export async function fetchDimPatch(): Promise<FetchResult<DimPatch[]>> {
  const { rows, durationSec } = await query<DimPatch>(DIM_PATCH_QUERY);
  return { data: rows, durationSec };
}
export async function fetchFactMinDate(): Promise<FetchResult<string | null>> {
  const { rows, durationSec } = await query<{ mn: string | null }>(FACT_MIN_DATE_QUERY);
  return { data: rows[0]?.mn ?? null, durationSec };
}
export async function fetchTopAgentsTimeline(startDate: string, endDate: string): Promise<FetchResult<TimelineRow[]>> {
  const { rows, durationSec } = await query<TimelineRow>(topAgentsTimelineQuery(startDate, endDate));
  return { data: rows, durationSec };
}
export async function fetchBannerGain( bannerStart: string, bannerEnd: string): Promise<FetchResult<BannerGainRow[]>> {
  console.log(bannerStart,bannerEnd,'dfak')
  const { rows, durationSec } = await query<BannerGainRow>(bannerAgentGainQuery(bannerStart, bannerEnd));
  return { data: rows, durationSec };
}
export async function fetchRisingCreators(timeRange: 'week' | 'month' | 'year'): Promise<FetchResult<RisingCreator[]>> {
  const { rows, durationSec } = await query<RisingCreator>(risingCreatorsQuery(timeRange));
  return { data: rows, durationSec };
}
export async function fetchAgentVideoTimeline(agentName: string, startDate: string, endDate: string): Promise<FetchResult<VideoTimelineRow[]>> {
  const { rows, durationSec } = await query<VideoTimelineRow>(agentVideoTimelineQuery(agentName, startDate, endDate));
  return { data: rows, durationSec };
}
export async function fetchAgentBanners(agentName: string): Promise<FetchResult<AgentBannerPeriod[]>> {
  const { rows, durationSec } = await query<AgentBannerPeriod>(agentBannersQuery(agentName));
  return { data: rows, durationSec };
}
export async function fetchAgentEngagement(agentName: string): Promise<FetchResult<EngagementRow[]>> {
  const { rows, durationSec } = await query<EngagementRow>(agentEngagementTrendQuery(agentName));
  return { data: rows, durationSec };
}
export async function fetchAgentMostLiked(agentName: string): Promise<FetchResult<MostLikedVideo[]>> {
  const { rows, durationSec } = await query<MostLikedVideo>(agentMostLikedVideoQuery(agentName));
  return { data: rows, durationSec };
}
export async function fetchAgentMostViewedOn(agentName: string): Promise<FetchResult<AgentMostViewedOn[]>> {
  const { rows, durationSec } = await query<AgentMostViewedOn>(agentMostViewedOnQuery(agentName));
  return { data: rows, durationSec };
}
export async function fetchAgentCoOccurring(agentName: string): Promise<FetchResult<CoOccurringAgent[]>> {
  const { rows, durationSec } = await query<CoOccurringAgent>(agentCoOccurringQuery(agentName));
  return { data: rows, durationSec };
}