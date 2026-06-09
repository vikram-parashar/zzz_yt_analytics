import { NextRequest, NextResponse } from 'next/server';
import {
  fetchAgentVideoTimeline,
  fetchAgentEngagement,
  fetchAgentBanners,
  fetchAgentMostLiked,
  fetchAgentMostViewedOn,
  fetchAgentCoOccurring,
} from '@/lib/fetch';
export async function GET(request: NextRequest) {
  try {
    const agentName = request.nextUrl.searchParams.get('agentName');
    if (!agentName) {
      return NextResponse.json({ error: 'agentName parameter required' }, { status: 400 });
    }
    const startDate = request.nextUrl.searchParams.get('startDate') || '';
    const endDate = request.nextUrl.searchParams.get('endDate') || '';
    const [videoTimeline, engagement, banners, mostLiked, mostViewedOn, coOccurring] = await Promise.all([
      startDate && endDate
        ? fetchAgentVideoTimeline(agentName, startDate, endDate)
        : Promise.resolve([]),
      fetchAgentEngagement(agentName),
      fetchAgentBanners(agentName),
      fetchAgentMostLiked(agentName, 50),
      fetchAgentMostViewedOn(agentName),
      fetchAgentCoOccurring(agentName),
    ]);
    return NextResponse.json({
      videoTimeline,
      engagement,
      banners,
      mostLiked,
      mostViewedOn,
      coOccurring,
    });
  } catch (error: any) {
    console.error('Agent detail API error:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}