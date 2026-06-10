import { NextRequest, NextResponse } from 'next/server';
import { fetchTopAgentsTimeline } from '@/lib/fetch';
export async function GET(request: NextRequest) {
  try {
    const startDate = request.nextUrl.searchParams.get('startDate');
    const endDate = request.nextUrl.searchParams.get('endDate');
    if (!startDate || !endDate) {
      return NextResponse.json({ error: 'startDate and endDate parameters required' }, { status: 400 });
    }
    const { data, durationSec } = await fetchTopAgentsTimeline(startDate, endDate);
    return NextResponse.json({ data, queryTime: durationSec });
  } catch (error: any) {
    console.error('Agent timeline API error:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}