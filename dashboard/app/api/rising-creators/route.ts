import { NextRequest, NextResponse } from 'next/server';
import { fetchRisingCreators } from '@/lib/fetch';
export async function GET(request: NextRequest) {
  try {
    const timeRange = (request.nextUrl.searchParams.get('timeRange') as 'week' | 'month' | 'year') || 'month';
    const data = await fetchRisingCreators(timeRange);
    return NextResponse.json(data);
  } catch (error: any) {
    console.error('Rising creators API error:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}