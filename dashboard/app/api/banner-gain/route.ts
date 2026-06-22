import { NextRequest, NextResponse } from 'next/server';
import { fetchBannerGain } from '@/lib/fetch';
export async function GET(request: NextRequest) {
  try {
    const start = request.nextUrl.searchParams.get('start');
    const end = request.nextUrl.searchParams.get('end');
    if (!start || !end) {
      return NextResponse.json(
        { error: 'start and end parameters are required' },
        { status: 400 }
      );
    }
    const { data, durationSec } = await fetchBannerGain(start, end);
    return NextResponse.json({
      data,
      queryTime: durationSec,
    });
  } catch (error: any) {
    console.error('Banner gain API error:', error);
    return NextResponse.json(
      { error: error.message ?? 'Internal server error' },
      { status: 500 }
    );
  }
}