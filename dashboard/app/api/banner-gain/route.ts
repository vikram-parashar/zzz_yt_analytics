import { NextRequest, NextResponse } from 'next/server';
import { fetchBannerGain } from '@/lib/fetch';
export async function GET(request: NextRequest) {
  try {
    const version = request.nextUrl.searchParams.get('version');
    if (!version) {
      return NextResponse.json({ error: 'version parameter required' }, { status: 400 });
    }
    const data = await fetchBannerGain(version);
    return NextResponse.json(data);
  } catch (error: any) {
    console.error('Banner gain API error:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}