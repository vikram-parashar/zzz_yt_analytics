import { query } from '@/lib/db';
export async function GET() {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    return Response.json({
      db: { connected: false, error: 'DATABASE_URL env var is not set' },
    });
  }
  try {
    const { rows, durationSec } = await query('SELECT * FROM dim_agent');
    return Response.json({
      db: { connected: true, agentCount: rows.length, durationSec },
      sample: rows.slice(0, 3),
    });
  } catch (err: any) {
    return Response.json({
      db: { connected: false, error: err?.message || String(err) },
    }, { status: 500 });
  }
}