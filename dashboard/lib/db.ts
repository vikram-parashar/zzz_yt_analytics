'use server'
import { Pool, PoolConfig } from 'pg';
let pool: Pool | null = null;
export interface QueryResult<T = Record<string, any>> {
  rows: T[];
  durationSec: number;
}
function getPoolConfig(): PoolConfig {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error('DATABASE_URL environment variable is not set');
  }
  return {
    connectionString: databaseUrl,
    ssl:
      process.env.DATABASE_SSL === 'false'
        ? false
        : { rejectUnauthorized: false },
    max: 5,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 10_000,
  };
}
function getPool(): Pool {
  if (!pool) {
    pool = new Pool(getPoolConfig());
    pool.on('error', (err) => {
      console.error('Unexpected pg pool error:', err.message);
    });
  }
  return pool;
}
export async function query<T = Record<string, any>>(
  sql: string,
  params?: any[]
): Promise<QueryResult<T>> {
  const start = performance.now();
  try {
    const result = await getPool().query(sql, params);
    const durationSec = Number(
      ((performance.now() - start) / 1000).toFixed(3)
    );
    const rows = result.rows.map((row: Record<string, any>) => {
      const obj: Record<string, any> = {};
      for (const [key, value] of Object.entries(row)) {
        obj[key] = typeof value === 'bigint' ? Number(value) : value;
      }
      return obj as T;
    });
    return { rows, durationSec };
  } catch (err: any) {
    const msg = err?.message || String(err);
    if (
      msg.includes('ECONNREFUSED') ||
      msg.includes('ENOTFOUND') ||
      msg.includes('ENOENT') ||
      msg.includes('connection refused') ||
      msg.includes('does not exist')
    ) {
      console.error(
        `PostgreSQL connection failed: ${msg}. ` +
        `Check your DATABASE_URL environment variable.`
      );
    }
    throw err;
  }
}
export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}