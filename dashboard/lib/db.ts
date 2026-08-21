import { Pool, PoolConfig } from 'pg';
let pool: Pool | null = null;
export interface QueryResult<T = Record<string, any>> {
  rows: T[];
  durationSec: number;
}
function getPoolConfig(): PoolConfig {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error(
      'DATABASE_URL environment variable is not set.\n' +
      'This dashboard connects to PostgreSQL (or a PG-compatible endpoint like MotherDuck PG proxy).\n' +
      'Set it in your .env.local or deployment environment, e.g.:\n' +
      '  DATABASE_URL=postgresql://user:pass@host:5432/dbname\n' +
      '  DATABASE_SSL=false  (set to "false" to disable SSL)'
    );
  }
  try {
    const url = new URL(databaseUrl);
    if (url.password) url.password = '****';
    console.log(`[db] Connecting to: ${url.toString()}`);
  } catch {
    console.log(`[db] DATABASE_URL is set (could not parse for logging)`);
  }
  const ssl = process.env.DATABASE_SSL === 'false'
    ? false
    : { rejectUnauthorized: false };
  console.log(`[db] SSL enabled: ${ssl !== false}`);
  return {
    connectionString: databaseUrl,
    ssl,
    max: 5,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 30_000,
  };
}
function getPool(): Pool {
  if (!pool) {
    pool = new Pool(getPoolConfig());
    pool.on('error', (err) => {
      console.error('[db] Unexpected pool error:', err.message);
      pool = null;
    });
  }
  return pool;
}
export async function query<T = Record<string, any>>(
  sql: string,
  params?: any[]
): Promise<QueryResult<T>> {
  const start = performance.now();
  let lastErr: any;
  for (let attempt = 1; attempt <= 2; attempt++) {
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
      lastErr = err;
      const msg = err?.message || String(err);
      const isConnectionError =
        msg.includes('ECONNREFUSED') ||
        msg.includes('ENOTFOUND') ||
        msg.includes('ENOENT') ||
        msg.includes('connection refused') ||
        msg.includes('does not exist') ||
        msg.includes('Connection terminated') ||
        msg.includes('timeout') ||
        msg.includes('TIMEDOUT') ||
        msg.includes('ETIMEDOUT') ||
        msg.includes('connect ETIMEDOUT') ||
        msg.includes('socket hang up');
      if (isConnectionError) {
        console.error(
          `[db] Connection error (attempt ${attempt}/2): ${msg}`
        );
        if (pool) {
          try { pool.end(); } catch {  }
          pool = null;
        }
        if (attempt < 2) {
          await new Promise(r => setTimeout(r, 500));
          continue;
        }
        throw new Error(
          `PostgreSQL connection failed after 2 attempts: ${msg}\n\n` +
          `Troubleshooting:\n` +
          `1. Check DATABASE_URL is correct (host, port, credentials).\n` +
          `2. If using MotherDuck, ensure you're using the PG proxy URL (not the DuckDB native URL).\n` +
          `3. Ensure the database host is reachable (firewall / VPC / IP allowlist).\n` +
          `4. Try setting DATABASE_SSL=false if SSL is not required.\n` +
          `5. If connecting locally, ensure PostgreSQL is running on the specified port.`
        );
      }
      throw err;
    }
  }
  throw lastErr;
}
export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}