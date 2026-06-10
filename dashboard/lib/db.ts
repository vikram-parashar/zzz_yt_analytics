import { DuckDBInstance } from '@duckdb/node-api';
import path from 'path';
let instance: DuckDBInstance | null = null;
export interface QueryResult<T = Record<string, any>> {
  rows: T[];
  durationSec: number;
}
async function getInstance(): Promise<DuckDBInstance> {
  if (!instance) {
    const token = process.env.MOTHERDUCK_TOKEN;
    if (token) {
      instance = await DuckDBInstance.create('md:zzz_yt_analytics', {
        motherduck_token: token,
      });
    } else {
      const dbPath = path.join(process.cwd(), 'public', 'warehouse.db');
      instance = await DuckDBInstance.create(dbPath);
    }
  }
  return instance;
}
export async function query<T = Record<string, any>>(sql: string): Promise<QueryResult<T>> {
  const inst = await getInstance();
  const connection = await inst.connect();
  const start = performance.now();
  try {
    const result = await connection.run(sql);
    const rows = await result.getRowObjectsJS();
    const durationSec = Number(((performance.now() - start) / 1000).toFixed(3));
    return {
      rows: rows.map(row => {
        const obj: Record<string, any> = {};
        for (const [key, value] of Object.entries(row)) {
          obj[key] = typeof value === 'bigint' ? Number(value) : value;
        }
        return obj as T;
      }),
      durationSec,
    };
  } finally {
    connection.disconnectSync();
  }
}