import { DuckDBInstance } from '@duckdb/node-api';
let instance: DuckDBInstance | null = null;
async function getInstance(): Promise<DuckDBInstance> {
  if (!instance) {
    const token = process.env.MOTHERDUCK_TOKEN;
    if (!token) {
      throw new Error('MOTHERDUCK_TOKEN environment variable is required');
    }
    instance = await DuckDBInstance.create('md:zzz_yt_analytics', {
      motherduck_token: token,
    });
  }
  return instance;
}
export async function query<T = Record<string, any>>(sql: string): Promise<T[]> {
  const inst = await getInstance();
  const connection = await inst.connect();
  try {
    const result = await connection.run(sql);
    const rows = await result.getRowObjectsJS();
    return rows.map(row => {
      const obj: Record<string, any> = {};
      for (const [key, value] of Object.entries(row)) {
        obj[key] = typeof value === 'bigint' ? Number(value) : value;
      }
      return obj as T;
    });
  } finally {
    connection.disconnectSync();
  }
}