import { DuckDBInstance, DuckDBConnection } from '@duckdb/node-api';
import path from 'path';
import fs from 'fs';
const DB_PATH = path.join(process.cwd(), 'data', 'warehouse.duckdb');
let instance: DuckDBInstance | null = null;
async function getInstance(): Promise<DuckDBInstance> {
  if (!instance) {
    const dataDir = path.dirname(DB_PATH);
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }
    if (!fs.existsSync(DB_PATH)) {
      const tempInstance = await DuckDBInstance.create(DB_PATH);
      tempInstance.closeSync();
    }
    instance = await DuckDBInstance.create(DB_PATH, { access_mode: 'READ_ONLY' });
  }
  return instance;
}
export async function query<T = Record<string, any>>(sql: string): Promise<T[]> {
  const inst = await getInstance();
  const connection = await inst.connect();
  try {
    const result = await connection.run(sql);
    const rows = await result.getRowObjectsJS();
    const sanitized = rows.map(row => {
      const obj: Record<string, any> = {};
      for (const [key, value] of Object.entries(row)) {
        obj[key] = typeof value === 'bigint' ? Number(value) : value;
      }
      return obj as T;
    });
    return sanitized;
  } finally {
    connection.disconnectSync();
  }
}