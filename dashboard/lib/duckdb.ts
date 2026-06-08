'use client';
import * as duckdb from '@duckdb/duckdb-wasm';
import type { Table as ArrowTable } from 'apache-arrow';
import { useEffect, useState, useCallback, useRef } from 'react';
let db: duckdb.AsyncDuckDB | null = null;
let conn: duckdb.AsyncDuckDBConnection | null = null;
let initPromise: Promise<{ db: duckdb.AsyncDuckDB; conn: duckdb.AsyncDuckDBConnection }> | null = null;
const PARQUET_TABLES = [
  'dim_agent', 'dim_video', 'dim_channel', 'dim_patch',
  'bridge_video_agent', 'bridge_agent_alias',
  'fact_video_daily', 'fact_channel_daily', 'fact_agent_daily',
];
async function initDuckDB() {
  if (db && conn) return { db, conn };
  if (initPromise) return initPromise;
  initPromise = (async () => {
    const worker = new Worker('/duckdb-browser-eh.worker.js');
    const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
    const _db = new duckdb.AsyncDuckDB(logger, worker);
    await _db.instantiate('/duckdb-eh.wasm');
    const _conn = await _db.connect();
    for (const table of PARQUET_TABLES) {
      const url = `/data/${table}.parquet`;
      const response = await fetch(url);
      if (!response.ok) throw new Error(`Failed to fetch ${url}: ${response.status}`);
      const buffer = await response.arrayBuffer();
      await _db.registerFileBuffer(`${table}.parquet`, new Uint8Array(buffer));
      await _conn.query(`CREATE TABLE ${table} AS SELECT * FROM read_parquet('${table}.parquet')`);
    }
    db = _db;
    conn = _conn;
    return { db: _db, conn: _conn };
  })();
  return initPromise;
}
type QueryResult = ArrowTable<Record<string, any>>;
export interface DuckDBState {
  loading: boolean; error: string | null;
  query: (sql: string) => Promise<QueryResult>; ready: boolean;
}
export function useDuckDB(): DuckDBState {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [ready, setReady] = useState(false);
  const connRef = useRef<duckdb.AsyncDuckDBConnection | null>(null);
  useEffect(() => {
    if (typeof window === 'undefined') return;
    let cancelled = false;
    (async () => {
      try {
        const { conn: c } = await initDuckDB();
        if (!cancelled) { connRef.current = c; setReady(true); setLoading(false); }
      } catch (e: any) {
        if (!cancelled) { setError(e.message || 'DuckDB init failed'); setLoading(false); }
      }
    })();
    return () => { cancelled = true; };
  }, []);
  const query = useCallback(async (sql: string): Promise<QueryResult> => {
    if (!connRef.current) throw new Error('DuckDB not initialized');
    return await connRef.current.query(sql);
  }, []);
  return { loading, error, query, ready };
}
export function tableToArray<T = Record<string, any>>(table: QueryResult): T[] {
  const rows: T[] = [];
  for (const row of table) {
    const obj: Record<string, any> = {};
    for (const [key, value] of Object.entries(row)) {
      obj[key] = typeof value === 'bigint' ? Number(value) : value;
    }
    rows.push(obj as T);
  }
  return rows;
}