import { Pool, PoolClient } from 'pg';
import dotenv from 'dotenv';
import crypto from 'node:crypto';

dotenv.config();

const requiredEnv = ['DATABASE_URL'];
for (const key of requiredEnv) {
  if (!process.env[key]) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
}

/**
 * Default database statement timeout (ms).
 *
 * Frontend fetches already abort after their own timeout, but that only closes
 * the HTTP side. PostgreSQL will keep executing the SQL unless the server-side
 * session also has a statement timeout. Without this, one expensive dashboard
 * query can survive multiple client retries, consume the whole pool, and make
 * unrelated endpoints look dead. Keep this above the pool construction so every
 * connection gets the setting at creation time.
 */
const QUERY_TIMEOUT_MS = 30000;
export const DB_POOL_MAX = Math.min(Math.max(Number(process.env.DB_POOL_MAX)||20,1),50);
const SLOW_QUERY_MS = Math.max(Number(process.env.SLOW_QUERY_MS)||500,50);
const DB_APPLICATION_NAME = String(process.env.DB_APPLICATION_NAME || 'paladinscat').trim() || 'paladinscat';

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  application_name: DB_APPLICATION_NAME,
  max: DB_POOL_MAX,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  statement_timeout: QUERY_TIMEOUT_MS,
  query_timeout: QUERY_TIMEOUT_MS,
});

pool.on('error', (err) => {
  console.error('Unexpected error on idle client', err);
});

export async function healthCheck(): Promise<boolean> {
  try {
    const client = await pool.connect();
    await client.query('SELECT 1');
    client.release();
    return true;
  } catch {
    return false;
  }
}

export async function query<T = any>(text: string, params?: any[]): Promise<T[]> {
  const started=performance.now();
  try{
    const res=await pool.query(text,params) as any;
    // node-postgres returns QueryResult[] for a multi-statement command. Schema
    // guards and migrations legitimately use that form, so observability must
    // not assume a single result has a top-level `rows` array.
    const results:any[]=Array.isArray(res)?res:[res];
    const rows:T[]=results.flatMap((result)=>Array.isArray(result?.rows)?result.rows:[]);
    reportSlowQuery(text,started,results.reduce((total,result)=>total+(result?.rowCount??result?.rows?.length??0),0));
    return rows;
  }catch(error){
    reportSlowQuery(text,started,0,true);
    throw error;
  }
}

export async function one<T = any>(text: string, params?: any[]): Promise<T | null> {
  const started=performance.now();
  try{
    const res=await pool.query(text,params) as any;
    const results:any[]=Array.isArray(res)?res:[res];
    const rows:T[]=results.flatMap((result)=>Array.isArray(result?.rows)?result.rows:[]);
    reportSlowQuery(text,started,results.reduce((total,result)=>total+(result?.rowCount??result?.rows?.length??0),0));
    return rows[0] || null;
  }catch(error){
    reportSlowQuery(text,started,0,true);
    throw error;
  }
}

function reportSlowQuery(text:string,started:number,rowCount:number,failed=false):void{
  const durationMs=Math.round((performance.now()-started)*10)/10;
  if(durationMs<SLOW_QUERY_MS&&!failed)return;
  const fingerprint=crypto.createHash('sha256').update(text.replace(/\s+/g,' ').trim()).digest('hex').slice(0,12);
  console.warn('[database-query]',{
    fingerprint,durationMs,rowCount,failed,
    pool:{total:pool.totalCount,idle:pool.idleCount,waiting:pool.waitingCount,max:DB_POOL_MAX},
  });
}

/**
 * Execute a function within a database transaction.
 * Automatically handles BEGIN, COMMIT, and ROLLBACK on error.
 * The client is returned to the pool after completion.
 *
 * @param fn - Async function receiving a PoolClient for queries.
 * @returns The result of the function.
 */
export async function transaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    // CRITICAL: Wrap ROLLBACK in try-catch. If the connection is dead
    // (network drop, server restart), ROLLBACK throws and prevents
    // client.release() in finally from running → connection permanently
    // lost from the pool. Under repeated failures, the pool exhausts
    // and the entire backend hangs on pool.connect().
    // Source: Fault #1 — "Connection leak on ROLLBACK failure"
    try {
      await client.query('ROLLBACK');
    } catch {
      // Connection is dead — release will also fail, but we suppress it
    }
    throw err;
  } finally {
    // CRITICAL: release() can throw if the connection is already dead.
    // Suppress that error so the original error propagates cleanly.
    try {
      client.release();
    } catch {
      // Connection is dead — nothing we can do
    }
  }
}

export async function shutdown() {
  await pool.end();
}
