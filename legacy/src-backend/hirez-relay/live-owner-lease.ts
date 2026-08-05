import type { PoolClient } from 'pg';
import { pool } from '../config/db';

const LOCK_NAME = process.env.HIREZ_RELAY_OWNER_LOCK || 'paladinscat:hirez-relay:live-owner';
let ownerClient: PoolClient | null = null;

export async function acquireLiveOwnerLease(): Promise<void> {
  if (ownerClient) return;
  const client = await pool.connect();
  try {
    const result = await client.query(
      'SELECT pg_try_advisory_lock(hashtext($1)) AS locked',
      [LOCK_NAME],
    );
    if (!result.rows[0]?.locked) {
      throw new Error('another HirezRelay process already owns the live provider lease');
    }
    ownerClient = client;
  } catch (error) {
    client.release();
    throw error;
  }
}

export async function liveOwnerHealthy(): Promise<boolean> {
  if (!ownerClient) return false;
  try {
    await ownerClient.query('SELECT 1');
    return true;
  } catch {
    return false;
  }
}

export async function releaseLiveOwnerLease(): Promise<void> {
  const client = ownerClient;
  ownerClient = null;
  if (!client) return;
  try {
    await client.query('SELECT pg_advisory_unlock(hashtext($1))', [LOCK_NAME]);
  } finally {
    client.release();
  }
}
