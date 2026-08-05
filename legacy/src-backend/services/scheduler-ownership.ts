import { randomUUID } from 'node:crypto';
import { hostname } from 'node:os';
import { query } from '../config/db';

const SCHEDULER_LEASE_SECONDS = 60;

export const TYPESCRIPT_SCHEDULER_OWNER_ID = [
  'typescript',
  hostname(),
  process.pid,
  randomUUID(),
].join(':');

export async function acquireTypescriptSchedulerOwnership(
  schedulerKey: string,
): Promise<boolean> {
  const rows = await query<{ scheduler_key: string }>(
    `INSERT INTO worker_scheduler_ownership (
       scheduler_key, owner_id, engine, lease_until,
       acquired_at, heartbeat_at, updated_at
     )
     SELECT
       $1::varchar(64), $2::varchar(120), 'typescript',
       now() + ($3::int * interval '1 second'),
       now(), now(), now()
     WHERE EXISTS (
       SELECT 1
       FROM worker_scheduler_assignments assignment
       WHERE assignment.scheduler_key = $1::varchar(64)
         AND assignment.desired_engine = 'typescript'
     )
     ON CONFLICT (scheduler_key) DO UPDATE SET
       owner_id = EXCLUDED.owner_id,
       engine = EXCLUDED.engine,
       lease_until = EXCLUDED.lease_until,
       acquired_at = CASE
         WHEN worker_scheduler_ownership.owner_id = EXCLUDED.owner_id
           THEN worker_scheduler_ownership.acquired_at
         ELSE now()
       END,
       heartbeat_at = now(),
       updated_at = now()
     WHERE (
         worker_scheduler_ownership.lease_until <= now()
         OR worker_scheduler_ownership.owner_id = EXCLUDED.owner_id
       )
       AND EXISTS (
         SELECT 1
         FROM worker_scheduler_assignments assignment
         WHERE assignment.scheduler_key = EXCLUDED.scheduler_key
           AND assignment.desired_engine = 'typescript'
       )
     RETURNING scheduler_key`,
    [schedulerKey, TYPESCRIPT_SCHEDULER_OWNER_ID, SCHEDULER_LEASE_SECONDS],
  );
  return rows.length === 1;
}

export async function heartbeatTypescriptSchedulerOwnership(
  schedulerKey: string,
): Promise<boolean> {
  const rows = await query<{ scheduler_key: string }>(
    `UPDATE worker_scheduler_ownership ownership
     SET lease_until = now() + ($3::int * interval '1 second'),
         heartbeat_at = now(),
         updated_at = now()
     WHERE ownership.scheduler_key = $1::varchar(64)
       AND ownership.owner_id = $2::varchar(120)
       AND ownership.engine = 'typescript'
       AND ownership.lease_until > now()
       AND EXISTS (
         SELECT 1
         FROM worker_scheduler_assignments assignment
         WHERE assignment.scheduler_key = ownership.scheduler_key
           AND assignment.desired_engine = 'typescript'
       )
     RETURNING ownership.scheduler_key`,
    [schedulerKey, TYPESCRIPT_SCHEDULER_OWNER_ID, SCHEDULER_LEASE_SECONDS],
  );
  return rows.length === 1;
}

export async function releaseTypescriptSchedulerOwnership(
  schedulerKey: string,
): Promise<boolean> {
  const rows = await query<{ scheduler_key: string }>(
    `DELETE FROM worker_scheduler_ownership
     WHERE scheduler_key = $1::varchar(64)
       AND owner_id = $2::varchar(120)
       AND engine = 'typescript'
     RETURNING scheduler_key`,
    [schedulerKey, TYPESCRIPT_SCHEDULER_OWNER_ID],
  );
  return rows.length === 1;
}
