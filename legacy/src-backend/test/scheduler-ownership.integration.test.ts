import assert from 'node:assert/strict';
import test from 'node:test';
import { query, shutdown } from '../config/db';
import {
  acquireTypescriptSchedulerOwnership,
  heartbeatTypescriptSchedulerOwnership,
  releaseTypescriptSchedulerOwnership,
} from '../services/scheduler-ownership';

const enabled = process.env.PALADINSCAT_TEST_SCHEDULER_OWNERSHIP === 'true';

test('TypeScript scheduler stops retaining a domain assigned to Rust', {
  skip: !enabled,
}, async (context) => {
  const schedulerKey = 'auto_ingester';
  context.after(async () => {
    await releaseTypescriptSchedulerOwnership(schedulerKey).catch(() => false);
    await query(
      `UPDATE worker_scheduler_assignments
       SET desired_engine = 'typescript',
           generation = generation + 1,
           updated_by = 'scheduler-integration-cleanup',
           updated_at = now()
       WHERE scheduler_key = $1`,
      [schedulerKey],
    );
    await shutdown();
  });

  await query(
    `UPDATE worker_scheduler_assignments
     SET desired_engine = 'typescript',
         generation = generation + 1,
         updated_by = 'scheduler-integration-setup',
         updated_at = now()
     WHERE scheduler_key = $1`,
    [schedulerKey],
  );
  assert.equal(
    await acquireTypescriptSchedulerOwnership(schedulerKey),
    true,
  );
  assert.equal(
    await heartbeatTypescriptSchedulerOwnership(schedulerKey),
    true,
  );

  await query(
    `UPDATE worker_scheduler_assignments
     SET desired_engine = 'rust',
         generation = generation + 1,
         updated_by = 'scheduler-integration-handoff',
         updated_at = now()
     WHERE scheduler_key = $1`,
    [schedulerKey],
  );
  assert.equal(
    await heartbeatTypescriptSchedulerOwnership(schedulerKey),
    false,
  );
});
