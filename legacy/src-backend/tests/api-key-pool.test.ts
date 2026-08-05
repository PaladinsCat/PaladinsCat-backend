import { apiKeyPool, BUDGET_THRESHOLD } from '../services/api-key-pool';

/**
 * Test: Sticky key behavior — waterfall returns the SAME key on consecutive
 * calls until its budget drops below BUDGET_THRESHOLD (100 remaining).
 *
 * Replaces the old testRoundRobin which expected different keys on each call.
 * Waterfall architecture uses a "sticky" activeDevId — it sticks to one key
 * until exhaustion, then falls to the next healthy key. This prevents the
 * "sorting trap" where keys oscillate between calls on every API request.
 *
 * Verify: Two consecutive getActiveKey() calls return the same devId.
 */
async function testStickyKey() {
  await apiKeyPool.init();
  const k1 = await apiKeyPool.getNext();
  const k2 = await apiKeyPool.getNext();
  console.log('Sticky key test: keys should be the same (waterfall sticks to active key)');
  console.log(`Key 1: ${k1.devId}, Key 2: ${k2.devId}`);
  console.log(k1.devId === k2.devId ? 'PASS' : 'FAIL (waterfall should return same sticky key)');
}

/**
 * Test: Health check — 5 consecutive failures mark a key as 'unhealthy'.
 * Once unhealthy, getActiveKey waterfall selection skips the key.
 *
 * Verify: After 5 recordFailure calls, key status is 'unhealthy'.
 */
async function testHealthCheck() {
  await apiKeyPool.init();
  const k = await apiKeyPool.getNext();
  await apiKeyPool.recordFailure(k.devId);
  await apiKeyPool.recordFailure(k.devId);
  await apiKeyPool.recordFailure(k.devId);
  await apiKeyPool.recordFailure(k.devId);
  await apiKeyPool.recordFailure(k.devId);
  const status = await apiKeyPool.getStatus();
  const unhealthy = status.find((s: any) => s.devId === k.devId);
  console.log(`Health check test: key ${k.devId} status = ${unhealthy?.status}`);
  console.log(unhealthy?.status === 'unhealthy' ? 'PASS' : 'FAIL');
}

/**
 * Test: Daily cap — incrementing usedToday past (dailyLimit - BUDGET_THRESHOLD)
 * causes the key to be marked 'limited' and waterfall falls to the next key.
 *
 * Uses incrementUsage() which actually increments usedToday in memory.
 * The old test called recordSuccess() 10,000 times — recordSuccess only
 * resets consecutiveFailures and recovers unhealthy status, it does NOT
 * affect usedToday or budget tracking. So the old test never triggered
 * the budget exhaustion logic and always returned the same key.
 *
 * Verify: After incrementing usedToday past threshold, next key differs.
 */
async function testDailyCap() {
  await apiKeyPool.init();
  const k = await apiKeyPool.getNext();
  // Increment usage in memory to exhaust the budget.
  // BUDGET_THRESHOLD = 100, so when usedToday > dailyLimit - 100,
  // getActiveKey marks this key 'limited' and falls to the next healthy key.
  // Increment one call beyond the usable ceiling to trigger rotation.
  const threshold = k.daily_limit - BUDGET_THRESHOLD;
  for (let i = 0; i < threshold + 1; i++) {
    apiKeyPool.increment(k.devId);
  }
  const next = await apiKeyPool.getNext();
  console.log(`Daily cap test: next key after exhausting ${k.devId} is ${next.devId}`);
  console.log(next.devId !== k.devId ? 'PASS' : 'FAIL (should have fallen to next key)');
}

/**
 * Test: Unhealthy revival — syncUsage revives 'unhealthy' keys when
 * calls age out of the rolling 24h window. This tests the revival logic
 * added in Phase 2 that extends syncUsage to also recover unhealthy keys.
 *
 * Note: This test requires a live Hi-Rez API connection for syncUsage
 * to call getDataUsed. If the API is unavailable, syncUsage silently
 * catches the error and the test may not trigger revival.
 *
 * Verify: Key marked unhealthy, then syncUsage revives if budget allows.
 */
async function testUnhealthyRevival() {
  await apiKeyPool.init();
  const keys = apiKeyPool.getStatus();
  if (keys.length === 0) {
    console.log('Unhealthy revival test: SKIP (no keys available)');
    return;
  }
  // Pick a key and mark it unhealthy via 5 failures
  const k = keys[0];
  for (let i = 0; i < 5; i++) {
    await apiKeyPool.recordFailure(k.devId);
  }
  let status = apiKeyPool.getStatus();
  const before = status.find((s: any) => s.devId === k.devId);
  console.log(`Unhealthy revival test: before sync, ${k.devId} status = ${before?.status}`);

  // Sync with Hi-Rez — if daily budget has reset, the key should revive
  await apiKeyPool.syncUsage(k.devId);

  status = apiKeyPool.getStatus();
  const after = status.find((s: any) => s.devId === k.devId);
  console.log(`Unhealthy revival test: after sync, ${k.devId} status = ${after?.status}`);
  // Result depends on actual Hi-Rez budget — may or may not revive
  console.log(after?.status === 'healthy' ? 'PASS (revived)' : 'INFO (still penalized — budget may not have reset)');
}

async function main() {
  console.log('=== API Key Pool Tests ===\n');
  await testStickyKey();
  console.log();
  await testHealthCheck();
  console.log();
  await testDailyCap();
  console.log();
  await testUnhealthyRevival();
}

main().catch(console.error);
