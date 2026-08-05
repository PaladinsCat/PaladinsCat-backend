import { sessionManager } from '../services/session-manager';
import { apiKeyPool } from '../services/api-key-pool';

async function testSessionCreation() {
  await apiKeyPool.init();
  const key = await apiKeyPool.getNext();
  const session = await sessionManager.acquireSession(key);
  console.log('Session creation test:');
  console.log(`  devId: ${session.devId}`);
  console.log(`  expiresAt: ${new Date(session.expiresAt).toISOString()}`);
  console.log(session.sessionKey ? 'PASS' : 'FAIL');
}

async function testSessionExpiry() {
  await apiKeyPool.init();
  const key = await apiKeyPool.getNext();
  const session = await sessionManager.acquireSession(key);
  const stored = await sessionManager.getSession(key.devId);
  console.log('Session retrieval test:');
  console.log(`  Retrieved: ${stored !== null}`);
  console.log(stored !== null ? 'PASS' : 'FAIL');
}

async function testSignatureGeneration() {
  const sig = sessionManager.sign('test', 'dev1', 'authKey', '20240101');
  console.log('Signature generation test:');
  console.log(`  Signature: ${sig}`);
  console.log(sig.length === 32 ? 'PASS' : 'FAIL');
}

async function main() {
  console.log('=== Session Manager Tests ===\n');
  await testSessionCreation();
  console.log();
  await testSessionExpiry();
  console.log();
  await testSignatureGeneration();
}

main().catch(console.error);
