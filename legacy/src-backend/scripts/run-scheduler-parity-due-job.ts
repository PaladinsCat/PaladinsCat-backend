// Local compatibility evidence only. This command is deliberately not wired
// into production startup or an operator command.
if (process.env.PALADINSCAT_SCHEDULER_CAPTURE_ENABLE !== 'true') {
  throw new Error('scheduler parity capture is disabled; set PALADINSCAT_SCHEDULER_CAPTURE_ENABLE=true');
}
if (!process.env.PALADINSCAT_SCHEDULER_CAPTURE_NOW) {
  throw new Error('scheduler parity capture requires PALADINSCAT_SCHEDULER_CAPTURE_NOW');
}

if (process.argv.slice(2).join(' ') !== 'auto_ingester discovery') {
  throw new Error('usage: run-scheduler-parity-due-job.ts auto_ingester discovery');
}

async function main(): Promise<void> {
  const { runScheduledDiscoveryDueJobForParity } = await import('../workers/auto-ingester-scheduler.js');
  await runScheduledDiscoveryDueJobForParity();
}

void main();
