import { copyFile, mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const backendRoot = resolve(scriptDirectory, '..');
const source = resolve(
  backendRoot,
  'contracts',
  'hirez-relay-operation-contract.json',
);
const destination = resolve(
  backendRoot,
  'dist',
  'contracts',
  'hirez-relay-operation-contract.json',
);

await mkdir(dirname(destination), { recursive: true });
await copyFile(source, destination);
