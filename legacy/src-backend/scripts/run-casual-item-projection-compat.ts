import { shutdown } from '../config/db';
import {
  upsertCasualItemProjectionForMatch,
} from '../services/casual-mechanics-projections';

async function main(): Promise<void> {
  const first = await upsertCasualItemProjectionForMatch(9_100_001);
  if (first !== 'projected') {
    throw new Error(`Expected projected, received ${first}`);
  }
  const replay = await upsertCasualItemProjectionForMatch(9_100_001);
  if (replay !== 'already_projected') {
    throw new Error(`Expected already_projected, received ${replay}`);
  }
  await upsertCasualItemProjectionForMatch(9_100_002)
    .then(() => {
      throw new Error('Ranked fixture match was incorrectly projected as casual');
    })
    .catch(error => {
      if (!String(error).includes('complete non-ranked canonical facts were not available')) {
        throw error;
      }
    });
}

void main()
  .finally(shutdown)
  .catch(error => {
    console.error(error);
    process.exitCode = 1;
  });
