import crypto from 'node:crypto';
import { readFileSync } from 'node:fs';

const ALGORITHM = 'aes-256-gcm';
const IV_LENGTH = 12; // 96-bit IV for GCM
const AUTH_TAG_LENGTH = 16; // 128-bit auth tag
let cachedFileMEK: string | null = null;

function readMEKFile(): string | undefined {
  const mekFile = process.env.MEK_FILE;
  if (!mekFile) return undefined;
  if (cachedFileMEK !== null) return cachedFileMEK;

  try {
    const raw = readFileSync(mekFile, 'utf8').trim();
    // Local Docker stores secrets/mek.txt as raw 64-char hex. Accepting
    // MEK=<hex> too keeps the same helper compatible with env-file style
    // secret mounts later.
    cachedFileMEK = raw.startsWith('MEK=') ? raw.slice(4).trim() : raw;
    return cachedFileMEK;
  } catch {
    cachedFileMEK = '';
    return undefined;
  }
}

function getMEK(): string | undefined {
  return process.env.MEK || readMEKFile();
}

/**
 * Validate the MEK: must be exactly 64 hex characters (32 bytes).
 */
export function validateMEK(): boolean {
  const mek = getMEK();
  if (!mek) return false;
  return /^[0-9a-f]{64}$/i.test(mek);
}

/**
 * Encrypt a plaintext string using AES-256-GCM.
 * Returns base64(IV + ciphertext + auth_tag).
 */
export function encrypt(plaintext: string): string {
  if (!validateMEK()) {
    throw new Error('MEK is not set or invalid');
  }
  const iv = crypto.randomBytes(IV_LENGTH);
  const key = Buffer.from(getMEK()!, 'hex');
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);
  const encrypted = Buffer.concat([
    cipher.update(plaintext, 'utf8'),
    cipher.final(),
  ]);
  const authTag = cipher.getAuthTag();
  const result = Buffer.concat([iv, encrypted, authTag]);
  return result.toString('base64');
}

/**
 * Decrypt a base64(IV + ciphertext + auth_tag) string using AES-256-GCM.
 * Returns plaintext.
 *
 * Fixed 2026-05-31:
 * - Input type guard: rejects non-string input (null, undefined, object) before
 *   Buffer.from() produces cryptic runtime errors.
 * - Buffer.concat: replaces string concatenation of decipher.update() + final().
 *   String concat truncates null bytes and corrupts binary payloads. Using
 *   Buffer.concat preserves all bytes, then converts to UTF-8 once at the end.
 *   Source: Fault #1 — "Binary data corruption on decrypt"
 * - Try-catch: wraps all crypto operations. Tampered ciphertext (bad IV, wrong
 *   auth tag, corrupted payload) throws from Node.js crypto internals with
 *   unhandled exceptions → 500 crash. Now returns a meaningful error message.
 *   Source: Fault #2 — "No error handling on decrypt"
 */
export function decrypt(ciphertextB64: string): string {
  // Input type guard — prevents cryptic Buffer.from() errors on null/undefined/object
  if (typeof ciphertextB64 !== 'string' || !ciphertextB64) {
    throw new Error('Invalid ciphertext: expected non-empty base64 string');
  }
  if (!validateMEK()) {
    throw new Error('MEK is not set or invalid');
  }
  try {
    const data = Buffer.from(ciphertextB64, 'base64');
    if (data.length < IV_LENGTH + AUTH_TAG_LENGTH) {
      throw new Error('Ciphertext too short');
    }
    const iv = data.subarray(0, IV_LENGTH);
    const authTag = data.subarray(data.length - AUTH_TAG_LENGTH);
    const ciphertext = data.subarray(IV_LENGTH, data.length - AUTH_TAG_LENGTH);
    const key = Buffer.from(getMEK()!, 'hex');
    const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);
    decipher.setAuthTag(authTag);
    // Buffer.concat preserves all bytes — string concat truncates null bytes
    const decrypted = Buffer.concat([
      decipher.update(ciphertext),
      decipher.final(),
    ]);
    return decrypted.toString('utf8');
  } catch (err: any) {
    // Crypto errors: bad auth tag, corrupted payload, wrong key — all return
    // a clean error rather than an unhandled Node.js exception
    throw new Error(`Decryption failed: ${err.message}`);
  }
}

/**
 * Quick smoke test: encrypt and decrypt a known value.
 */
export function smokeTest(): boolean {
  const test = 'smoke-test-value';
  const encrypted = encrypt(test);
  const decrypted = decrypt(encrypted);
  return decrypted === test;
}
