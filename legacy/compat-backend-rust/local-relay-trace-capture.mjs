import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import http from 'node:http';

// Fixture-only relay for disposable runtime evidence. It never proxies an
// upstream, so a capture cannot accidentally contact Hi-Rez or production.
export async function startFixtureRelayCapture({ fixturePath, fixtureSha256, tracePath, runtime, scenario, runMarker }) {
  if (!/^[a-f0-9]{64}$/i.test(runMarker ?? '')) throw new Error('runMarker must be a cryptographic marker');
  const rawFixture = await fs.readFile(fixturePath);
  if (sha256(rawFixture) !== fixtureSha256) throw new Error('fixture hash mismatch');
  const fixture = JSON.parse(rawFixture.toString('utf8'));
  if (typeof fixture.id !== 'string' || !fixture.id) throw new Error('fixture id is required');
  if (!fixture.responses || typeof fixture.responses !== 'object') throw new Error('fixture responses are required');
  let sequence = 0;
  const server = http.createServer(async (request, response) => {
    if (request.method !== 'POST' || request.url !== '/v1/call') {
      response.writeHead(404).end();
      return;
    }
    const requestBody = await readBody(request);
    let payload;
    try {
      payload = JSON.parse(requestBody);
    } catch {
      response.writeHead(400, { 'content-type': 'application/json' }).end('{"ok":false,"error":"invalid JSON"}');
      return;
    }
    const operation = String(payload.operation ?? '');
    if (!Object.hasOwn(fixture.responses, operation)) {
      response.writeHead(400, { 'content-type': 'application/json' }).end(JSON.stringify({ ok: false, error: `fixture has no ${operation}` }));
      return;
    }
    const responseBody = JSON.stringify(fixture.responses[operation]);
    await append(tracePath, normalized('relay.request', requestBody));
    await append(tracePath, normalized('relay.response', responseBody, {
      fixtureId: fixture.id,
      fixtureResponseSha256: fixtureSha256,
    }));
    response.writeHead(200, { 'content-type': 'application/json' }).end(responseBody);
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('fixture relay did not bind TCP');
  return {
    url: `http://127.0.0.1:${address.port}`,
    fixture: { id: fixture.id, responseSha256: fixtureSha256 },
    close: () => new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve())),
  };

  function normalized(type, body, extra = {}) {
    return { runtime, scenario, runMarker, sequence: ++sequence, type, payload: { body, sha256: sha256(body), ...extra } };
  }
}

async function readBody(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  return Buffer.concat(chunks).toString('utf8');
}

function append(file, event) {
  return fs.appendFile(file, `${JSON.stringify(event)}\n`, 'utf8');
}

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}
