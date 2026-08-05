const TWITCH_TOKEN_URL = 'https://id.twitch.tv/oauth2/token';
const TWITCH_API_URL = 'https://api.twitch.tv/helix';
const PALADINS_CATEGORY_NAME = 'Paladins';
const STREAM_CACHE_MS = 60_000;
const HIDDEN_CHANNEL_LOGINS = new Set(['paladins2ttv']);

type AccessToken = {
  value: string;
  expiresAt: number;
};

type TwitchGameResponse = {
  data: Array<{ id: string }>;
};

type TwitchStreamsResponse = {
  data: Array<{
    user_login: string;
    user_name: string;
    title: string;
    viewer_count: number;
    language: string;
    thumbnail_url: string;
    tags: string[];
  }>;
};

export type TwitchStream = {
  userLogin: string;
  userName: string;
  title: string;
  viewerCount: number;
  language: string;
  thumbnailUrl: string;
  tags: string[];
  url: string;
};

type TwitchStreamResult = {
  configured: boolean;
  streams: TwitchStream[];
};

let accessToken: AccessToken | null = null;
let paladinsCategoryId: string | null = null;
let streamCache: { expiresAt: number; result: TwitchStreamResult } | null = null;

function credentials() {
  const clientId = process.env.TWITCH_CLIENT_ID?.trim();
  const clientSecret = process.env.TWITCH_CLIENT_SECRET?.trim();
  return clientId && clientSecret ? { clientId, clientSecret } : null;
}

async function getAccessToken(clientId: string, clientSecret: string): Promise<string> {
  if (accessToken && accessToken.expiresAt > Date.now()) return accessToken.value;

  const response = await fetch(TWITCH_TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: clientId,
      client_secret: clientSecret,
      grant_type: 'client_credentials',
    }),
    signal: AbortSignal.timeout(8_000),
  });
  if (!response.ok) throw new Error(`Twitch token request failed (${response.status})`);

  const payload = await response.json() as { access_token?: string; expires_in?: number };
  if (!payload.access_token || !payload.expires_in) throw new Error('Twitch token response was incomplete');

  accessToken = {
    value: payload.access_token,
    expiresAt: Date.now() + Math.max(0, payload.expires_in - 60) * 1_000,
  };
  return accessToken.value;
}

async function helixGet<T>(path: string, clientId: string, clientSecret: string): Promise<T> {
  const token = await getAccessToken(clientId, clientSecret);
  const response = await fetch(`${TWITCH_API_URL}${path}`, {
    headers: {
      Authorization: `Bearer ${token}`,
      'Client-Id': clientId,
    },
    signal: AbortSignal.timeout(8_000),
  });

  if (response.status === 401) accessToken = null;
  if (!response.ok) throw new Error(`Twitch API request failed (${response.status})`);
  return response.json() as Promise<T>;
}

async function getPaladinsCategoryId(clientId: string, clientSecret: string): Promise<string | null> {
  if (paladinsCategoryId) return paladinsCategoryId;
  const response = await helixGet<TwitchGameResponse>(`/games?name=${encodeURIComponent(PALADINS_CATEGORY_NAME)}`, clientId, clientSecret);
  paladinsCategoryId = response.data[0]?.id ?? null;
  return paladinsCategoryId;
}

export async function getPaladinsTwitchStreams(limit = 10): Promise<TwitchStreamResult> {
  const configuredCredentials = credentials();
  if (!configuredCredentials) return { configured: false, streams: [] };
  if (streamCache && streamCache.expiresAt > Date.now()) return streamCache.result;

  let result: TwitchStreamResult;
  try {
    const categoryId = await getPaladinsCategoryId(configuredCredentials.clientId, configuredCredentials.clientSecret);
    if (!categoryId) {
      result = { configured: true, streams: [] };
    } else {
      const requestedLimit = Math.min(Math.max(Math.floor(limit), 1), 20);
      const first = Math.min(requestedLimit + HIDDEN_CHANNEL_LOGINS.size, 20);
      const response = await helixGet<TwitchStreamsResponse>(
        `/streams?game_id=${encodeURIComponent(categoryId)}&first=${first}`,
        configuredCredentials.clientId,
        configuredCredentials.clientSecret,
      );
      result = {
        configured: true,
        streams: response.data
          .filter((stream) => !HIDDEN_CHANNEL_LOGINS.has(stream.user_login.trim().toLowerCase()))
          .slice(0, requestedLimit)
          .map((stream) => ({
            userLogin: stream.user_login,
            userName: stream.user_name,
            title: stream.title,
            viewerCount: stream.viewer_count,
            language: stream.language,
            thumbnailUrl: stream.thumbnail_url.replace('{width}', '320').replace('{height}', '180'),
            tags: stream.tags.slice(0, 3),
            url: `https://www.twitch.tv/${encodeURIComponent(stream.user_login)}`,
          })),
      };
    }
  } catch (error) {
    console.warn('[twitch] Unable to load Paladins streams:', error instanceof Error ? error.message : error);
    result = { configured: true, streams: [] };
  }

  streamCache = { expiresAt: Date.now() + STREAM_CACHE_MS, result };
  return result;
}
