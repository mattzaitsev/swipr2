import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

// Defaults match docker-compose.yml for running on the host (`npm run dev`). Inside the app container,
// docker-compose.yml sets the service hostnames. .env is optional.
try {
  process.loadEnvFile();
} catch {
  // no .env file: use defaults
}

const env = (key: string, fallback: string) => process.env[key] || fallback;
const num = (key: string, fallback: number) => Number(env(key, String(fallback)));

/** A location-scoped search area. Marketplace search is always "query near a point within a radius". */
export interface Area {
  label: string;
  lat: number;
  lng: number;
  radiusKm: number;
  country: string;                   // ISO 3166-1 alpha-2; also used for proxy geo-targeting
  region?: string;
  city?: string;
}

/**
 * Settings that request strategies (src/gateway/strategy.ts) read. Defaults reproduce the baseline exactly:
 * default strategy everywhere, sticky sessions per flow, no extra headers or delay, no retries.
 */
export interface RequestStrategySettings {
  /** Strategy per source, '*' = fallback. Env REQUEST_STRATEGY="facebook=default,*=default". Unknown names fall back to 'default'. */
  strategyBySource: Record<string, string>;
  session: {
    /** Baseline session for requests that carry a sessionKey: 'sticky' keeps one exit IP per flow. Env SESSION_MODE. */
    mode: 'sticky' | 'rotating';
    /** Provider-side lifetime of a sticky session, minutes ({ttl} in PROXY_USERNAME_TEMPLATE). Env PROXY_STICKY_TTL_MIN. */
    ttlMin: number;
  };
  headers: {
    /** Baseline User-Agent for requests, or '' for the HTTP client's default. Env USER_AGENT. Strategies may override per request. */
    userAgent: string;
    /** Accept-Language sent with every request, or '' for none. Env ACCEPT_LANGUAGE (e.g. "en-US,en;q=0.9"). */
    acceptLanguage: string;
  };
  timing: {
    /** Extra random delay [min, max] ms a strategy may add before each request. Env REQUEST_JITTER_MS="0-0" (off). */
    extraDelayMs: [number, number];
  };
  retry: {
    /** Starting delay strategies use for retries. Env RETRY_BASE_DELAY_MS. */
    baseDelayMs: number;
  };
  /** 'log' prints every gateway event; 'off' only counts them. Env GATEWAY_EVENTS. */
  events: 'off' | 'log';
}

function parseStrategyMap(raw: string): Record<string, string> {
  const map: Record<string, string> = {};
  for (const pair of raw.split(',').map((p) => p.trim()).filter(Boolean)) {
    const [source, name] = pair.includes('=') ? pair.split('=') : ['*', pair];
    map[source.trim()] = name.trim();
  }
  return map;
}

function parseRange(raw: string): [number, number] {
  const [min, max] = raw.split('-').map((v) => Math.max(0, Number(v) || 0));
  return [min, Math.max(min, max ?? min)];
}

/** What to search for, and where: search.config.json (queries x areas). */
const searchConfigPath = env('SEARCH_CONFIG', fileURLToPath(new URL('../search.config.json', import.meta.url)));
const search = JSON.parse(readFileSync(searchConfigPath, 'utf8')) as { queries: string[]; areas: Area[] };

export const config = {
  databaseUrl: env('DATABASE_URL', 'postgres://swipr:swipr@localhost:5433/swipr'),
  redisUrl: env('REDIS_URL', 'redis://localhost:6379'),
  typesense: {
    url: env('TYPESENSE_URL', 'http://localhost:8108'),
    apiKey: env('TYPESENSE_API_KEY', 'swipr-dev-key'),
    alias: 'listings',               // searches go through the alias; collections are listings_v1, _v2, ...
  },
  bindHost: env('BIND_HOST', '127.0.0.1'), // the app container sets 0.0.0.0
  apiPort: num('API_PORT', 4200),
  ingestConcurrency: num('INGEST_CONCURRENCY', 2),
  search,

  /**
   * Outbound proxy, used only for sources listed in PROXY_SOURCES. Empty PROXY_URL = everything direct.
   * Username template placeholders: {user} {session} {ttl} {country} {region} {city}. A [bracketed] part is
   * dropped when any placeholder inside it is empty, so one template serves rotating and sticky requests:
   *   {user}-country-{country}[-region-{region}][-session-{session}-sesstime-{ttl}]
   * (Illustrative: copy the exact format from your provider's docs.)
   */
  proxy: {
    url: env('PROXY_URL', ''),                          // e.g. http://gate.example-provider.com:7000
    username: env('PROXY_USERNAME', ''),
    password: env('PROXY_PASSWORD', ''),
    usernameTemplate: env('PROXY_USERNAME_TEMPLATE', '{user}'),   // sticky TTL: requestStrategy.session.ttlMin
    sources: env('PROXY_SOURCES', 'facebook').split(',').map((s) => s.trim()).filter(Boolean),
  },

  requestStrategy: {
    strategyBySource: parseStrategyMap(env('REQUEST_STRATEGY', '')),
    session: {
      mode: env('SESSION_MODE', 'sticky') === 'rotating' ? 'rotating' : 'sticky',
      ttlMin: num('PROXY_STICKY_TTL_MIN', 10),
    },
    headers: {
      userAgent: env('USER_AGENT', ''),
      acceptLanguage: env('ACCEPT_LANGUAGE', ''),
    },
    timing: {
      extraDelayMs: parseRange(env('REQUEST_JITTER_MS', '0-0')),
    },
    retry: {
      baseDelayMs: num('RETRY_BASE_DELAY_MS', 1_000),
    },
    events: env('GATEWAY_EVENTS', 'off') === 'log' ? 'log' : 'off',
  } satisfies RequestStrategySettings as RequestStrategySettings,

  mock: {
    port: num('MOCK_SOURCE_PORT', 4210),
    url: env('MOCK_SOURCE_URL', 'http://localhost:4210'),          // where the connector reaches the mock
    publicUrl: env('MOCK_PUBLIC_URL', 'http://localhost:4210'),    // where a browser reaches its images
    tickMs: num('MOCK_TICK_MS', 8_000),
    failRate: num('MOCK_FAIL_RATE', 0),
    imageTtlMin: num('MOCK_IMAGE_TTL_MIN', 60),
    newEveryMs: num('MOCK_NEW_EVERY_MS', 20_000),
    refreshEveryMs: num('MOCK_REFRESH_EVERY_MS', 180_000),
  },
};
