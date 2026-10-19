/**
 * ─── PLUG POINT: the ingestion fetch gateway ───────────────────────────────────────────────
 * Connectors depend only on the `Gateway` interface below and never see proxies, sessions or
 * health state. The Beta ships `HttpGateway`:
 *  - direct or env-configured proxy
 *  - per-source pacing and cooldowns
 *  - request strategies (src/gateway/strategy.ts) and structured events (src/gateway/events.ts)
 *  - proxy byte metering
 * The full FetchGateway (health tracking, breaker, recovery strategies) replaces it inside `createGateway()`
 * and calls the same strategy hooks at the same points; nothing else changes.
 */
import type { Redis } from 'ioredis';
import type { Verdict } from './events';
import { HttpGateway } from './http-gateway';

export interface Geo {
  country: string;
  region?: string;
  city?: string;
}

export interface GatewayRequest {
  source: string;                    // connector id; selects policy and strategy
  url: string;
  method?: 'GET' | 'POST';
  headers?: Record<string, string>;
  body?: string;
  geo?: Geo;                         // exit location for proxied requests (match the search area)
  sessionKey?: string;               // set => requests share a sticky proxy session (one flow); omit => rotating
  flow?: string;                     // flow kind for labels/metrics, e.g. 'search' | 'detail'
  /** Source-specific reading of a response (e.g. errors inside an HTTP 200). Supplied by the connector. */
  classify?: (status: number, body: Buffer) => Verdict;
}

export interface GatewayResponse {
  status: number;
  headers: Record<string, string | string[] | undefined>;
  body: Buffer;
}

export interface Gateway {
  fetch(req: GatewayRequest): Promise<GatewayResponse>;
}

export { RetryLater, SourceChanged } from './errors';
export type { Verdict } from './events';

export function createGateway(redis: Redis): Gateway {
  // Swap in the full gateway here, e.g.:
  //   return new FetchGateway(provider, policies, health, sessions, usage, emit);
  return new HttpGateway(redis);
}
