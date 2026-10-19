/** Env-driven residential/ISP proxy: username templating (geo + sticky sessions), session ids, agent reuse. */
import { createHash } from 'node:crypto';
import { ProxyAgent } from 'undici';
import { config } from '../config';
import type { GatewayRequest } from './index';

const cfg = config.proxy;
export const proxyEnabled = () => !!cfg.url;

const flowKey = (req: GatewayRequest) => {
  const geo = req.geo ? `${req.geo.country}|${req.geo.region ?? ''}` : '';
  return `${req.source}|${req.sessionKey}|${geo}`;
};

// Generation per flow: strategy-requested rotation bumps it, which yields a new session id.
const generations = new Map<string, number>();

/** Stable session id per (source, sessionKey, geo, generation): all requests of one flow keep the same exit IP. */
export function sessionIdFor(req: GatewayRequest): string {
  const key = flowKey(req);
  return createHash('sha256').update(`${key}|${generations.get(key) ?? 0}`).digest('hex').slice(0, 12);
}

/** Give this flow a fresh session id (and so a fresh exit IP). Returns the new id. */
export function rotateSession(req: GatewayRequest): string {
  const key = flowKey(req);
  generations.set(key, (generations.get(key) ?? 0) + 1);
  if (generations.size > 10_000) generations.delete(generations.keys().next().value!);
  return sessionIdFor(req);
}

/** Provider-style username, e.g. "user-country-us-region-tx-session-ab12cd34ef56-sesstime-10". */
export function proxyUsername(req: GatewayRequest, sessionId?: string): string {
  const slug = (v?: string) => (v ?? '').toLowerCase().replace(/[^a-z0-9]/g, '');
  const vars: Record<string, string> = {
    user: cfg.username,
    session: sessionId ?? '',
    ttl: sessionId ? String(config.requestStrategy.session.ttlMin) : '',
    country: slug(req.geo?.country),
    region: slug(req.geo?.region),
    city: slug(req.geo?.city),
  };
  const fill = (s: string) => s.replace(/\{(\w+)\}/g, (_, k: string) => vars[k] ?? '');
  return cfg.usernameTemplate
    .replace(/\[([^\]]*)\]/g, (_, part: string) => {
      const keys = [...part.matchAll(/\{(\w+)\}/g)].map((m) => m[1]);
      return keys.every((k) => vars[k]) ? fill(part) : '';
    })
    .replace(/\{(\w+)\}/g, (_, k: string) => vars[k] ?? '');
}

// One agent per distinct username (= per geo/session), reused for keep-alive; oldest evicted past the cap.
const agents = new Map<string, ProxyAgent>();
const MAX_AGENTS = 100;

/** sessionId set => sticky; undefined => rotating (new exit IP per connection). */
export function proxyAgentFor(req: GatewayRequest, sessionId?: string): ProxyAgent {
  const username = proxyUsername(req, sessionId);
  let agent = agents.get(username);
  if (agent) {
    agents.delete(username);                             // re-insert = most recently used
  } else {
    agent = new ProxyAgent({
      uri: cfg.url,
      token: `Basic ${Buffer.from(`${username}:${cfg.password}`).toString('base64')}`,
      keepAliveTimeout: sessionId ? 30_000 : 1_000,      // rotating: short keep-alive so the next request gets a new exit IP
      connections: 4,
    });
    if (agents.size >= MAX_AGENTS) {
      const [oldestKey, oldest] = agents.entries().next().value!;
      agents.delete(oldestKey);
      void oldest.close();
    }
  }
  agents.set(username, agent);
  return agent;
}

/** For logs: never print the password, and only the username's shape. */
export const describeProxy = () => (proxyEnabled() ? `${new URL(cfg.url).host} (username ${cfg.usernameTemplate})` : 'direct');
