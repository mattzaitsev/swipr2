/**
 * Per-source request policy: pacing, timeouts, cooldowns. Reading source-specific signals in responses is the
 * connector's job (Connector.classifyResponse); acting on them is the gateway's.
 * ─── EXTENSION POINT (Facebook Marketplace recovery) ─── tune the `facebook` entry against real behavior
 * before enabling the connector.
 */
import { config } from '../config';

/**
 * Baseline values per source. Strategies adjust behavior at runtime through primitives (strategy.ts).
 */
export interface SourcePolicy {
  minIntervalMs: number;             // baseline gap between requests (per process); strategies may override
  maxConcurrency: number;            // baseline in-flight limit; strategies may override; Infinity = no cap
  timeoutMs: number;
  throttledCooldownMs: number;       // baseline cooldown after a throttle signal with no Retry-After
  blockedCooldownMs: number;         // baseline cooldown after a block / login wall
  serverErrorCooldownMs: number;     // pause after a 5xx the strategy didn't retry
  changedPauseMs: number;            // pause for the whole source when its responses change shape (connector needs updating)
}

const base = {
  maxConcurrency: Infinity,
  throttledCooldownMs: 10 * 60_000,
  blockedCooldownMs: 60 * 60_000,
  serverErrorCooldownMs: 2 * 60_000,
  changedPauseMs: 30 * 60_000,
};

export const policies: Record<string, SourcePolicy> = {
  // Local mock: no pacing, short cooldowns so injected failures recover quickly while testing.
  mock: { ...base, minIntervalMs: 0, timeoutMs: 10_000, throttledCooldownMs: 30_000, blockedCooldownMs: 60_000, serverErrorCooldownMs: 15_000 },

  // Facebook Marketplace (connector not implemented yet). Starting values: a few seconds between requests per
  // worker process, long cooldowns. Real load is set by crawl cadence (see facebook.ts).
  facebook: { ...base, minIntervalMs: 4_000, timeoutMs: 20_000 },
};

/** The policy for a source, plus whether it goes through the proxy (PROXY_SOURCES is the only switch for that). */
export function policyFor(source: string): SourcePolicy & { proxy: boolean } {
  const policy = policies[source] ?? { ...base, minIntervalMs: 1_000, timeoutMs: 15_000 };
  return { ...policy, proxy: config.proxy.sources.includes(source) };
}
