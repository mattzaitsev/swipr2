/**
 * Structured gateway events. Every event says who caused it:
 *   origin 'baseline' = normal operation (config, policy, connector), origin 'strategy' = a RequestStrategy acted.
 * Header events carry header *names* only, never values. Session ids are already hashes.
 *
 * How these can inform tuning later: compare verdict rates (throttled/blocked) and latency per source, per flow kind
 * and per strategy; check that strategy-driven delays/retries actually change outcomes before keeping them.
 * EXTENSION POINT: subscribe a metrics exporter (Prometheus counters) or write events to a table.
 */
import { logger } from '../runtime';

export type Origin = 'baseline' | 'strategy';
export type Side = 'proxy' | 'target' | 'network';
export type Verdict = 'ok' | 'throttled' | 'blocked' | 'changed';

interface Base {
  source: string;
  flow?: string;                     // flow kind, e.g. 'search' | 'detail'
  by: string;                        // 'baseline' or the strategy name
  origin: Origin;
}

export type GatewayEvent = Base &
  (
    | { type: 'request.sent'; attempt: number; proxied: boolean; session: 'direct' | 'rotating' | 'sticky'; sessionId?: string }
    | { type: 'request.completed'; attempt: number; status: number; verdict: Verdict; ms: number; bytes: number }
    | { type: 'session.started' | 'session.rotated'; sessionId: string; reason: string }
    | { type: 'session.mode'; mode: 'sticky' | 'rotating'; reason: string }
    | { type: 'headers.applied'; names: string[] }
    | { type: 'headers.rejected'; names: string[]; reason: string }
    | { type: 'timing.adjusted'; delayMs: number; reason: string }
    | { type: 'timeout.changed'; timeoutMs: number; reason: string }
    | { type: 'error.classified'; attempt: number; side: Side; kind: string; message: string }
    | { type: 'retry.scheduled'; attempt: number; side: Side; delayMs: number; reason: string }
    | { type: 'retry.refused'; attempt: number; side: Side; reason: string }
    | { type: 'cooldown.decided'; verdict: Verdict; ms: number; scope: 'search' | 'source'; reason: string }
    | { type: 'pacing.changed'; minIntervalMs: number; reason: string }
    | { type: 'concurrency.changed'; limit: number; reason: string }
    | { type: 'strategy.note'; name: string; labels: Record<string, string | number> }
  );

/** An event without the request labels (source, flow), which the gateway fills in. */
export type EventInput = GatewayEvent extends infer E ? (E extends unknown ? Omit<E, 'source' | 'flow'> : never) : never;

type Listener = (e: GatewayEvent & { at: number }) => void;

export class GatewayEvents {
  private listeners: Listener[] = [];
  readonly counts = new Map<string, number>();       // "type|origin|by" -> count, for a quick look while tuning

  on(listener: Listener): () => void {
    this.listeners.push(listener);
    return () => (this.listeners = this.listeners.filter((l) => l !== listener));
  }

  emit(event: GatewayEvent): void {
    const key = `${event.type}|${event.origin}|${event.by}`;
    this.counts.set(key, (this.counts.get(key) ?? 0) + 1);
    const stamped = { ...event, at: Date.now() };
    for (const listener of this.listeners) {
      try {
        listener(stamped);
      } catch {
        // a broken listener must never break a request
      }
    }
  }
}

export const gatewayEvents = new GatewayEvents();

/** GATEWAY_EVENTS=log prints every event (noisy; for local tuning). */
export function logGatewayEvents(): () => void {
  const log = logger('gateway');
  return gatewayEvents.on(({ at: _at, type, source, flow, origin, by, ...rest }) =>
    log(`${type} ${source}${flow ? `/${flow}` : ''} [${origin === 'strategy' ? `strategy:${by}` : 'baseline'}]`, rest));
}
