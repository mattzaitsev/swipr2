/**
 * Request strategies: the replaceable decision layer of the gateway. Connectors only declare what they need
 * (URL, geo, session affinity, headers); every request-handling decision is made here, through four hooks:
 *
 *   beforeRequest    -> headers, delay, sticky/rotating session, pacing/concurrency for the source
 *   afterResponse    -> observe the classified response; adjust pacing/concurrency for later requests
 *   onVerdict        -> throttle, block or changed response: defer, retry, or return the response; rotate session
 *   onRetryableError -> proxy/network failure or target 5xx: retry or give up; rotate session
 *
 * Recovery levers are primitives (SourceControls below), so a strategy composes its own policy from them instead of
 * picking from fixed behaviors. The same primitives are public on the gateway (GatewayControls) for ops tooling.
 * Swapping a strategy never touches connectors, the normalizer, outbox, indexer or API: register it here and select
 * it with REQUEST_STRATEGY=<source>=<name>.
 */
import type { RequestStrategySettings } from '../config';
import type { Side, Verdict } from './events';
import type { GatewayRequest } from './index';
import type { SourcePolicy } from './policies';

export type CooldownScope = 'search' | 'source';
export interface CooldownPlan {
  ms: number;
  scope: CooldownScope;
}

/** Read-only view of the request being made. */
export interface RequestContext {
  readonly req: Readonly<GatewayRequest>;
  readonly attempt: number;                       // 0 on the first try
  readonly policy: Readonly<SourcePolicy>;
  readonly settings: Readonly<RequestStrategySettings>;
  readonly proxied: boolean;
  readonly sessionMode: 'sticky' | 'rotating';    // current plan (baseline: sticky when the request has a sessionKey)
  readonly headers: Readonly<Record<string, string>>;   // headers planned so far (baseline + connector)
}

export interface ResponseInfo {
  status: number;
  verdict: Verdict;
  ms: number;
  bytes: number;
  retryAfterMs?: number;
}

export interface Failure {
  side: Side;                                     // 'proxy' (exit node/provider), 'network' (direct), 'target' (5xx)
  kind: string;                                   // e.g. 'timeout', 'proxy_auth', 'econnreset', 'http_502'
  message: string;
}

export type RetryDecision = { action: 'retry'; delayMs: number; reason: string } | { action: 'give_up' };
export type VerdictDecision = RetryDecision | { action: 'return' };

/**
 * Recovery primitives, implemented by the gateway. Public on the gateway for ops tooling (with an explicit `by`);
 * strategies get them pre-bound to the request's source via SourceControls.
 */
export interface GatewayControls {
  /** Minimum gap between requests to a source. Returns the effective value. */
  setMinInterval(source: string, ms: number, reason: string, by?: string): number;
  /** Set in-flight requests for a source, without a policy ceiling. Infinity disables the cap. Minimum 1. */
  setConcurrency(source: string, limit: number, reason: string, by?: string): number;
  /** One-shot extra delay before the next request to a source. */
  delayNext(source: string, ms: number, reason: string, by?: string): number;
}

/** Primitives bound to the current request's source; available in every hook. */
export interface SourceControls {
  setMinInterval(ms: number, reason: string): number;
  setConcurrency(limit: number, reason: string): number;
  delayNext(ms: number, reason: string): number;
  /** Emit a strategy.note event (labels show up in GATEWAY_EVENTS=log and any metrics subscriber). */
  note(name: string, labels?: Record<string, string | number>): void;
}

export interface BeforeActions extends SourceControls {
  /** Add or override any header (including user-agent and sec-ch-ua*). */
  setHeader(name: string, value: string, reason: string): void;
  /** Wait this much longer before this request. */
  addDelay(ms: number, reason: string): void;
  /** Override the request timeout in milliseconds; 0 disables it. Baseline: policy.timeoutMs. */
  setTimeout(ms: number, reason: string): void;
  /** Sticky = keep the flow's exit IP; rotating = new exit IP per request. */
  useSessionMode(mode: 'sticky' | 'rotating', reason: string): void;
}

export type AfterActions = SourceControls;

export interface VerdictActions extends SourceControls {
  /** Set this cooldown's length and scope, including zero; applies to changed responses too. */
  cooldown(ms: number, scope: CooldownScope, reason: string): void;
  /** Move this flow to a fresh proxy session on a target throttle or block. */
  rotateSession(reason: string): void;
}

export interface FailureActions extends SourceControls {
  /** Move this flow to a fresh proxy session on any retryable failure. */
  rotateSession(reason: string): void;
}

export interface RequestStrategy {
  readonly name: string;
  beforeRequest?(ctx: RequestContext, act: BeforeActions): void | Promise<void>;
  afterResponse?(ctx: RequestContext, res: ResponseInfo, act: AfterActions): void | Promise<void>;
  /** Throttled/blocked/changed. Return retry or return to override baseline handling. Otherwise defer as usual. */
  onVerdict?(ctx: RequestContext, res: ResponseInfo, proposed: Readonly<CooldownPlan>, act: VerdictActions):
    VerdictDecision | void | Promise<VerdictDecision | void>;
  onRetryableError?(ctx: RequestContext, failure: Failure, act: FailureActions): RetryDecision | Promise<RetryDecision>;
}

/**
 * Default: no hook acts, so behavior is the baseline (no retries, no extra delay or headers, policy cooldowns).
 * EXTENSION POINT: write new strategies next to this one and register them below. Natural places for later logic:
 *  - beforeRequest: per-flow session preference (e.g. rotating for one-off detail fetches), accept-language
 *    matching the search area, identity headers, politeness delay from settings.timing.extraDelayMs.
 *  - afterResponse: setMinInterval/setConcurrency from latency trends or partial GraphQL errors.
 *  - onVerdict: custom cooldowns or session rotation on throttles/blocks, halving concurrency.
 *  - onRetryableError: retry proxy timeouts or 5xx on a fresh session with backoff.
 */
export const defaultStrategy: RequestStrategy = {
  name: 'default',
  onRetryableError: () => ({ action: 'give_up' }),
};

/** Example: retry a proxy-side failure once on a fresh session (registered, not enabled by default). */
export const retryProxyFailuresOnce: RequestStrategy = {
  name: 'retry-proxy-once',
  onRetryableError(ctx, failure, act) {
    if (failure.side !== 'proxy' || ctx.attempt > 0) return { action: 'give_up' };
    act.rotateSession(`${failure.kind} on attempt ${ctx.attempt}`);
    return { action: 'retry', delayMs: ctx.settings.retry.baseDelayMs, reason: failure.kind };
  },
};

/** Example: on a throttle, double the cooldown and halve concurrency (registered, not enabled by default). */
export const conservativeBackoff: RequestStrategy = {
  name: 'conservative-backoff',
  onVerdict(_ctx, res, proposed, act) {
    if (res.verdict !== 'throttled') return;
    act.cooldown(proposed.ms * 2, proposed.scope, 'double the throttle cooldown');
    act.setConcurrency(1, 'throttled: one request at a time');
  },
};

const registry = new Map<string, RequestStrategy>(
  [defaultStrategy, retryProxyFailuresOnce, conservativeBackoff].map((s) => [s.name, s]),
);

export function registerStrategy(strategy: RequestStrategy): void {
  registry.set(strategy.name, strategy);
}

export function strategyFor(source: string, settings: RequestStrategySettings): RequestStrategy {
  const name = settings.strategyBySource[source] ?? settings.strategyBySource['*'] ?? 'default';
  return registry.get(name) ?? defaultStrategy;
}
