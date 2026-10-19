/**
 * Beta gateway: direct or proxied HTTP (per source), pacing and concurrency per source, cooldowns from response
 * verdicts, proxied byte metering, and the request-strategy layer with structured events.
 *
 * Per attempt:  baseline plan -> [beforeRequest] -> concurrency slot + pacing -> send -> classify -> [afterResponse]
 *               throttled/blocked -> [onVerdict] -> cooldown                     changed -> SourceChanged
 *               transport error or 5xx -> [onRetryableError] -> retry or the baseline outcome
 * With the default strategy every hook is inert, so behavior is the baseline.
 * Strategies control headers, pacing, concurrency, timeouts, cooldowns, retries, and session rotation.
 */
import type { Redis } from 'ioredis';
import { fetch } from 'undici';
import { config } from '../config';
import { sleep } from '../runtime';
import { RetryLater, SourceChanged } from './errors';
import { gatewayEvents, type EventInput, type Side, type Verdict } from './events';
import type { Gateway, GatewayRequest, GatewayResponse } from './index';
import { policyFor, type SourcePolicy } from './policies';
import { proxyAgentFor, proxyEnabled, rotateSession, sessionIdFor } from './proxy';
import {
  strategyFor, type CooldownPlan, type Failure, type FailureActions, type GatewayControls, type RequestContext,
  type RequestStrategy, type RetryDecision, type SourceControls,
} from './strategy';

const REQUEST_OVERHEAD_BYTES = 2_048;                    // rough headers + TLS share; providers bill both directions
const BASELINE = { by: 'baseline', origin: 'baseline' } as const;
type Emit = (e: EventInput) => void;
const emitFor = (source: string, flow?: string): Emit => (e) =>
  gatewayEvents.emit({ ...e, source, flow } as Parameters<typeof gatewayEvents.emit>[0]);

export class HttpGateway implements Gateway, GatewayControls {
  private nextSlot = new Map<string, number>();         // pacing: earliest next send per source
  private minInterval = new Map<string, number>();      // pacing overrides
  private oneShotDelay = new Map<string, number>();     // delayNext()
  private concurrency = new Map<string, number>();      // strategy-selected in-flight caps
  private inFlight = new Map<string, number>();
  private waiters = new Map<string, Array<() => void>>();
  private seenSessions = new Set<string>();
  private settings = config.requestStrategy;

  constructor(private redis: Redis) {}

  // ── Recovery primitives (GatewayControls): public for ops tooling, pre-bound for strategies ──

  setMinInterval(source: string, ms: number, reason: string, by = 'ops'): number {
    const effective = Math.max(0, ms);
    this.minInterval.set(source, effective);
    emitFor(source)({ type: 'pacing.changed', minIntervalMs: effective, reason, by, origin: by === 'baseline' ? 'baseline' : 'strategy' });
    return effective;
  }

  setConcurrency(source: string, limit: number, reason: string, by = 'ops'): number {
    const effective = Math.max(1, Math.floor(limit));
    this.concurrency.set(source, effective);
    this.wake(source);
    emitFor(source)({ type: 'concurrency.changed', limit: effective, reason, by, origin: by === 'baseline' ? 'baseline' : 'strategy' });
    return effective;
  }

  delayNext(source: string, ms: number, reason: string, by = 'ops'): number {
    const d = Math.max(0, ms);
    this.oneShotDelay.set(source, Math.max(this.oneShotDelay.get(source) ?? 0, d));
    emitFor(source)({ type: 'timing.adjusted', delayMs: d, reason: `next request: ${reason}`, by, origin: by === 'baseline' ? 'baseline' : 'strategy' });
    return d;
  }

  // ── Request path ──

  async fetch(req: GatewayRequest): Promise<GatewayResponse> {
    const policy = policyFor(req.source);
    const strategy = strategyFor(req.source, this.settings);
    const proxied = policy.proxy && proxyEnabled();
    const emit = emitFor(req.source, req.flow);
    const byStrategy = { by: strategy.name, origin: 'strategy' } as const;
    const controls = this.boundControls(req.source, strategy.name, emit);

    for (let attempt = 0; ; attempt++) {
      // ── Baseline plan: static config headers, then the connector's per-request headers ──
      const headers: Record<string, string> = {
        ...(this.settings.headers.userAgent ? { 'user-agent': this.settings.headers.userAgent } : {}),
        ...(this.settings.headers.acceptLanguage ? { 'accept-language': this.settings.headers.acceptLanguage } : {}),
        ...lowerKeys(req.headers ?? {}),
      };
      let sessionMode = this.settings.session.mode;
      let timeoutMs = policy.timeoutMs;
      let extraDelayMs = 0;
      const ctx: RequestContext = { req, attempt, policy, settings: this.settings, proxied, sessionMode, headers: { ...headers } };

      // ── Hook 1: beforeRequest ──  EXTENSION POINT: session preference, headers (including identity), delay, pacing.
      await strategy.beforeRequest?.(ctx, {
        ...controls,
        setHeader: (name, value, _reason) => {
          headers[name.toLowerCase()] = value;
          emit({ type: 'headers.applied', names: [name.toLowerCase()], ...byStrategy });
        },
        addDelay: (ms, reason) => {
          const d = Math.max(0, ms);
          extraDelayMs += d;
          emit({ type: 'timing.adjusted', delayMs: d, reason, ...byStrategy });
        },
        setTimeout: (ms, reason) => {
          timeoutMs = Math.max(0, ms);
          emit({ type: 'timeout.changed', timeoutMs, reason, ...byStrategy });
        },
        useSessionMode: (mode, reason) => {
          if (mode === sessionMode) return;
          sessionMode = mode;
          emit({ type: 'session.mode', mode, reason, ...byStrategy });
        },
      });

      // ── Send (concurrency slot + pacing) ──
      const release = await this.acquire(req.source, policy);
      let status: number;
      let resHeaders: Headers;
      let body: Buffer;
      let ms: number;
      try {
        await this.waitTurn(req.source, policy, extraDelayMs);
        const sticky = !!req.sessionKey && sessionMode === 'sticky';
        const sessionId = proxied && sticky ? sessionIdFor(req) : undefined;
        if (sessionId && !this.seenSessions.has(sessionId)) {
          this.trackSession(sessionId);
          emit({ type: 'session.started', sessionId, reason: 'new flow', ...BASELINE });
        }
        emit({ type: 'request.sent', attempt, proxied, session: !proxied ? 'direct' : sticky ? 'sticky' : 'rotating', sessionId, ...BASELINE });

        const started = performance.now();
        const res = await fetch(req.url, {
          method: req.method ?? 'GET',
          headers,
          body: req.body,
          dispatcher: proxied ? proxyAgentFor(req, sessionId) : undefined,
          signal: timeoutMs > 0 && Number.isFinite(timeoutMs) ? AbortSignal.timeout(timeoutMs) : undefined,
        });
        status = res.status;
        resHeaders = res.headers as unknown as Headers;
        body = Buffer.from(await res.arrayBuffer());
        ms = Math.round(performance.now() - started);
      } catch (err) {
        release();
        // ── Hook 4a: transport failure (timeout, refused, proxy error) ──
        const failure = classifyError(err, proxied);
        emit({ type: 'error.classified', attempt, side: failure.side, kind: failure.kind, message: failure.message, ...BASELINE });
        const decision = await this.decide(strategy, ctx, failure, req, controls, emit);
        if (decision.action === 'retry') {
          await sleep(decision.delayMs);
          continue;
        }
        throw err;                                       // baseline: the job fails; the next scheduled run tries again
      } finally {
        release();
      }

      if (proxied) await this.meter(body.length + (req.body?.length ?? 0) + REQUEST_OVERHEAD_BYTES);
      const verdict: Verdict = status === 429 ? 'throttled' : status === 403 ? 'blocked' : (req.classify?.(status, body) ?? 'ok');
      const retryAfterSec = Number(resHeaders.get('retry-after'));
      const retryAfterMs = Number.isFinite(retryAfterSec) && retryAfterSec > 0 ? retryAfterSec * 1000 : undefined;
      const info = { status, verdict, ms, bytes: body.length, retryAfterMs };
      emit({ type: 'request.completed', attempt, status, verdict, ms, bytes: body.length, ...BASELINE });

      // ── Hook 2: afterResponse ──  EXTENSION POINT: adapt pacing/concurrency to latency or partial errors.
      await strategy.afterResponse?.(ctx, info, controls);

      // ── Hook 3: onVerdict (throttled / blocked / changed) ── custom recovery or baseline handling.
      // session rotation, scope adjustments, concurrency cuts. No cooldown floor is enforced.
      if (verdict === 'throttled' || verdict === 'blocked' || verdict === 'changed') {
        const baselinePlan: CooldownPlan = verdict === 'throttled'
          ? { ms: retryAfterMs ?? policy.throttledCooldownMs, scope: 'search' }
          : { ms: verdict === 'changed' ? policy.changedPauseMs : retryAfterMs ?? policy.blockedCooldownMs, scope: 'source' };
        let requested: CooldownPlan | undefined;
        let reason = verdict === 'throttled'
          ? `throttled (${status === 429 ? 'HTTP 429' : 'in-body'})`
          : verdict === 'changed' ? 'unexpected response body'
          : `blocked (${status === 403 ? 'HTTP 403' : 'login wall/checkpoint'})`;
        const decision = await strategy.onVerdict?.(ctx, info, baselinePlan, {
          ...controls,
          cooldown: (cooldownMs, scope, why) => {
            requested = { ms: Math.max(0, cooldownMs), scope };
            reason = `${reason}; ${why}`;
          },
          rotateSession: (why) => {
            if (!req.sessionKey) return;
            const sessionId = rotateSession(req);
            this.trackSession(sessionId);
            emit({ type: 'session.rotated', sessionId, reason: why, ...byStrategy });
          },
        });
        if (decision?.action === 'retry') {
          const retry = this.scheduleRetry(ctx, decision, 'target', strategy.name, emit);
          await sleep(retry.delayMs);
          continue;
        }
        if (decision?.action === 'return') return { status, headers: Object.fromEntries(resHeaders), body };
        if (verdict === 'changed' && !requested) throw new SourceChanged(req.source, 'unexpected response body');
        const plan = requested ?? baselinePlan;
        emit({ type: 'cooldown.decided', verdict, ms: plan.ms, scope: plan.scope, reason, ...(requested ? byStrategy : BASELINE) });
        throw new RetryLater(Date.now() + plan.ms, `${req.source} ${reason}`, plan.scope);
      }

      if (status >= 500) {
        // ── Hook 4b: target server error ──
        const failure: Failure = { side: 'target', kind: `http_${status}`, message: `${req.source} HTTP ${status}` };
        emit({ type: 'error.classified', attempt, side: 'target', kind: failure.kind, message: failure.message, ...BASELINE });
        const decision = await this.decide(strategy, ctx, failure, req, controls, emit);
        if (decision.action === 'retry') {
          await sleep(decision.delayMs);
          continue;
        }
        throw new RetryLater(Date.now() + policy.serverErrorCooldownMs, failure.message);
      }
      return { status, headers: Object.fromEntries(resHeaders), body };
    }
  }

  /** Primitives bound to one source and labelled with the strategy that calls them. */
  private boundControls(source: string, by: string, emit: Emit): SourceControls {
    return {
      setMinInterval: (ms, reason) => this.setMinInterval(source, ms, reason, by),
      setConcurrency: (limit, reason) => this.setConcurrency(source, limit, reason, by),
      delayNext: (ms, reason) => this.delayNext(source, ms, reason, by),
      note: (name, labels = {}) => emit({ type: 'strategy.note', name, labels, by, origin: 'strategy' }),
    };
  }

  /** Ask the strategy about a retryable failure. The strategy owns its retry policy. */
  private async decide(strategy: RequestStrategy, ctx: RequestContext, failure: Failure, req: GatewayRequest,
                       controls: SourceControls, emit: Emit): Promise<RetryDecision> {
    const byStrategy = { by: strategy.name, origin: 'strategy' } as const;
    const act: FailureActions = {
      ...controls,
      rotateSession: (reason) => {
        if (!req.sessionKey) return;                     // rotating requests get a new exit IP anyway
        const sessionId = rotateSession(req);
        this.trackSession(sessionId);
        emit({ type: 'session.rotated', sessionId, reason, ...byStrategy });
      },
    };
    const decision = (await strategy.onRetryableError?.(ctx, failure, act)) ?? { action: 'give_up' as const };
    if (decision.action !== 'retry') return decision;
    return this.scheduleRetry(ctx, decision, failure.side, strategy.name, emit);
  }

  private scheduleRetry(ctx: RequestContext, decision: Extract<RetryDecision, { action: 'retry' }>,
                        side: Side, by: string, emit: Emit) {
    const delayMs = Math.max(0, decision.delayMs);
    emit({ type: 'retry.scheduled', attempt: ctx.attempt, side, delayMs, reason: decision.reason, by, origin: 'strategy' });
    return { ...decision, delayMs };
  }

  /** Wait for an in-flight slot for this source. Returns an idempotent release. */
  private async acquire(source: string, policy: SourcePolicy): Promise<() => void> {
    while ((this.inFlight.get(source) ?? 0) >= (this.concurrency.get(source) ?? policy.maxConcurrency)) {
      await new Promise<void>((resolve) => this.waiters.set(source, [...(this.waiters.get(source) ?? []), resolve]));
    }
    this.inFlight.set(source, (this.inFlight.get(source) ?? 0) + 1);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.inFlight.set(source, (this.inFlight.get(source) ?? 1) - 1);
      this.wake(source);
    };
  }

  private wake(source: string) {
    const queue = this.waiters.get(source);
    if (queue?.length) queue.shift()!();
  }

  /** Space requests to one source at least the effective min interval apart (per process), plus extra delays. */
  private async waitTurn(source: string, policy: SourcePolicy, extraDelayMs: number) {
    const oneShot = this.oneShotDelay.get(source) ?? 0;
    this.oneShotDelay.delete(source);
    const interval = this.minInterval.get(source) ?? policy.minIntervalMs;
    const now = Date.now();
    const slot = Math.max(now, this.nextSlot.get(source) ?? 0) + extraDelayMs + oneShot;
    this.nextSlot.set(source, slot + Math.max(0, interval));
    if (slot > now) await sleep(slot - now);
  }

  private trackSession(sessionId: string) {
    this.seenSessions.add(sessionId);
    if (this.seenSessions.size > 10_000) this.seenSessions.delete(this.seenSessions.values().next().value!);
  }

  private usageKey = () => `swipr:proxy-bytes:${new Date().toISOString().slice(0, 10)}`;

  private async meter(bytes: number) {
    const key = this.usageKey();
    await this.redis.multi().incrby(key, bytes).expire(key, 3 * 86_400).exec();
  }
}

const lowerKeys = (h: Record<string, string>) => Object.fromEntries(Object.entries(h).map(([k, v]) => [k.toLowerCase(), v]));

/**
 * Transport errors. Through a proxy we can't always tell a dead exit node from a reset by the target, so anything
 * that fails before an HTTP response counts as proxy-side; direct failures are 'network'.
 */
function classifyError(err: unknown, proxied: boolean): Failure {
  const e = err as { name?: string; code?: string; message?: string; cause?: { code?: string; message?: string } };
  const message = String(e?.cause?.message ?? e?.message ?? err).replace(/\/\/[^@/\s]+@/g, '//***@');
  const code = String(e?.cause?.code ?? e?.code ?? e?.name ?? 'error');
  const kind = /timeout|abort/i.test(code) ? 'timeout' : /\b407\b/.test(message) ? 'proxy_auth' : code.toLowerCase();
  const side: Side = proxied ? 'proxy' : 'network';
  return { side, kind, message };
}
