import assert from 'node:assert/strict';
import { after, afterEach, before, test } from 'node:test';
import { getGlobalDispatcher, MockAgent, setGlobalDispatcher } from 'undici';
import type { Redis } from 'ioredis';
import { config } from '../config';
import { RetryLater, SourceChanged } from './errors';
import { HttpGateway } from './http-gateway';
import { policies } from './policies';
import { sessionIdFor } from './proxy';
import { gatewayEvents } from './events';
import { registerStrategy, type RequestStrategy } from './strategy';

// MockAgent prevents all real network connections. No Redis, proxy, or marketplace is used.
let status = 200;
let received = new Headers();
let requests = 0;
let responseStatus: (() => number) | undefined;
const previousDispatcher = getGlobalDispatcher();
const transport = new MockAgent();
let sequence = 0;
const originalSettings = structuredClone(config.requestStrategy);

before(() => {
  transport.disableNetConnect();
  setGlobalDispatcher(transport);
});
after(async () => {
  setGlobalDispatcher(previousDispatcher);
  await transport.close();
});
afterEach(() => {
  Object.assign(config.requestStrategy, structuredClone(originalSettings));
  status = 200;
  requests = 0;
  responseStatus = undefined;
});

function setup(strategy?: Omit<RequestStrategy, 'name'>) {
  const source = `gateway-test-${++sequence}`;
  const origin = `http://${source}.test`;
  transport.get(origin).intercept({ path: '/' }).reply((opts) => {
    received = new Headers(opts.headers as Record<string, string>);
    requests++;
    return {
      statusCode: responseStatus?.() ?? status, data: 'test response',
      responseOptions: { headers: { 'retry-after': '120' } },
    };
  }).persist();
  policies[source] = {
    minIntervalMs: 0, maxConcurrency: 2, timeoutMs: 2_000,
    throttledCooldownMs: 10_000, blockedCooldownMs: 20_000,
    serverErrorCooldownMs: 1_000, changedPauseMs: 1_000,
  };
  if (strategy) {
    registerStrategy({ ...strategy, name: source });
    config.requestStrategy.strategyBySource = { [source]: source };
  } else {
    config.requestStrategy.strategyBySource = {};
  }
  return { gateway: new HttpGateway({} as Redis), req: { source, url: `${origin}/`, sessionKey: source } };
}

test('connector identity headers can change between requests', async () => {
  const { gateway, req } = setup();
  await gateway.fetch({ ...req, headers: { 'User-Agent': 'connector-one', 'Sec-CH-UA': 'browser-one' } });
  assert.equal(received.get('user-agent'), 'connector-one');
  assert.equal(received.get('sec-ch-ua'), 'browser-one');
  await gateway.fetch({ ...req, headers: { 'User-Agent': 'connector-two', 'Sec-CH-UA': 'browser-two' } });
  assert.equal(received.get('user-agent'), 'connector-two');
  assert.equal(received.get('sec-ch-ua'), 'browser-two');
});

test('strategy identity headers override connector and configured headers', async () => {
  config.requestStrategy.headers.userAgent = 'configured';
  const { gateway, req } = setup({
    beforeRequest(_ctx, act) {
      act.setHeader('User-Agent', 'strategy-agent', 'test');
      act.setHeader('Sec-CH-UA', 'strategy-browser', 'test');
    },
  });
  await gateway.fetch({ ...req, headers: { 'User-Agent': 'connector-agent' } });
  assert.equal(received.get('user-agent'), 'strategy-agent');
  assert.equal(received.get('sec-ch-ua'), 'strategy-browser');
});

test('a strategy can choose zero cooldown and narrower scope despite Retry-After', async () => {
  status = 403;
  const { gateway, req } = setup({
    onVerdict(_ctx, _res, _proposed, act) { act.cooldown(0, 'search', 'test'); },
  });
  await assert.rejects(gateway.fetch(req), (err) => {
    assert.ok(err instanceof RetryLater);
    assert.equal(err.scope, 'search');
    assert.ok(err.retryAt <= Date.now());
    return true;
  });
});

test('a strategy can rotate a session after a target block', async () => {
  status = 403;
  const { gateway, req } = setup({
    onVerdict(_ctx, _res, _proposed, act) { act.rotateSession('test'); },
  });
  const previous = sessionIdFor(req);
  await assert.rejects(gateway.fetch(req), RetryLater);
  assert.notEqual(sessionIdFor(req), previous);
});

test('a strategy can rotate a session after a target server error', async () => {
  status = 503;
  const { gateway, req } = setup({
    onRetryableError(_ctx, failure, act) {
      assert.equal(failure.side, 'target');
      act.rotateSession('test');
      return { action: 'give_up' };
    },
  });
  const previous = sessionIdFor(req);
  await assert.rejects(gateway.fetch(req), RetryLater);
  assert.notEqual(sessionIdFor(req), previous);
});

test('the default strategy still uses Retry-After and the baseline block scope', async () => {
  status = 403;
  const { gateway, req } = setup();
  const started = Date.now();
  await assert.rejects(gateway.fetch(req), (err) => {
    assert.ok(err instanceof RetryLater);
    assert.equal(err.scope, 'source');
    assert.ok(err.retryAt >= started + 120_000);
    assert.ok(err.retryAt <= Date.now() + 120_000);
    return true;
  });
});

test('strategies can raise concurrency above the baseline or remove the cap', () => {
  const { gateway, req } = setup();
  assert.equal(gateway.setConcurrency(req.source, 5, 'test'), 5);
  assert.equal(gateway.setConcurrency(req.source, Infinity, 'test'), Infinity);
});

test('strategy delays can exceed the former thirty-second ceiling', () => {
  const { gateway, req } = setup();
  assert.equal(gateway.delayNext(req.source, 60_000, 'test'), 60_000);
});

test('a strategy owns its retry count beyond the former one-attempt default', async () => {
  responseStatus = () => requests <= 3 ? 503 : 200;
  const { gateway, req } = setup({
    onRetryableError(ctx) {
      return ctx.attempt < 3 ? { action: 'retry', delayMs: 0, reason: 'test' } : { action: 'give_up' };
    },
  });
  assert.equal((await gateway.fetch(req)).status, 200);
  assert.equal(requests, 4);
});

test('a verdict strategy can retry a blocked request immediately', async () => {
  responseStatus = () => requests === 1 ? 403 : 200;
  const { gateway, req } = setup({
    onVerdict() { return { action: 'retry', delayMs: 0, reason: 'test' }; },
  });
  assert.equal((await gateway.fetch(req)).status, 200);
  assert.equal(requests, 2);
});

test('a verdict strategy can pass a classified response back to its connector', async () => {
  status = 429;
  const { gateway, req } = setup({ onVerdict() { return { action: 'return' }; } });
  assert.equal((await gateway.fetch(req)).status, 429);
  assert.equal(requests, 1);
});

test('changed responses retain their default error but can be handled by a strategy', async () => {
  const baseline = setup();
  await assert.rejects(baseline.gateway.fetch({ ...baseline.req, classify: () => 'changed' }), SourceChanged);
  const custom = setup({ onVerdict() { return { action: 'return' }; } });
  assert.equal((await custom.gateway.fetch({ ...custom.req, classify: () => 'changed' })).status, 200);
});

test('a strategy can disable the timeout even when the policy would reject it', async () => {
  const { gateway, req } = setup({ beforeRequest(_ctx, act) { act.setTimeout(0, 'test'); } });
  policies[req.source].timeoutMs = -1;
  assert.equal((await gateway.fetch(req)).status, 200);
});

test('a strategy can choose its cooldown for a changed response', async () => {
  const { gateway, req } = setup({
    onVerdict(_ctx, _res, _proposed, act) { act.cooldown(0, 'search', 'test'); },
  });
  await assert.rejects(gateway.fetch({ ...req, classify: () => 'changed' }), (err) => {
    assert.ok(err instanceof RetryLater);
    assert.equal(err.scope, 'search');
    assert.ok(err.retryAt <= Date.now());
    return true;
  });
});

test('retry delays retain the full strategy duration beyond thirty seconds', { timeout: 5_000 }, async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  responseStatus = () => requests === 1 ? 503 : 200;
  const { gateway, req } = setup({
    onRetryableError() { return { action: 'retry', delayMs: 60_000, reason: 'test' }; },
  });
  let scheduled!: () => void;
  const ready = new Promise<void>((resolve) => { scheduled = resolve; });
  const off = gatewayEvents.on((event) => {
    if (event.source === req.source && event.type === 'retry.scheduled') scheduled();
  });
  try {
    const result = gateway.fetch(req);
    await ready;
    await new Promise<void>((resolve) => setImmediate(resolve));
    t.mock.timers.tick(59_999);
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(requests, 1);
    t.mock.timers.tick(1);
    assert.equal((await result).status, 200);
    assert.equal(requests, 2);
  } finally {
    off();
  }
});
