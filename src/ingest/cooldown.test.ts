import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { Redis } from 'ioredis';
import { RetryLater } from '../gateway/errors';
import { applyCooldown } from './cooldown';

test('zero and expired cooldowns do not create a Redis pause', async () => {
  const redis = { set() { assert.fail('zero cooldown must not write to Redis'); } } as unknown as Redis;
  assert.deepEqual(await applyCooldown(redis, 'search', new RetryLater(100, 'test'), 100), { waitMs: 0, first: false });
  assert.deepEqual(await applyCooldown(redis, 'search', new RetryLater(99, 'test'), 100), { waitMs: 0, first: false });
});

test('subsecond cooldowns retain the exact requested duration', async () => {
  const calls: unknown[][] = [];
  const redis = { async set(...args: unknown[]) { calls.push(args); return 'OK'; } } as unknown as Redis;
  assert.deepEqual(await applyCooldown(redis, 'search', new RetryLater(125, 'test'), 100), { waitMs: 25, first: true });
  assert.deepEqual(calls, [['search', 'test', 'PX', 25, 'NX']]);
});

test('an existing cooldown is not overwritten', async () => {
  const redis = { async set() { return null; } } as unknown as Redis;
  assert.deepEqual(await applyCooldown(redis, 'source', new RetryLater(200, 'test'), 100), { waitMs: 100, first: false });
});
