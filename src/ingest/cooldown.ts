import type { Redis } from 'ioredis';
import type { RetryLater } from '../gateway/errors';

/** Apply the requested pause exactly; an expired or zero cooldown creates no Redis pause. */
export async function applyCooldown(redis: Pick<Redis, 'set'>, key: string, err: RetryLater, now = Date.now()) {
  const waitMs = Math.max(0, Math.ceil(err.retryAt - now));
  if (waitMs === 0) return { waitMs, first: false };
  const first = await redis.set(key, err.reason, 'PX', waitMs, 'NX');
  return { waitMs, first: first !== null };
}
