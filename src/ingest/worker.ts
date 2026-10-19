/**
 * Ingest process. Every connector search runs on two BullMQ schedules:
 *   new      (frequent): newest first, stop at listings we already have
 *   refresh  (rare):     all pages, to catch price/status changes and renew expiring image URLs
 * Each run: connector.crawl() --gateway--> source  =>  raw items  =>  normalize()  =>  Postgres + outbox
 *
 * The worker makes no request-handling decisions. It carries out the gateway's (RetryLater: pause this search or
 * the whole source until retryAt) and applies the source policy's changedPauseMs when a response changes shape.
 */
import { Queue, Worker } from 'bullmq';
import { config } from '../config';
import { connectors, getConnector, type CrawlTask } from '../connectors';
import { openFlow, searchId } from '../connectors/flow';
import { pool } from '../db';
import { createGateway, RetryLater, SourceChanged } from '../gateway';
import { logGatewayEvents } from '../gateway/events';
import { policyFor } from '../gateway/policies';
import { describeProxy, proxyEnabled } from '../gateway/proxy';
import { bullConnection, createRedis } from '../redis';
import { isMain, logger, runMain } from '../runtime';
import { normalize, type NormalizeResult } from './normalize';
import { applyCooldown } from './cooldown';

const log = logger('ingest');
const deferKey = (t: CrawlTask) => `swipr:defer:${searchId(t)}`;   // shared by both modes: a cooldown pauses the search
const sourcePauseKey = (source: string) => `swipr:defer:source:${source}`;

async function known(source: string, ids: string[]): Promise<Set<string>> {
  if (!ids.length) return new Set();
  const { rows } = await pool.query<{ external_id: string }>(
    `select external_id from listings where source = $1 and external_id = any($2)`,
    [source, ids],
  );
  return new Set(rows.map((r) => r.external_id));
}

export async function start() {
  const queue = new Queue<CrawlTask>('ingest', { connection: bullConnection });
  const redis = createRedis();

  // Schedules: upserted on every boot; schedules for searches that no longer exist are removed.
  // EXTENSION POINT: per-search cadence (busy areas more often, quiet ones less).
  const wanted = new Set<string>();
  for (const connector of connectors) {
    for (const search of connector.searches()) {
      for (const mode of ['new', 'refresh'] as const) {
        const id = `${searchId(search)}:${mode}`;
        wanted.add(id);
        const every = mode === 'new' ? connector.cadence.newEveryMs : connector.cadence.refreshEveryMs;
        await queue.upsertJobScheduler(id, { every }, {
          name: mode,
          data: { ...search, mode },
          opts: { removeOnComplete: 100, removeOnFail: 500 },
        });
      }
    }
  }
  for (const scheduler of await queue.getJobSchedulers()) {
    if (scheduler.key && !wanted.has(scheduler.key)) await queue.removeJobScheduler(scheduler.key);
  }

  // A restart is usually a deploy (e.g. a connector fix for a rotated doc_id): clear source-wide pauses from SourceChanged.
  for (const connector of connectors) await redis.del(`${sourcePauseKey(connector.source)}:changed`);

  const gateway = createGateway(redis);
  if (config.requestStrategy.events === 'log') logGatewayEvents();
  const worker = new Worker<CrawlTask>(
    'ingest',
    async (job) => {
      const task = job.data;
      // Inside a cooldown from an earlier RetryLater (this search or the whole source): skip without sending a request.
      const paused = await redis.exists(deferKey(task), sourcePauseKey(task.source), `${sourcePauseKey(task.source)}:changed`);
      if (paused) return { deferred: true };

      const connector = getConnector(task.source);
      const counts: Record<NormalizeResult, number> = { created: 0, updated: 0, unchanged: 0, skipped: 0 };
      const ctx = {
        log,
        // Flows share a session per search by default: its pages (and later its detail fetches) use one exit IP.
        openFlow: (kind: string, options?: Parameters<typeof openFlow>[4]) => openFlow(gateway, connector, task, kind, options),
        known: (ids: string[]) => known(task.source, ids),
      };
      try {
        for await (const item of connector.crawl(task, ctx)) {
          counts[await normalize(connector, item)]++;
        }
      } catch (err) {
        // "Not now": remember it for this search and let scheduled runs skip until then. Nothing fails, nothing piles up,
        // and items already normalized are kept (normalize is idempotent).
        if (err instanceof RetryLater) {
          const key = err.scope === 'source' ? sourcePauseKey(task.source) : deferKey(task);
          const { waitMs, first } = await applyCooldown(redis, key, err);
          if (first) log(`${err.scope === 'source' ? `all ${task.source} searches` : searchId(task)} paused ${Math.round(waitMs / 1000)}s: ${err.reason}`);
          return { ...counts, deferred: true };
        }
        // The source changed shape (e.g. rotated doc_id): retrying won't help. Fail this job, pause the whole source,
        // and alert once. EXTENSION POINT (Marketplace recovery): page a human here (Slack/email).
        if (err instanceof SourceChanged) {
          const pauseMs = policyFor(task.source).changedPauseMs;
          const first = await redis.set(`${sourcePauseKey(task.source)}:changed`, err.message, 'PX', pauseMs, 'NX');
          if (first) log(`ALERT ${err.message}. Pausing all ${task.source} searches for ${Math.round(pauseMs / 60_000)} min (or until restart).`);
        }
        throw err;
      }
      if (counts.created || counts.updated) log(`${searchId(task)} [${task.mode}]`, counts);
      return counts;
    },
    { connection: bullConnection, concurrency: config.ingestConcurrency },
  );
  worker.on('failed', (job, err) => {
    if (!(err instanceof SourceChanged)) log(`${job ? searchId(job.data) : '?'} failed: ${err.message}`);
  });

  const searches = connectors.reduce((n, c) => n + c.searches().length, 0);
  const outbound = proxyEnabled()
    ? `proxy ${describeProxy()} for [${config.proxy.sources.join(', ')}], direct for the rest`
    : 'all direct (PROXY_URL not set)';
  log(`${searches} searches scheduled (new + refresh); outbound: ${outbound}`);
  return async () => {
    await worker.close();
    await queue.close();
    await redis.quit();
  };
}

if (isMain(import.meta.url)) runMain(start);
