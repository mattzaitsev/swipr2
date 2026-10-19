/**
 * Connector contract: connectors DECLARE, the gateway DECIDES.
 * A connector states what it needs: URLs, the search area (geo), which requests belong together (flows = session
 * affinity), protocol headers, and how to read its source's responses. It never chooses proxies, sessions, identity,
 * timing, retries or cooldowns; those belong to the gateway's strategy layer (src/gateway/strategy.ts). Changing that
 * layer never requires connector changes.
 */
import type { Area } from '../config';
import type { ListingDraft } from '../domain';
import type { GatewayResponse, Geo, Verdict } from '../gateway';
import type { Logger } from '../runtime';

/** One location-scoped search: a query near a point within a radius (how Marketplace search works). */
export interface SearchSpec {
  source: string;
  query: string;
  area: Area;
  maxPages: number;
}

/**
 * The scheduler runs every SearchSpec in two modes:
 *  - new:     frequent. Newest first; stop once we reach listings we already have ("seen this before").
 *  - refresh: rare. Walk all pages to catch price drops, pending/sold, and renewed (expiring) image URLs.
 */
export interface CrawlTask extends SearchSpec {
  mode: 'new' | 'refresh';
}

/** A listing payload exactly as the source returned it, plus where we found it. Stored in raw_events. */
export interface RawItem {
  externalId: string;                // keep as string: Marketplace ids exceed 2^53
  payload: unknown;
  context: { query: string; area: Area };
}

/** One request inside a flow. The flow adds source, session, geo, the connector's headers and its classifier. */
export interface FlowRequest {
  url: string;
  method?: 'GET' | 'POST';
  headers?: Record<string, string>;  // per-request extras; win over requestHeaders()
  body?: string;
}

/**
 * Related requests that should look like one visit and share a session (one exit IP when proxied):
 * a search and its pages, then detail fetches for listings that search found.
 */
export interface Flow {
  readonly kind: string;             // 'search' | 'detail' | ... (labels events and metrics)
  readonly sessionKey: string | null; // null = rotating (no shared session)
  fetch(req: FlowRequest): Promise<GatewayResponse>;
}

export interface FlowOptions {
  /** Default: the search's id, so its pages and follow-up detail fetches share a session. null = rotating. */
  sessionKey?: string | null;
  /** Default: the search area (exit location near where we're searching). */
  geo?: Geo;
}

export interface ConnectorContext {
  log: Logger;
  /** The only way a connector may touch the network: open a flow and fetch through it. */
  openFlow(kind: string, options?: FlowOptions): Flow;
  /** Which of these external ids are already stored (drives the incremental stop). */
  known(externalIds: string[]): Promise<Set<string>>;
}

export interface Connector {
  readonly source: string;
  /** How often each search runs in each mode. Keep this conservative for real sources. */
  readonly cadence: { newEveryMs: number; refreshEveryMs: number };

  /** The searches to run (queries x areas, usually from search.config.json). */
  searches(): SearchSpec[];

  /** Fetch one task's pages through ctx.openFlow() and yield raw items. Let RetryLater/SourceChanged propagate. */
  crawl(task: CrawlTask, ctx: ConnectorContext): AsyncIterable<RawItem>;

  /** Pure: raw payload -> ListingDraft, or null to skip. Separate from crawl() so raw_events can be re-parsed. */
  parse(item: RawItem): ListingDraft | null;

  /** Per-request protocol headers (content type, accept, ...). Source-specific header adjustments live here. */
  requestHeaders?(req: FlowRequest, flow: { kind: string; task: CrawlTask }): Record<string, string>;

  /** Read source-specific signals in a response: errors inside an HTTP 200, login walls, a changed shape.
   *  The gateway acts on the verdict (cooldown / pause / alert); the connector only interprets. */
  classifyResponse?(status: number, body: Buffer, flow: { kind: string }): Verdict;
}
