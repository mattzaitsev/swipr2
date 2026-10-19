# swipr2

Swipr's existing marketplace pipeline with strategy-controlled request handling. Strategies can override headers, pacing, concurrency, timeouts, cooldowns, retry decisions, and session rotation. The gateway imposes no policy floors or ceilings on these choices and no daily proxy-bandwidth stop.

The default strategy still makes no retries and uses the original source-policy pacing, timeouts, and response cooldowns. Strategies decide their own retry termination. Connector implementations, normalization, database migrations, search, alerts, and API behavior are preserved.

A marketplace aggregator, built to connect to Facebook Marketplace. It runs as a single-machine local MVP. It crawls location-scoped searches through a fetch gateway, normalizes listings into Postgres/PostGIS, indexes them in Typesense, and alerts saved searches when new matches appear.

```
                     ┌──────────────────────── ingest worker (BullMQ) ─────────────────────────┐
 schedules ─jobs─►   connector.crawl() ─flow─► gateway (strategy hooks, proxy, pacing) ─► source │
 (new + refresh)     │      │ raw items                                                         │
                     │      ▼                                                                   │
                     │  normalize(): raw_events, listings, sellers, images, price_history       │
                     │               + outbox  (one transaction)                                │
                     └──────┬──────────────────────────────────────────────────────────────────┘
                            │ outbox (one row per consumer topic)
              ┌─────────────┴──────────────┐
              ▼ 'search'                   ▼ 'alerts'
    indexer ──► Typesense           matcher ──► saved_search_hits ──► Redis pub/sub
              ▲                                                         (email/push later)
    api: /search ─┘ (+ fresh image URLs from Postgres)    /listings/:id, /saved-searches ──► Postgres
```

## Local MVP

You need Docker Desktop. Node is optional and only used for the host-side scripts.

| | Command |
|---|---|
| Start everything (build + run) | `docker compose up -d --build` |
| Watch logs | `docker compose logs -f app` |
| Check the pipeline end to end | `docker compose exec app node --import tsx src/scripts/smoke.ts` |
| Stop (keeps data) | `docker compose down` |
| Reset (wipes all data) | `docker compose down -v` |
| Apply code changes | `docker compose restart app` (`src/` is mounted) |

With Node installed, the npm shortcuts are `npm run up | logs | down | reset | smoke`.

- **What runs:** Postgres + PostGIS, Redis, Typesense, and one app container. The app container runs the mock marketplace, the ingest worker, the indexer, the matcher and the API.
- **Ports:** everything binds to `127.0.0.1` only. The API is http://localhost:4200 and the mock marketplace is http://localhost:4210.
- **Cost:** none. The only source is a local mock marketplace. It speaks the same wire shape as the real connector and behaves the way Marketplace does: listings appear, prices drop, items sell, image URLs expire, and it can rate-limit.
- **Running the app on the host instead:** `docker compose up -d postgres redis typesense`, then `npm install && npm run dev`. Stop the `app` container first so the two don't crawl twice.

**What to search** is set in `search.config.json` as queries × areas, where each area is a point, a radius, and a country/region. **Everything else** is in `.env`; copy `.env.example`. Every setting has a default that works locally.

## Adding a residential proxy later (no code changes)

```env
PROXY_URL=http://gate.your-provider.example:7000
PROXY_USERNAME=...
PROXY_PASSWORD=...
PROXY_USERNAME_TEMPLATE={user}-country-{country}[-region-{region}][-session-{session}-sesstime-{ttl}]
PROXY_SOURCES=facebook        # only these sources use the proxy; the mock never does
```

- **Username template.** Copy the exact format from your provider's docs. `[…]` marks optional parts, dropped when a placeholder inside them is empty. That way one template covers both rotating requests (no session) and sticky ones.
- **Sticky sessions.** These come from connector flows: every page of one search shares a session, so one exit IP serves the whole search. The exit location follows the search area.
- **Usage.** Proxied bytes are counted per day in Redis (`swipr:proxy-bytes:YYYY-MM-DD`). This includes response bytes, request-body bytes, and an estimate for protocol overhead; it is not the provider's exact billing figure. Metering does not stop requests.

## Shaped for Facebook Marketplace

| Marketplace reality | How swipr handles it |
|---|---|
| Search is a GraphQL POST (`doc_id` + `variables`) with location, radius and cursor pages | `createMarketplaceConnector()` in `src/connectors/marketplace.ts`. The mock speaks the same shape. |
| You re-crawl the same searches all day | Each search has two schedules. **new** (frequent) sorts newest first and stops after `stopAfterKnown` listings it already has in a row. **refresh** (rare) walks every page to catch price drops and sold items. |
| Paging should look like one visit | A connector opens a **flow** per search. Its requests share one session (one exit IP when proxied) and the search's geo. |
| Ids are numeric strings longer than 2^53 | They are stored as text everywhere and never parsed as numbers. |
| Search results have approximate coordinates, or only a city | `geo_precision` records which: `approximate` when the source gave coordinates, `area` when it falls back to the search area's centre. A fallback never overwrites known coordinates. |
| Sellers show a struck-through "was" price | `original_price_cents`, alongside our own observed `price_history`. |
| Image URLs are signed and expire (`oe=`) | `listing_images.expires_at`. The signature is left out of change detection, so a new signature isn't counted as an update. URLs stay out of the search index; the API reads fresh ones from Postgres. |
| Failures arrive inside HTTP 200 (rate limits, login walls), and `doc_id`s rotate | The connector's `classifyResponse` reads these signals. By default, throttled pauses the search, blocked pauses the source, and a changed shape raises `SourceChanged`. A strategy can choose its own cooldown, retry, or return the response to the connector. |
| Sellers are people | Display name and kind only. The crawler never follows sellers to their profiles. |

## Facebook Marketplace: where it plugs in

1. **Connector:** `src/connectors/facebook.ts` holds the intended shape and a checklist: verify field paths against captured responses, keep the `doc_id` in env, add a detail fetch. Then register it in `src/connectors/index.ts`.
2. **Response signals:** `classifyMarketplace()` in `src/connectors/marketplace.ts`. Confirm it against real throttle and block responses.
3. **Pacing and cooldowns:** the `facebook` entry in `src/gateway/policies.ts`.
4. **Recovery:** a strategy's `onVerdict` / `onRetryableError` hooks, using the recovery primitives. The full FetchGateway (health tracking, cooldown → probe → ramp) replaces `HttpGateway` in `createGateway()` (`src/gateway/index.ts`).
5. **Request strategies:** `src/gateway/strategy.ts`. Register one and select it with `REQUEST_STRATEGY=facebook=<name>`.
6. **Proxy:** the env vars above. Add `facebook` to `PROXY_SOURCES`; it's already the default.
7. **Alerts to a human:** the `SourceChanged` handler in `src/ingest/worker.ts`.

The built-in connector skeleton targets logged-out public listings; account login and CAPTCHA handling are not implemented. The gateway does not restrict identity headers or require waiting after a block. Those decisions belong to the selected strategy.

## Request strategies

**Connectors declare, the gateway decides.** A connector states its URLs, search area, flows (which requests share a session), protocol headers and how to read its source's responses. Every request-handling decision lives in the gateway's strategy layer (`src/gateway/strategy.ts`). Adding or swapping a strategy never touches connectors, the normalizer, outbox, indexer or API: register it and select it with `REQUEST_STRATEGY=<source>=<name>`.

| Hook | When | What a strategy can do |
|---|---|---|
| `beforeRequest` | Before every attempt | Set any headers, add delay, choose a sticky or rotating session, override the timeout (`setTimeout(0, reason)` disables it) |
| `afterResponse` | After the response is classified | Observe; adjust pacing or concurrency for later requests |
| `onVerdict` | Throttled, blocked, or changed response | Choose cooldown length and scope; rotate the session; return `{ action: 'retry', delayMs, reason }` to retry in the same gateway call or `{ action: 'return' }` to pass the response to the connector |
| `onRetryableError` | Proxy/network failure or target 5xx | Retry or give up; rotate the session on any failure side. The strategy decides the number of attempts and delay |

**Recovery primitives** are available in every hook, bound to the request's source. They're also public on the gateway for ops tooling:
- `setMinInterval`: pacing; 0 removes spacing, even when the baseline policy is slower.
- `setConcurrency`: any positive integer, or `Infinity` to remove the gateway's in-flight cap.
- `delayNext`: a one-shot delay before the next request.

Cooldowns are set through `onVerdict`. Session rotation is offered in `onVerdict` and `onRetryableError`. Built-in examples, registered but off by default:
- `retry-proxy-once`: retries a proxy failure once on a fresh session.
- `conservative-backoff`: doubles the throttle cooldown and drops concurrency to 1.

Returning nothing (or `give_up`) from `onVerdict` keeps baseline handling, with any chosen cooldown applied. An explicit cooldown also overrides the default `SourceChanged` pause. Zero or expired cooldowns create no worker pause; a deferred crawl still ends and the scheduler starts a later run. Use an explicit `retry` decision for another attempt within the same gateway call.

Swipr1's `CONNECTOR_IDENTITY_HEADERS`, `REQUEST_MAX_ATTEMPTS`, `STRATEGY_MAX_DELAY_MS`, and `PROXY_DAILY_MB` settings are no longer read. Strategies own these decisions. Nonnegative delays and a positive concurrency value are required for meaningful timer and queue operation; these are input-domain rules, not source-policy limits.

**Settings** live in the `RequestStrategySettings` type in `src/config.ts`: session mode, headers, timing ranges, retries and event logging. They load from env, and the defaults change nothing. Per-source baselines are in `src/gateway/policies.ts`.

**Events** (`src/gateway/events.ts`) are labelled `origin: baseline | strategy` with `by: <strategy name>`. They cover:
- requests and their verdicts
- session starts and rotations
- headers (names only)
- timing, pacing and concurrency changes
- timeout changes
- cooldown decisions
- error classification and retries

Set `GATEWAY_EVENTS=log` to print them.

## API

Saved-search routes need an `x-user-id` header. That's Beta auth; see `userId()` in `src/api/server.ts`.

```bash
# keyword + geo + filters (prices in dollars); sort: relevance|newest|price_asc|price_desc|distance
curl "localhost:4200/search?q=couch&lat=30.2672&lng=-97.7431&radius_km=25&max_price=500"
curl "localhost:4200/search?sort=newest&limit=20&cursor=<next_cursor>"
curl "localhost:4200/listings/1"
curl -X POST localhost:4200/saved-searches -H "x-user-id: me" -H "content-type: application/json" \
  -d '{"name":"Cheap couches","query":"couch","lat":30.2672,"lng":-97.7431,"radius_km":25,"max_price":400}'
curl -H "x-user-id: me" localhost:4200/saved-searches/1/hits
```

**Pagination:**
- `newest`, `price_asc` and `price_desc` use keyset cursors, which stay stable while new listings arrive.
- `relevance` and `distance` use page cursors.

**Saved searches** match listings that arrive after the search is created.

## Testing failure modes against the mock (zero cost)

`npm test` runs gateway and worker-cooldown regressions using an in-process HTTP mock with network access disabled. It needs no services, proxies, or paid bandwidth. `npm run typecheck` checks all TypeScript sources. The stack smoke test (`npm run smoke`) still requires the running local Postgres/Redis/Typesense/app stack.

| Set in `.env`, then `docker compose up -d` | You should see |
|---|---|
| `MOCK_FAIL_RATE=0.5` | "Rate limit exceeded" inside HTTP 200s, and `… paused 30s: mock throttled (in-body)` |
| `MOCK_ACCEPT_DOC_ID=mock-search-v2` | A rotated doc_id. One `ALERT … response shape changed`, then the source pauses |
| `MOCK_IMAGE_TTL_MIN=1` | Images expire quickly; refresh crawls renew them |
| `GATEWAY_EVENTS=log` | Every gateway event, labelled baseline or strategy |

## Data model

`db/migrations/001_init.sql` and `002_marketplace.sql`:

| Table | Purpose |
|---|---|
| `listings` | Keyed by `(source, external_id)`. `content_hash` (excluding volatile fields) detects real changes. Also holds `geo_precision` and `original_price_cents`. |
| `sellers` | Display name and kind only. |
| `listing_images` | Signed source URLs with `expires_at`. `content_hash`/`storage_key` are reserved for mirroring. |
| `price_history` | A row on first sight and on every observed price change. |
| `raw_events` | Each distinct payload stored once, plus the search that found it, for replay and debugging. |
| `outbox` | Change events, one row per consumer topic, written in the same transaction as the listing. |
| `saved_searches`, `saved_search_hits` | Alert definitions and matches. |

`npm run reindex` rebuilds the Typesense collection behind the `listings` alias with no downtime.

## Known gaps

- **Removal detection.** A listing missing from search results isn't proof it was removed. Marking listings `removed` needs a detail fetch for listings whose `last_seen_at` has gone stale.
- **Thin search results.** They carry no description, condition or extra photos; those also come from a detail fetch.
- **Images.** They aren't mirrored yet: expired ones show as `image: null` until a refresh crawl renews them.
- **Auth and frontend.** There's no real auth and no frontend yet.
