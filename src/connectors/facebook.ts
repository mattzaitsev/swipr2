/**
 * ─── EXTENSION POINT: the real Facebook Marketplace connector (not implemented yet) ───
 *
 * Intended shape: the same factory the mock uses, pointed at Facebook, e.g.
 *
 *   export const facebookConnector = createMarketplaceConnector({
 *     source: 'facebook',                                   // matches policies.facebook and PROXY_SOURCES
 *     endpoint: 'https://www.facebook.com/api/graphql/',
 *     docIds: { search: process.env.FB_SEARCH_DOC_ID! },    // rotates with Facebook deploys: keep it in env
 *     listingUrl: (id) => `https://www.facebook.com/marketplace/item/${id}/`,
 *     cadence: { newEveryMs: 10 * 60_000, refreshEveryMs: 2 * 60 * 60_000 },
 *     maxPages: 2, pageSize: 24, stopAfterKnown: 5,
 *   });
 *
 * Then register it in src/connectors/index.ts. Before that:
 *  1. Capture real logged-out responses and confirm the request variables (marketplace.ts searchVariables) and every
 *     field path in parseMarketplaceListing(): id, price, strikethrough price, location, seller, photo, flags.
 *     Add fixtures from those captures and a parse test.
 *  2. Expect doc_id rotation: it surfaces as SourceChanged (job fails, logged), not as retries. Update the env var.
 *  3. Geo: if search results lack coordinates, listings get the search area's centre (geo_precision 'area').
 *     Smaller search radii give better geo; a detail fetch can give approximate coordinates.
 *  4. Add a detail fetch (second doc_id) for description, condition, all photos, and removal detection for listings
 *     whose last_seen_at goes stale. Absence from search results is not proof of removal.
 *  5. Tune policies.facebook (pacing, cooldowns) and classifyMarketplace() against real throttle/block responses.
 *     Set PROXY_URL / PROXY_USERNAME / PROXY_PASSWORD / PROXY_USERNAME_TEMPLATE; proxied bytes are metered.
 *
 * Boundaries: logged-out access to public listings only. Never log in or use accounts, never solve CAPTCHAs,
 * and on a block wait out the cooldown; don't rotate identity to get past it.
 */
export {};
