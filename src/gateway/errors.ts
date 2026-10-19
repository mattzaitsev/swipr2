/**
 * "Not now" (e.g. throttled or blocked). The worker skips work until `retryAt`; it never fails it.
 * scope 'search' pauses one search; 'source' pauses every search on that source (e.g. a login wall hits the whole IP).
 */
export class RetryLater extends Error {
  constructor(public retryAt: number, public reason: string, public scope: 'search' | 'source' = 'search') {
    super(`retry later: ${reason}`);
    this.name = 'RetryLater';
  }
}

/** The source answered, but not in the shape the connector expects (e.g. a rotated GraphQL doc_id).
 *  Retrying won't help: the job fails loudly so a human updates the connector. */
export class SourceChanged extends Error {
  constructor(public source: string, detail: string) {
    super(`${source}: response shape changed (${detail}); update the connector`);
    this.name = 'SourceChanged';
  }
}
