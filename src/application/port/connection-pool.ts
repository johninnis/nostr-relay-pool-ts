import type { NostrFilter, RelayUrl } from "@innis/nostr-core"
import type { RelaySubscribeCallbacks, Subscription } from "../../domain/value-object/subscription.ts"

/**
 * The slice of the pool the internal subscription fan-out drives. It speaks the domain `RelayUrl`,
 * not raw strings: callers hand it URLs already normalised at the public boundary, so normalisation
 * happens exactly once rather than again on every internal hop.
 */
export interface ConnectionPool {
  /** Open (or join) a single-relay subscription for already-normalised `url`. */
  readonly subscribe: (
    url: RelayUrl,
    filters: ReadonlyArray<NostrFilter>,
    callbacks: RelaySubscribeCallbacks,
  ) => Subscription
  /** Adaptive EOSE timeout (ms) for the relay, derived from its observed latency. */
  readonly suggestedTimeout: (url: RelayUrl) => number
}
