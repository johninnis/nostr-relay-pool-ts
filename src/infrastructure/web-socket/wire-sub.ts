import type { NostrFilter } from "@innis/nostr-core"
import type { RelaySubscribeCallbacks } from "../../domain/value-object/subscription.ts"

// A wire subscription's listeners are exactly the public per-subscribe callbacks: the pool fans one
// relay sub out to every caller that deduplicated onto the same filter hash. One shape, one name.
export type SubListener = RelaySubscribeCallbacks

// Mutable transport state for a single REQ in flight on a socket — not a domain value object, which
// is why it lives beside the socket plumbing rather than under domain/value-object.
//
// `reqSentAt` is the wall-clock time the REQ was last put on the wire (re-stamped on every reconnect
// and auth re-issue), so EOSE latency is measured against the live request rather than the original
// subscribe time — a reconnect after a long backoff would otherwise log the whole downtime as latency.
export interface WireSub {
  readonly filters: ReadonlyArray<NostrFilter>
  readonly filterHash: string
  readonly listeners: Set<SubListener>
  eoseFired: boolean
  reqSentAt: number
}
