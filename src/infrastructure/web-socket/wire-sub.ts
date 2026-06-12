import type { CompiledFilter, NostrFilter } from "@innis/nostr-core"
import type { RelaySubscribeCallbacks } from "../../domain/value-object/subscription.ts"

// A wire subscription's listeners are exactly the public per-subscribe callbacks: the pool fans one
// relay sub out to every caller that deduplicated onto the same filter hash. One shape, one name.
export type SubListener = RelaySubscribeCallbacks

// Mutable transport state for a single REQ in flight on a socket — not a domain value object, which
// is why it lives beside the socket plumbing rather than under domain/value-object.
//
// `compiled` is `filters` compiled once at construction, so the per-event match in the message
// handler never recompiles; `filters` itself is kept for REQ serialisation and history.
//
// `listeners` is copy-on-write: never mutated in place, only replaced with a new array. Dispatch
// sites iterate the array directly — `for...of` evaluates the expression once, so a re-entrant
// subscribe/unsubscribe that replaces the array cannot affect the in-flight iteration.
//
// `reqSentAt` is the wall-clock time the REQ was last put on the wire (re-stamped on every reconnect
// and auth re-issue), so EOSE latency is measured against the live request rather than the original
// subscribe time — a reconnect after a long backoff would otherwise log the whole downtime as latency.
export interface WireSub {
  readonly filters: ReadonlyArray<NostrFilter>
  readonly filterHash: string
  readonly compiled: CompiledFilter
  listeners: ReadonlyArray<SubListener>
  eoseFired: boolean
  reqSentAt: number
}
