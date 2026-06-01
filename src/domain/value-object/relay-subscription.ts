import type { NostrFilter } from "@innis/nostr-core"

/**
 * State of one subscription: `active` (live on the open socket), `pending` (awaiting a socket open
 * or AUTH handshake), or `closed` (terminated — EOSE-driven teardown, relay `CLOSED`, or pool teardown).
 */
export type SubscriptionStatus = "active" | "pending" | "closed"

/**
 * Read-only projection of one subscription as observed via {@link RelayPool.getRelaySubscriptions}.
 * The pool's internal `SubHistoryRecord` is structurally assignable to this shape.
 */
export interface RelaySubscriptionEntry {
  /** The pool-assigned wire subscription id (`pool-N`). */
  readonly subId: string
  /** The filters this subscription requested. */
  readonly filters: ReadonlyArray<NostrFilter>
  /** Current state of the subscription. */
  readonly status: SubscriptionStatus
  /** Wall-clock time (ms since epoch) the subscription was opened. */
  readonly openedAt: number
  /** Wall-clock time (ms since epoch) the subscription closed; `null`/absent while still live. */
  readonly closedAt?: number | null
  /** Count of events delivered on this subscription. */
  readonly eventCount: number
}
