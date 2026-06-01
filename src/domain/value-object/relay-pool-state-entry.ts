import type { RelayUrl } from "@innis/nostr-core"
import type { BackoffFailureInfo } from "./backoff-record.ts"
import type { RelayStatus } from "./relay-status.ts"

/**
 * Read-only snapshot of one relay's state, as returned by {@link RelayPool.getRelayPoolState}.
 *
 * `status` is the single source of connection state — there is no separate `connected` boolean,
 * because it would only ever be `status === "connected"`. Likewise the backoff cooldown is reported
 * solely by `disabledUntil` (a timestamp, or `null` when not in cooldown); test it with
 * `disabledUntil !== null` rather than expecting a duplicate boolean.
 */
export interface RelayPoolStateEntry {
  /** The relay this entry describes. */
  readonly url: RelayUrl
  /** Connection state — the single source of truth for connectedness. */
  readonly status: RelayStatus
  /** Whether a NIP-42 AUTH handshake has completed on the live socket. */
  readonly authed: boolean
  /** Wall-clock time (ms since epoch) the backoff cooldown lifts, or `null` when not in cooldown. */
  readonly disabledUntil: number | null
  /** Detail of the failure that opened the current cooldown, or `null` when there is none. */
  readonly lastFailure: BackoffFailureInfo | null
  /** Subscriptions live on the open socket. */
  readonly activeSubscriptionCount: number
  /** Subscriptions awaiting a socket open or an AUTH handshake. */
  readonly pendingSubscriptionCount: number
  /** Subscriptions that have closed (capped diagnostic history). */
  readonly pastSubscriptionCount: number
  /** Lifetime count of events this relay has delivered. */
  readonly eventCount: number
  /** Lifetime count of publishes initiated against this relay. */
  readonly publishCount: number
}
