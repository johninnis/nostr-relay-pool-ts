import type { EventId, RelayUrl } from "@innis/nostr-core"

/**
 * Terminal state of a publish: `pending` until the relay replies, then `ok` (relay accepted),
 * `failed` (relay rejected via `OK false`), or `timeout` (no reply before the deadline / socket dropped).
 */
export type PublishOutcome = "pending" | "ok" | "failed" | "timeout"

/**
 * The pool's reply to `RelayPool.publish`. `from` is the normalised relay URL the publish was
 * targeted at — `null` only when the URL failed to parse.
 */
export interface PublishResponse {
  /** The normalised relay the publish targeted; `null` only when the raw URL failed to parse. */
  readonly from: RelayUrl | null
  /** `true` if the relay accepted the event (`OK true`). */
  readonly ok: boolean
  /** The relay's `OK` message, or a pool-supplied reason (`"timeout"`, `"disconnected"`, `"invalid url"`). */
  readonly message: string
}

/**
 * Read-only projection of one entry in a relay's publish history.
 * Returned by {@link RelayPool.getRelayPublishHistory}; the pool's internal store
 * (`PublishHistoryRecord`) is structurally assignable to this shape.
 */
export interface PublishHistoryEntry {
  /** Id of the published event. */
  readonly eventId: EventId
  /** Kind of the published event. */
  readonly kind: number
  /** Wall-clock time (ms since epoch) the publish was initiated. */
  readonly publishedAt: number
  /** Current outcome of the publish. */
  readonly result: PublishOutcome
  /** The relay's `OK` message, or the pool's reason once the publish settled. */
  readonly message: string
}
