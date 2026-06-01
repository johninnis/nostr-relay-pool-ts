import type { RelayUrl } from "@innis/nostr-core"

/** Diagnostic detail of the most recent connection failure that drove a relay into backoff. */
export interface BackoffFailureInfo {
  /** WebSocket close code, or `null` when the socket never opened (construction threw). */
  readonly code: number | null
  /** Relay-supplied close reason, or the constructor error message; `""` when none was given. */
  readonly reason: string
  /** Wall-clock time (ms since epoch) the failure was recorded. */
  readonly at: number
}

/**
 * A relay's persisted backoff state. Supplied via `initialBackoff` to restore cooldowns across
 * restarts, and handed to {@link BackoffPersistence} as the tracker mutates them.
 */
export interface BackoffRecord {
  /** The relay this cooldown applies to. */
  readonly url: RelayUrl
  /** Wall-clock time (ms since epoch) the relay may be retried; a past value means not in cooldown. */
  readonly disabledUntil: number
  /** Index into the backoff schedule — how many consecutive failures have elapsed. */
  readonly step: number
  /** Detail of the failure that opened this cooldown, when known. */
  readonly lastFailure?: BackoffFailureInfo
}
