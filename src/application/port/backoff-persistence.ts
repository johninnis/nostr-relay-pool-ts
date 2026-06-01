import type { RelayUrl } from "@innis/nostr-core"
import type { BackoffRecord } from "../../domain/value-object/backoff-record.ts"

/**
 * Optional sink for backoff state so cooldowns survive a restart. The pool calls `write` whenever a
 * relay's cooldown changes and `remove` when it clears; a host persists these and replays them via
 * `initialBackoff`.
 */
export interface BackoffPersistence {
  /** Persist (insert or replace) the cooldown record for `record.url`. */
  readonly write: (record: BackoffRecord) => void
  /** Delete any persisted cooldown record for this relay. */
  readonly remove: (url: RelayUrl) => void
}
