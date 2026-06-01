import type { EventId, NostrFilter, RelayUrl } from "@innis/nostr-core"
import type { WallClock } from "../port/clock.ts"
import type { PublishOutcome } from "../../domain/value-object/publish-history.ts"

/**
 * Internal mutable record the pool maintains as a subscription's life proceeds:
 * `closedAt` is stamped on close, `eventCount` ticks per EVENT. Never crosses the public API —
 * consumers see the read-only {@link RelaySubscriptionEntry} projection.
 */
export interface SubHistoryRecord {
  readonly subId: string
  readonly filters: ReadonlyArray<NostrFilter>
  readonly openedAt: number
  closedAt: number | null
  eventCount: number
}

/**
 * Internal mutable record maintained by the pool. The pool flips `result` and `message` once
 * the relay replies (or the publish times out). Never crosses the public API boundary —
 * consumers see the read-only {@link PublishHistoryEntry} projection.
 */
export interface PublishHistoryRecord {
  readonly eventId: EventId
  readonly kind: number
  readonly publishedAt: number
  result: PublishOutcome
  message: string
}

export type SubHistoryMap = Map<RelayUrl, Map<string, SubHistoryRecord>>

// Per-relay diagnostic ring buffers — recent activity for inspection, not an audit log. The two
// caps are equal but separate: they bound different things (publishes trim wholesale; subscriptions
// evict closed entries only, never live ones), so they are not the same policy under one name.
export const PUBLISH_HISTORY_LIMIT = 100
export const SUB_HISTORY_LIMIT = 100

export interface RecordSubHistoryInput {
  readonly subHistory: SubHistoryMap
  readonly url: RelayUrl
  readonly subId: string
  readonly filters: ReadonlyArray<NostrFilter>
  readonly openedAt: number
}

// Evict the oldest *closed* entries (Map iteration is insertion order) until back within the
// limit. Active and pending subscriptions are never dropped — their history is still live — so a
// relay holding more than `SUB_HISTORY_LIMIT` open subscriptions stays above the cap by design.
const pruneClosedSubHistory = (entries: Map<string, SubHistoryRecord>): void => {
  if (entries.size <= SUB_HISTORY_LIMIT) return
  for (const [subId, entry] of entries) {
    if (entries.size <= SUB_HISTORY_LIMIT) return
    if (entry.closedAt !== null) entries.delete(subId)
  }
}

export const recordSubHistory = (
  { subHistory, url, subId, filters, openedAt }: RecordSubHistoryInput,
): void => {
  let entries = subHistory.get(url)
  if (!entries) {
    entries = new Map()
    subHistory.set(url, entries)
  }
  entries.set(subId, { subId, filters, openedAt, closedAt: null, eventCount: 0 })
  pruneClosedSubHistory(entries)
}

export interface CloseSubHistoryInput {
  readonly subHistory: ReadonlyMap<RelayUrl, Map<string, SubHistoryRecord>>
  readonly url: RelayUrl
  readonly subId: string
  readonly clock: WallClock
}

export const closeSubHistory = (input: CloseSubHistoryInput): void => {
  const { subHistory, url, subId, clock } = input
  const entry = subHistory.get(url)?.get(subId)
  if (entry && entry.closedAt === null) entry.closedAt = clock()
}

export const recordPublishEntry = (
  publishHistory: Map<RelayUrl, Array<PublishHistoryRecord>>,
  url: RelayUrl,
  record: PublishHistoryRecord,
): void => {
  let entries = publishHistory.get(url)
  if (!entries) {
    entries = []
    publishHistory.set(url, entries)
  }
  entries.push(record)
  if (entries.length > PUBLISH_HISTORY_LIMIT) {
    entries.splice(0, entries.length - PUBLISH_HISTORY_LIMIT)
  }
}

// Drop every *closed* subscription from a relay's history, leaving live (active/pending) entries
// untouched — the diagnostic-reset counterpart to recordSubHistory. Keeps all history-store
// mutation in this module rather than letting callers reach into the maps directly.
export const clearClosedSubHistory = (
  subHistory: ReadonlyMap<RelayUrl, Map<string, SubHistoryRecord>>,
  url: RelayUrl,
): void => {
  const entries = subHistory.get(url)
  if (!entries) return
  for (const [subId, entry] of entries) {
    if (entry.closedAt !== null) entries.delete(subId)
  }
}

// Drop every *settled* publish from a relay's history, retaining only the still-pending ones whose
// resolvers are live. The settled records carry no further state, so clearing them is safe.
export const retainPendingPublishHistory = (
  publishHistory: ReadonlyMap<RelayUrl, Array<PublishHistoryRecord>>,
  url: RelayUrl,
): void => {
  const entries = publishHistory.get(url)
  if (!entries) return
  const pending = entries.filter((record) => record.result === "pending")
  entries.length = 0
  entries.push(...pending)
}
