import type { EventId, NostrFilter, RelayUrl, SubscriptionId } from "@innis/nostr-core"
import type { WallClock } from "../port/clock.ts"
import type { PublishOutcome } from "../../domain/value-object/publish-history.ts"

export interface SubHistoryRecord {
  readonly subId: SubscriptionId
  readonly filters: ReadonlyArray<NostrFilter>
  readonly openedAt: number
  closedAt: number | null
  eventCount: number
}

export interface PublishHistoryRecord {
  readonly eventId: EventId
  readonly kind: number
  readonly publishedAt: number
  result: PublishOutcome
  message: string
}

export type SubHistoryMap = Map<RelayUrl, Map<SubscriptionId, SubHistoryRecord>>

export const PUBLISH_HISTORY_LIMIT = 100
export const SUB_HISTORY_LIMIT = 100

export interface RecordSubHistoryInput {
  readonly subHistory: SubHistoryMap
  readonly url: RelayUrl
  readonly subId: SubscriptionId
  readonly filters: ReadonlyArray<NostrFilter>
  readonly openedAt: number
}

const pruneClosedSubHistory = (entries: Map<SubscriptionId, SubHistoryRecord>): void => {
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
  readonly subHistory: ReadonlyMap<RelayUrl, Map<SubscriptionId, SubHistoryRecord>>
  readonly url: RelayUrl
  readonly subId: SubscriptionId
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

export const clearClosedSubHistory = (
  subHistory: ReadonlyMap<RelayUrl, Map<SubscriptionId, SubHistoryRecord>>,
  url: RelayUrl,
): void => {
  const entries = subHistory.get(url)
  if (!entries) return
  for (const [subId, entry] of entries) {
    if (entry.closedAt !== null) entries.delete(subId)
  }
}

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
