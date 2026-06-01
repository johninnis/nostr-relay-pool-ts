import type { RelayUrl } from "@innis/nostr-core"
import type { BackoffTracker } from "../../application/service/backoff-tracker.ts"
import type { PublishHistoryRecord, SubHistoryRecord } from "../../application/service/relay-history.ts"
import type { PublishHistoryEntry } from "../../domain/value-object/publish-history.ts"
import type { RelayPoolStateEntry } from "../../domain/value-object/relay-pool-state-entry.ts"
import type { RelaySubscriptionEntry, SubscriptionStatus } from "../../domain/value-object/relay-subscription.ts"
import type { RelayStatus } from "../../domain/value-object/relay-status.ts"
import type { WireSub } from "./wire-sub.ts"
import type { RelayState } from "./relay-state.ts"
import { isConnecting, isOpen } from "./web-socket-helpers.ts"

export interface PoolSnapshot {
  readonly connections: ReadonlyMap<RelayUrl, RelayState>
  readonly attemptedRelays: ReadonlySet<RelayUrl>
  readonly subHistory: ReadonlyMap<RelayUrl, Map<string, SubHistoryRecord>>
  readonly publishHistory: ReadonlyMap<RelayUrl, ReadonlyArray<PublishHistoryRecord>>
  readonly relayEventCounts: ReadonlyMap<RelayUrl, number>
  readonly relayPublishCounts: ReadonlyMap<RelayUrl, number>
  readonly reconnectingUrls: ReadonlySet<RelayUrl>
  readonly backoff: BackoffTracker
}

const isSocketOpen = (state: RelayState | undefined): boolean => isOpen(state?.ws ?? null)

export const computeRelayStatus = (state: RelayState): RelayStatus => {
  if (isOpen(state.ws)) return "connected"
  if (isConnecting(state.ws)) return "connecting"
  return "disconnected"
}

interface SubscriptionCounts {
  readonly active: number
  readonly pending: number
  readonly closed: number
}

// The single source of truth for "what subscriptions does this relay have, and in what state".
// Active subs come from the live map, pending from the not-yet-open and auth-parked maps, and any
// history entry not represented in those maps is a past sub (closed) or an orphaned pending one.
// Both the counts and the detailed listing derive from this one walk.
const classifySubscriptions = (
  state: RelayState | undefined,
  history: ReadonlyMap<string, SubHistoryRecord> | undefined,
): ReadonlyArray<RelaySubscriptionEntry> => {
  const accounted = new Set<string>()
  const result: Array<RelaySubscriptionEntry> = []

  const collectFromMap = (map: ReadonlyMap<string, WireSub>, status: SubscriptionStatus): void => {
    for (const [subId, sub] of map) {
      if (accounted.has(subId)) continue
      accounted.add(subId)
      const historyEntry = history?.get(subId)
      result.push({
        subId,
        filters: sub.filters,
        status,
        openedAt: historyEntry?.openedAt ?? 0,
        eventCount: historyEntry?.eventCount ?? 0,
      })
    }
  }

  if (state) {
    collectFromMap(state.subs, "active")
    collectFromMap(state.pendingSubs, "pending")
    collectFromMap(state.pendingAuthSubs, "pending")
  }

  if (history) {
    for (const [, entry] of history) {
      if (accounted.has(entry.subId)) continue
      accounted.add(entry.subId)
      result.push({
        subId: entry.subId,
        filters: entry.filters,
        status: entry.closedAt !== null ? "closed" : "pending",
        openedAt: entry.openedAt,
        closedAt: entry.closedAt,
        eventCount: entry.eventCount,
      })
    }
  }

  return result
}

const countSubscriptions = (
  state: RelayState | undefined,
  history: ReadonlyMap<string, SubHistoryRecord> | undefined,
): SubscriptionCounts => {
  let active = 0
  let pending = 0
  let closed = 0
  for (const entry of classifySubscriptions(state, history)) {
    if (entry.status === "active") active++
    else if (entry.status === "pending") pending++
    else closed++
  }
  return { active, pending, closed }
}

const resolveStatus = (
  url: RelayUrl,
  state: RelayState | undefined,
  snapshot: PoolSnapshot,
): RelayStatus => {
  if (!isSocketOpen(state)) {
    // A pending reconnect means the relay dropped while it still had subscriptions and the pool
    // is scheduled to revive it — more informative than the "disabled" backoff window it sits in.
    if (snapshot.reconnectingUrls.has(url)) return "reconnecting"
    if (snapshot.backoff.isDisabled(url)) return "disabled"
  }
  return state ? computeRelayStatus(state) : "disconnected"
}

const buildEntry = (
  url: RelayUrl,
  state: RelayState | undefined,
  snapshot: PoolSnapshot,
): RelayPoolStateEntry => {
  const counts = countSubscriptions(state, snapshot.subHistory.get(url))
  return {
    url,
    status: resolveStatus(url, state, snapshot),
    authed: state?.authed ?? false,
    disabledUntil: snapshot.backoff.disabledUntil(url),
    lastFailure: snapshot.backoff.getLastFailure(url),
    activeSubscriptionCount: counts.active,
    pendingSubscriptionCount: counts.pending,
    pastSubscriptionCount: counts.closed,
    eventCount: snapshot.relayEventCounts.get(url) ?? 0,
    publishCount: snapshot.relayPublishCounts.get(url) ?? 0,
  }
}

export const buildPoolState = (snapshot: PoolSnapshot): ReadonlyArray<RelayPoolStateEntry> => {
  const entries = new Map<RelayUrl, RelayPoolStateEntry>()
  for (const [url, state] of snapshot.connections) entries.set(url, buildEntry(url, state, snapshot))
  for (const url of snapshot.attemptedRelays) {
    if (!entries.has(url)) entries.set(url, buildEntry(url, undefined, snapshot))
  }
  for (const url of snapshot.backoff.getDisabledUrls()) {
    if (!entries.has(url)) entries.set(url, buildEntry(url, undefined, snapshot))
  }
  // Stable, intent-free order: connections first (insertion order), then attempt-only relays, then
  // backoff-disabled ones. The pool ranks nothing — every entry carries an `eventCount`, and any
  // ordering policy (by latency, by event volume, by allow-list) belongs to the selection layer.
  return [...entries.values()]
}

type SubscriptionSnapshot = Pick<PoolSnapshot, "connections" | "subHistory">

export const buildRelaySubscriptions = (
  url: RelayUrl,
  snapshot: SubscriptionSnapshot,
): ReadonlyArray<RelaySubscriptionEntry> =>
  classifySubscriptions(snapshot.connections.get(url), snapshot.subHistory.get(url))

// Snapshot each record by value, newest first. The internal record is mutated in place when its
// publish later settles, so returning the live object would leak that mutation to the caller.
export const buildRelayPublishHistory = (
  url: RelayUrl,
  snapshot: Pick<PoolSnapshot, "publishHistory">,
): ReadonlyArray<PublishHistoryEntry> =>
  (snapshot.publishHistory.get(url) ?? [])
    .map((record): PublishHistoryEntry => ({
      eventId: record.eventId,
      kind: record.kind,
      publishedAt: record.publishedAt,
      result: record.result,
      message: record.message,
    }))
    .reverse()
