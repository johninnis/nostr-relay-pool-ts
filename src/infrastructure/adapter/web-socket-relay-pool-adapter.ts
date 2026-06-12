import type { NostrEvent, NostrFilter, RelayUrl } from "@innis/nostr-core"
import { normaliseRelayUrl } from "@innis/nostr-core"
import type { AuthHandler } from "../../application/port/auth-handler.ts"
import type { WallClock } from "../../application/port/clock.ts"
import type { Scheduler } from "../../application/port/scheduler.ts"
import { systemWallClock } from "./system-wall-clock-adapter.ts"
import { systemScheduler } from "./system-scheduler-adapter.ts"
import type { ConnectionPool } from "../../application/port/connection-pool.ts"
import type { RelayPool } from "../../application/port/relay-pool.ts"
import type { RelayPoolConfig } from "../../application/port/relay-pool-config.ts"
import { createBackoffTracker } from "../../application/service/backoff-tracker.ts"
import { createLatencyTracker } from "../../application/service/latency-tracker.ts"
import { createSubscribeMany } from "../../application/service/subscribe-many.ts"
import {
  clearClosedSubHistory,
  type PublishHistoryRecord,
  retainPendingPublishHistory,
  type SubHistoryMap,
} from "../../application/service/relay-history.ts"
import type { PublishHistoryEntry, PublishResponse } from "../../domain/value-object/publish-history.ts"
import type { RelayPoolStateEntry } from "../../domain/value-object/relay-pool-state-entry.ts"
import type { RelaySubscriptionEntry } from "../../domain/value-object/relay-subscription.ts"
import type { RelaySubscribeCallbacks, Subscription } from "../../domain/value-object/subscription.ts"
import { createPublish } from "../web-socket/publish.ts"
import { createSubscribe, INACTIVE_SUBSCRIPTION } from "../web-socket/subscribe.ts"
import { createSocketManager, type PendingReconnect } from "../web-socket/socket-manager.ts"
import { closeIntentionally, findWireSub, tearDownSubs } from "../web-socket/relay-state.ts"
import type { RelayState } from "../web-socket/relay-state.ts"
import {
  buildPoolState,
  buildRelayPublishHistory,
  buildRelaySubscriptions,
  type PoolSnapshot,
} from "../web-socket/pool-state-projection.ts"
import type { MessageContext } from "../web-socket/relay-message-handler.ts"
import { isOpen } from "../web-socket/web-socket-helpers.ts"

const DEFAULT_STABLE_CONNECTION_MS = 30_000
const DEFAULT_IDLE_SOCKET_TIMEOUT_MS = 30_000
const DEFAULT_PUBLISH_TIMEOUT_MS = 8_000
const DEFAULT_PENDING_SUB_TIMEOUT_MS = 30_000
const DEFAULT_RELAY_CONNECTION_HARD_TIMEOUT_MS = 12_000
const DEFAULT_AUTH_TIMEOUT_MS = 60_000

/**
 * Construct a {@link RelayPool} backed by the host's `WebSocket`. The sole entry point of the
 * package: wires the backoff tracker, latency tracker, socket manager, and subscribe/publish
 * machinery together over the injected (or default) {@link WallClock} and {@link Scheduler}, and
 * returns the frozen public facade. See {@link RelayPoolConfig} for the options and their defaults.
 */
export const createRelayPool = (config: RelayPoolConfig = {}): RelayPool => {
  const clock: WallClock = config.clock ?? systemWallClock
  const scheduler: Scheduler = config.scheduler ?? systemScheduler
  const stableConnectionMs = config.stableConnectionMs ?? DEFAULT_STABLE_CONNECTION_MS
  const idleSocketTimeoutMs = config.idleSocketTimeoutMs ?? DEFAULT_IDLE_SOCKET_TIMEOUT_MS
  const publishTimeoutMs = config.publishTimeoutMs ?? DEFAULT_PUBLISH_TIMEOUT_MS
  const pendingSubTimeoutMs = config.pendingSubTimeoutMs ?? DEFAULT_PENDING_SUB_TIMEOUT_MS
  const relayConnectionHardTimeoutMs = config.relayConnectionHardTimeoutMs ??
    DEFAULT_RELAY_CONNECTION_HARD_TIMEOUT_MS
  const authTimeoutMs = config.authTimeoutMs ?? DEFAULT_AUTH_TIMEOUT_MS

  const connections = new Map<RelayUrl, RelayState>()
  const relayEventCounts = new Map<RelayUrl, number>()
  const relayPublishCounts = new Map<RelayUrl, number>()
  const attemptedRelays = new Set<RelayUrl>()
  const subHistory: SubHistoryMap = new Map()
  const publishHistory = new Map<RelayUrl, Array<PublishHistoryRecord>>()
  const pendingReconnects = new Map<RelayUrl, PendingReconnect>()
  const connectionChangeListeners = new Set<(url: RelayUrl, connected: boolean) => void>()

  let authHandler: AuthHandler | null = config.onAuthChallenge ?? null
  let poolStateCache: ReadonlyArray<RelayPoolStateEntry> | null = null
  // A relay's `disabled` status and `disabledUntil` are derived live from the clock, so a cached
  // snapshot goes stale the moment the earliest cooldown window lapses even with no intervening
  // mutation. Cap the cache's lifetime at that instant; `null` means nothing is in cooldown and the
  // snapshot is valid until the next mutation invalidates it.
  let poolStateCacheValidUntil: number | null = null
  let gate: (url: RelayUrl) => boolean = () => true
  let disposed = false

  const latencyTracker = createLatencyTracker(config.latency)
  const invalidateCache = (): void => {
    poolStateCache = null
  }
  const backoff = createBackoffTracker({
    onChange: invalidateCache,
    clock,
    initial: config.initialBackoff,
    persistence: config.backoffPersistence,
  })

  const incrementEventCount = (url: RelayUrl): void => {
    relayEventCounts.set(url, (relayEventCounts.get(url) ?? 0) + 1)
    invalidateCache()
  }

  // Lifetime publish tally, mirroring relayEventCounts. Kept separate from publishHistory.length,
  // which is a capped ring and would under-report once a relay passes the history limit.
  const incrementPublishCount = (url: RelayUrl): void => {
    relayPublishCounts.set(url, (relayPublishCounts.get(url) ?? 0) + 1)
    invalidateCache()
  }

  const emitConnectionChange = (url: RelayUrl, connected: boolean): void => {
    // Snapshot is intentional — a listener that calls onConnectionChange's returned disposer
    // during its own dispatch must not skip subsequent listeners.
    for (const listener of [...connectionChangeListeners]) listener(url, connected)
  }

  const buildMessageContext = (state: RelayState, url: RelayUrl): MessageContext => ({
    state,
    url,
    subHistory,
    authHandler: () => authHandler,
    authTimeoutMs,
    clock,
    scheduler,
    onEventReceived: incrementEventCount,
    onEoseLatency: (relayUrl, ms) => latencyTracker.record(relayUrl, ms),
    onStateChange: invalidateCache,
  })

  const socketManager = createSocketManager({
    clock,
    scheduler,
    backoff,
    stableConnectionMs,
    idleSocketTimeoutMs,
    connections,
    pendingReconnects,
    attemptedRelays,
    invalidateCache,
    emitConnectionChange,
    connectionGate: () => gate,
    buildMessageContext,
  })

  // The state owning a sub is usually the live connection, but a relay that dropped mid-sub parks
  // its state on the pending reconnect — and a fresh subscribe during the backoff window can put a
  // new live state alongside it, so ownership is decided by which state actually holds the subId.
  const findOwningState = (url: RelayUrl, subId: string): RelayState | undefined => {
    const live = connections.get(url)
    if (live && findWireSub(live, subId)) return live
    const parked = pendingReconnects.get(url)?.state
    if (parked && findWireSub(parked, subId)) return parked
    return undefined
  }

  const subscribe = createSubscribe({
    clock,
    scheduler,
    pendingSubTimeoutMs,
    subHistory,
    findOwningState,
    invalidateCache,
    isDisposed: () => disposed,
    getOrCreateConnection: socketManager.getOrCreateConnection,
    releaseIfIdle: socketManager.releaseIfIdle,
  })

  const publish = createPublish({
    clock,
    scheduler,
    publishTimeoutMs,
    publishHistory,
    invalidateCache,
    onPublishInitiated: incrementPublishCount,
    getOrCreateConnection: socketManager.getOrCreateConnection,
    releaseIfIdle: socketManager.releaseIfIdle,
  })

  // Public boundary: normalise the raw URL once, then hand the branded RelayUrl to the core
  // subscribe. The internal fan-out (subscribeMany) already holds RelayUrls and calls the core
  // directly, so a relay URL is normalised exactly once on the way in.
  const subscribeByRawUrl = (
    rawUrl: string,
    filters: ReadonlyArray<NostrFilter>,
    callbacks: RelaySubscribeCallbacks,
  ): Subscription => {
    const url = normaliseRelayUrl(rawUrl)
    if (!url) return INACTIVE_SUBSCRIPTION
    return subscribe(url, filters, callbacks)
  }

  const publishByRawUrl = (rawUrl: string, event: NostrEvent): Promise<PublishResponse> => {
    if (disposed) return Promise.resolve({ from: null, ok: false, message: "disposed" })
    const url = normaliseRelayUrl(rawUrl)
    if (!url) return Promise.resolve({ from: null, ok: false, message: "invalid url" })
    return publish(url, event)
  }

  const snapshot = (): PoolSnapshot => ({
    connections,
    attemptedRelays,
    subHistory,
    publishHistory,
    relayEventCounts,
    relayPublishCounts,
    reconnectingUrls: new Set(pendingReconnects.keys()),
    backoff,
  })

  const earliestCooldownExpiry = (entries: ReadonlyArray<RelayPoolStateEntry>): number | null => {
    let earliest: number | null = null
    for (const entry of entries) {
      if (entry.disabledUntil === null) continue
      if (earliest === null || entry.disabledUntil < earliest) earliest = entry.disabledUntil
    }
    return earliest
  }

  const getRelayPoolState = (): ReadonlyArray<RelayPoolStateEntry> => {
    const stale = poolStateCacheValidUntil !== null && clock() >= poolStateCacheValidUntil
    if (!poolStateCache || stale) {
      poolStateCache = buildPoolState(snapshot())
      poolStateCacheValidUntil = earliestCooldownExpiry(poolStateCache)
    }
    return poolStateCache
  }

  const getRelaySubscriptions = (rawUrl: string): ReadonlyArray<RelaySubscriptionEntry> => {
    const url = normaliseRelayUrl(rawUrl)
    if (!url) return []
    return buildRelaySubscriptions(url, { connections, subHistory })
  }

  const getRelayPublishHistory = (rawUrl: string): ReadonlyArray<PublishHistoryEntry> => {
    const url = normaliseRelayUrl(rawUrl)
    if (!url) return []
    return buildRelayPublishHistory(url, { publishHistory })
  }

  const getConnectedRelayUrls = (): ReadonlyArray<RelayUrl> => {
    const urls: Array<RelayUrl> = []
    for (const [url, state] of connections) {
      if (isOpen(state.ws)) urls.push(url)
    }
    return urls
  }

  const getAttemptedRelayUrls = (): ReadonlyArray<RelayUrl> => [...attemptedRelays]

  const setAuthHandler = (handler: AuthHandler): void => {
    authHandler = handler
  }

  const clearDisabled = (rawUrl: string): void => {
    const url = normaliseRelayUrl(rawUrl)
    if (!url) return
    backoff.clear(url)
    // A relay that dropped while it still had subscriptions sits in pendingReconnects: fire its
    // reconnect now rather than wait out the (just-cleared) cooldown, so its subs are re-established
    // at once. A relay with no queued reconnect has nothing to revive — clearing the disabled flag
    // is enough; the next subscribe or publish opens the socket on demand, as everywhere else.
    pendingReconnects.get(url)?.fire()
  }

  const tearDownStateSubs = (url: RelayUrl, state: RelayState, reason: string): void =>
    tearDownSubs({ state, url, subHistory, clock, scheduler, reason })

  // Settle every publish still awaiting an ack on a socket the pool is taking down. Each settle clears
  // its own timeout and drops its in-flight record, so the relay's onclose finds an empty queue. Every
  // teardown path (dispose, disconnect, gate-reject) pre-settles here rather than leaning on the async
  // onclose, so an awaited publish promise resolves in the same turn as the teardown call.
  const settleInFlightPublishes = (state: RelayState, message: string): void => {
    for (const inFlight of [...state.inFlightPublishes.values()]) inFlight.settle({ ok: false, message })
  }

  const disconnect = (rawUrl: string): void => {
    const url = normaliseRelayUrl(rawUrl)
    if (!url) return
    // A relay that dropped while it still had subscriptions sits in pendingReconnects with no live
    // socket; without cancelling that timer the pool would silently revive a relay the caller just
    // asked to disconnect.
    socketManager.cancelPendingReconnect(url)
    const state = connections.get(url)
    if (!state) return
    tearDownStateSubs(url, state, "disconnected")
    settleInFlightPublishes(state, "disconnected")
    closeIntentionally(state)
  }

  const setConnectionGate = (next: (url: RelayUrl) => boolean): void => {
    gate = next
    for (const [url, state] of [...connections]) {
      if (gate(url)) continue
      tearDownStateSubs(url, state, "connection gate rejected")
      settleInFlightPublishes(state, "disconnected")
      closeIntentionally(state)
    }
    // A relay that dropped with live subs sits in pendingReconnects with no socket in `connections`,
    // so the loop above misses it. Cancel any queued reconnect the new gate now rejects — otherwise
    // it would keep reporting `reconnecting` until the timer fired into a gate-blocked no-op.
    for (const url of [...pendingReconnects.keys()]) {
      if (!gate(url)) socketManager.cancelPendingReconnect(url)
    }
    invalidateCache()
  }

  const clearRelayHistory = (rawUrl: string): void => {
    const url = normaliseRelayUrl(rawUrl)
    if (!url) return
    clearClosedSubHistory(subHistory, url)
    retainPendingPublishHistory(publishHistory, url)
    relayEventCounts.delete(url)
    relayPublishCounts.delete(url)
    invalidateCache()
  }

  const onConnectionChange = (listener: (url: RelayUrl, connected: boolean) => void): () => void => {
    connectionChangeListeners.add(listener)
    return (): void => {
      connectionChangeListeners.delete(listener)
    }
  }

  const suggestedTimeoutByUrl = (url: RelayUrl): number => latencyTracker.suggestedTimeout(url)

  const suggestedTimeout = (rawUrl: string): number => {
    const url = normaliseRelayUrl(rawUrl)
    if (url === null) return latencyTracker.defaultTimeoutMs
    return suggestedTimeoutByUrl(url)
  }

  const connectionPool: ConnectionPool = {
    subscribe,
    suggestedTimeout: suggestedTimeoutByUrl,
  }

  const subscribeMany = createSubscribeMany({
    connectionPool,
    scheduler,
    hardTimeoutMs: relayConnectionHardTimeoutMs,
  })

  const dispose = (): void => {
    if (disposed) return
    disposed = true
    for (const [url, state] of [...connections]) {
      tearDownStateSubs(url, state, "disposed")
      if (state.stabilityTimer !== null) {
        scheduler.clearTimer(state.stabilityTimer)
        state.stabilityTimer = null
      }
      if (state.idleTimer !== null) {
        scheduler.clearTimer(state.idleTimer)
        state.idleTimer = null
      }
      settleInFlightPublishes(state, "disposed")
      closeIntentionally(state)
    }
    connections.clear()
    for (const reconnect of pendingReconnects.values()) reconnect.cancel()
    pendingReconnects.clear()
    connectionChangeListeners.clear()
    // Drop the diagnostic stores too: a disposed pool serves no further reads, so retaining them
    // would only pin memory for a reference the host is expected to release.
    attemptedRelays.clear()
    relayEventCounts.clear()
    relayPublishCounts.clear()
    subHistory.clear()
    publishHistory.clear()
    invalidateCache()
  }

  return Object.freeze({
    subscribe: subscribeByRawUrl,
    subscribeMany,
    publish: publishByRawUrl,
    getConnectedRelayUrls,
    getAttemptedRelayUrls,
    getRelayPoolState,
    getRelaySubscriptions,
    getRelayPublishHistory,
    setAuthHandler,
    clearDisabled,
    disconnect,
    setConnectionGate,
    clearRelayHistory,
    onConnectionChange,
    suggestedTimeout,
    dispose,
  })
}
