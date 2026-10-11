import type { NostrEvent, NostrFilter, RelayUrl, SubscriptionId } from "@innis/nostr-core"
import { parseRelayUrl } from "@innis/nostr-core"
import type { AuthHandler } from "../../application/port/auth-handler.ts"
import type { WallClock } from "../../application/port/clock.ts"
import type { Scheduler } from "../../application/port/scheduler.ts"
import { systemWallClock } from "../time/system-wall-clock.ts"
import { systemScheduler } from "../time/system-scheduler.ts"
import type { ConnectionPool } from "../../application/port/connection-pool.ts"
import type { RelayPool } from "../../application/port/relay-pool.ts"
import type { RelayPoolConfig } from "../../application/port/relay-pool-config.ts"
import type { RelayInformationLookup } from "../../application/port/relay-information-lookup.ts"
import { createBackoffTracker } from "../../application/service/backoff-tracker.ts"
import { createLatencyTracker } from "../../application/service/latency-tracker.ts"
import { createSubscribeMany, createSubscribeManyLive } from "../../application/service/subscribe-many.ts"
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
import { createPublish } from "./publish.ts"
import { createSubscribe, INACTIVE_SUBSCRIPTION } from "./subscribe.ts"
import { createSocketManager, type PendingReconnect } from "./socket-manager.ts"
import {
  abandonAuth,
  closeIntentionally,
  findWireSub,
  settleInFlightPublishes,
  stopConnectionTimers,
  tearDownSubs,
} from "./relay-state.ts"
import type { RelayState } from "./relay-state.ts"
import {
  buildPoolState,
  buildRelayPublishHistory,
  buildRelaySubscriptions,
  type PoolSnapshot,
} from "./pool-state-projection.ts"
import type { MessageContext } from "./relay-message-handler.ts"
import { isOpen } from "./web-socket-helpers.ts"

const DEFAULT_STABLE_CONNECTION_MS = 30_000
const DEFAULT_IDLE_SOCKET_TIMEOUT_MS = 30_000
const DEFAULT_PUBLISH_TIMEOUT_MS = 8_000
const DEFAULT_PENDING_SUB_TIMEOUT_MS = 30_000
const DEFAULT_RELAY_CONNECTION_HARD_TIMEOUT_MS = 12_000
const DEFAULT_AUTH_TIMEOUT_MS = 60_000
const DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000
const DEFAULT_MAX_MESSAGE_BYTES = 262_144
const NO_RELAY_INFORMATION: RelayInformationLookup = () => null

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
  const heartbeatIntervalMs = config.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS
  const maxMessageBytes = config.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES
  const relayInformation = config.relayInformation ?? NO_RELAY_INFORMATION

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

  const incrementPublishCount = (url: RelayUrl): void => {
    relayPublishCounts.set(url, (relayPublishCounts.get(url) ?? 0) + 1)
    invalidateCache()
  }

  const emitConnectionChange = (url: RelayUrl, connected: boolean): void => {
    const listenersBeforeDispatch = [...connectionChangeListeners]
    for (const listener of listenersBeforeDispatch) listener(url, connected)
  }

  const buildMessageContext = (state: RelayState, url: RelayUrl): MessageContext => ({
    state,
    url,
    subHistory,
    authHandler: () => authHandler,
    authTimeoutMs,
    maxMessageBytes,
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
    heartbeatIntervalMs,
    connections,
    pendingReconnects,
    attemptedRelays,
    invalidateCache,
    emitConnectionChange,
    connectionGate: () => gate,
    buildMessageContext,
  })

  const findOwningState = (url: RelayUrl, subId: SubscriptionId): RelayState | undefined =>
    [connections.get(url), pendingReconnects.get(url)?.droppedState].find(
      (state) => state !== undefined && findWireSub(state, subId) !== undefined,
    )

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
    relayInformation,
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

  const subscribeByRawUrl = (
    rawUrl: string,
    filters: ReadonlyArray<NostrFilter>,
    callbacks: RelaySubscribeCallbacks,
  ): Subscription => {
    const url = parseRelayUrl(rawUrl)
    if (!url) return INACTIVE_SUBSCRIPTION
    return subscribe(url, filters, callbacks)
  }

  const publishByRawUrl = (rawUrl: string, event: NostrEvent): Promise<PublishResponse> => {
    if (disposed) return Promise.resolve({ from: null, ok: false, message: "disposed" })
    const url = parseRelayUrl(rawUrl)
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
    const url = parseRelayUrl(rawUrl)
    if (!url) return []
    return buildRelaySubscriptions(url, { connections, subHistory })
  }

  const getRelayPublishHistory = (rawUrl: string): ReadonlyArray<PublishHistoryEntry> => {
    const url = parseRelayUrl(rawUrl)
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
    const url = parseRelayUrl(rawUrl)
    if (!url) return
    backoff.clear(url)
    pendingReconnects.get(url)?.fire()
  }

  const tearDownStateSubs = (url: RelayUrl, state: RelayState, reason: string): void =>
    tearDownSubs({ state, url, subHistory, clock, scheduler, reason })

  const disconnect = (rawUrl: string): void => {
    const url = parseRelayUrl(rawUrl)
    if (!url) return
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
    for (const url of [...pendingReconnects.keys()]) {
      if (!gate(url)) socketManager.cancelPendingReconnect(url)
    }
    invalidateCache()
  }

  const clearRelayHistory = (rawUrl: string): void => {
    const url = parseRelayUrl(rawUrl)
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
    const url = parseRelayUrl(rawUrl)
    if (url === null) return latencyTracker.defaultTimeoutMs
    return suggestedTimeoutByUrl(url)
  }

  const connectionPool: ConnectionPool = {
    subscribe,
    suggestedTimeout: suggestedTimeoutByUrl,
  }

  const fanOutDeps = { connectionPool, scheduler, hardTimeoutMs: relayConnectionHardTimeoutMs }
  const subscribeMany = createSubscribeMany(fanOutDeps)
  const subscribeManyLive = createSubscribeManyLive(fanOutDeps)

  const forgetDiagnostics = (): void => {
    attemptedRelays.clear()
    relayEventCounts.clear()
    relayPublishCounts.clear()
    subHistory.clear()
    publishHistory.clear()
  }

  const dispose = (): void => {
    if (disposed) return
    disposed = true
    for (const [url, state] of [...connections]) {
      tearDownStateSubs(url, state, "disposed")
      stopConnectionTimers(state, scheduler)
      abandonAuth(state, scheduler)
      settleInFlightPublishes(state, "disposed")
      closeIntentionally(state)
    }
    connections.clear()
    for (const reconnect of pendingReconnects.values()) reconnect.cancel()
    pendingReconnects.clear()
    connectionChangeListeners.clear()
    forgetDiagnostics()
    invalidateCache()
  }

  return Object.freeze({
    subscribe: subscribeByRawUrl,
    subscribeMany,
    subscribeManyLive,
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
