import type { NostrFilter, RelayUrl } from "@innis/nostr-core"
import { compileFilters, hashFilters, serialiseCloseMessage } from "@innis/nostr-core"
import type { WallClock } from "../../application/port/clock.ts"
import type { Scheduler } from "../../application/port/scheduler.ts"
import { closeSubHistory, recordSubHistory, type SubHistoryMap } from "../../application/service/relay-history.ts"
import type { RelaySubscribeCallbacks, Subscription } from "../../domain/value-object/subscription.ts"
import type { SubListener, WireSub } from "./wire-sub.ts"
import type { RelayState } from "./relay-state.ts"
import { findWireSub, reissueReq } from "./relay-state.ts"
import { clearTimerEntry, isOpen, sendOnWebSocket } from "./web-socket-helpers.ts"

export const INACTIVE_SUBSCRIPTION: Subscription = Object.freeze({ active: false, unsubscribe: (): void => {} })

export interface SubscribeDeps {
  readonly clock: WallClock
  readonly scheduler: Scheduler
  readonly pendingSubTimeoutMs: number
  readonly subHistory: SubHistoryMap
  /**
   * Resolve the state that owns `subId` — the live connection, or the state parked on a pending
   * reconnect after its socket dropped. Unsubscribe must reach the parked state too: a sub left
   * there would be resurrected by the reconnect as a REQ no caller can ever close.
   */
  readonly findOwningState: (url: RelayUrl, subId: string) => RelayState | undefined
  readonly invalidateCache: () => void
  readonly isDisposed: () => boolean
  readonly getOrCreateConnection: (url: RelayUrl) => RelayState | null
  readonly releaseIfIdle: (url: RelayUrl, state: RelayState) => void
}

export const createSubscribe = (deps: SubscribeDeps) => {
  const {
    clock,
    scheduler,
    pendingSubTimeoutMs,
    subHistory,
    findOwningState,
    invalidateCache,
    isDisposed,
    getOrCreateConnection,
    releaseIfIdle,
  } = deps
  let subCounter = 0
  const nextSubId = (): string => `pool-${++subCounter}`

  const schedulePendingSubTimeout = (state: RelayState, url: RelayUrl, subId: string): void => {
    const timeoutId = scheduler.setTimer(() => {
      const wireSub = state.pendingSubs.get(subId)
      if (!wireSub) return
      state.pendingSubs.delete(subId)
      state.pendingSubTimeouts.delete(subId)
      if (state.subIdByFilterHash.get(wireSub.filterHash) === subId) {
        state.subIdByFilterHash.delete(wireSub.filterHash)
      }
      // The socket never opened in time, so this sub is dropped from pendingSubs and will not be
      // re-issued — it is dead and will not revive. Fire the terminal onClosed, as every other
      // death path does, rather than leaving the subscriber with no signal.
      for (const listener of wireSub.listeners) listener.onClosed?.("timeout")
      closeSubHistory({ subHistory, url, subId, clock })
      invalidateCache()
      releaseIfIdle(url, state)
    }, pendingSubTimeoutMs)
    state.pendingSubTimeouts.set(subId, timeoutId)
  }

  const subscribe = (
    url: RelayUrl,
    filters: ReadonlyArray<NostrFilter>,
    callbacks: RelaySubscribeCallbacks,
  ): Subscription => {
    if (isDisposed()) return INACTIVE_SUBSCRIPTION
    const state = getOrCreateConnection(url)
    if (!state) return INACTIVE_SUBSCRIPTION

    const filterHash = hashFilters(filters)
    const listener: SubListener = {
      onEvent: callbacks.onEvent,
      onEose: callbacks.onEose,
      onClosed: callbacks.onClosed,
    }

    const existingSubId = state.subIdByFilterHash.get(filterHash)
    const existingWireSub = existingSubId !== undefined ? findWireSub(state, existingSubId) : undefined

    let subId: string
    let wireSub: WireSub

    if (existingWireSub && existingSubId !== undefined) {
      subId = existingSubId
      wireSub = existingWireSub
      wireSub.listeners = [...wireSub.listeners, listener]
      // Synthetic catch-up EOSE for a late joiner. Re-check membership on the microtask: a caller
      // that unsubscribes synchronously has already left the listener array and must not be signalled.
      if (wireSub.eoseFired) {
        queueMicrotask(() => {
          if (wireSub.listeners.includes(listener)) listener.onEose?.()
        })
      }
    } else {
      subId = nextSubId()
      wireSub = {
        filters,
        filterHash,
        compiled: compileFilters(filters),
        listeners: [listener],
        eoseFired: false,
        reqSentAt: 0,
      }
      state.subIdByFilterHash.set(filterHash, subId)

      const ws = state.ws
      if (ws !== null && isOpen(ws)) {
        state.subs.set(subId, wireSub)
        reissueReq({ ws, subId, sub: wireSub, clock })
      } else {
        state.pendingSubs.set(subId, wireSub)
        schedulePendingSubTimeout(state, url, subId)
      }

      recordSubHistory({ subHistory, url, subId, filters, openedAt: clock() })
    }

    invalidateCache()

    const unsubscribe = (): void => {
      // Operate on the closure's wireSub, not a fresh lookup: it is the same object wherever it
      // currently lives (live state, or parked on a pending reconnect), so the listener removal
      // sticks even while the relay's socket is down.
      wireSub.listeners = wireSub.listeners.filter((l) => l !== listener)
      if (wireSub.listeners.length > 0) {
        invalidateCache()
        return
      }

      const owningState = findOwningState(url, subId)
      if (owningState && findWireSub(owningState, subId) === wireSub) {
        const wasPending = owningState.pendingSubs.has(subId) || owningState.pendingAuthSubs.has(subId)
        owningState.subs.delete(subId)
        owningState.pendingSubs.delete(subId)
        owningState.pendingAuthSubs.delete(subId)
        if (owningState.subIdByFilterHash.get(wireSub.filterHash) === subId) {
          owningState.subIdByFilterHash.delete(wireSub.filterHash)
        }
        clearTimerEntry(scheduler, owningState.pendingSubTimeouts, subId)
        // A live (non-pending) sub needs a CLOSE on the wire. sendOnWebSocket is the single send
        // primitive and no-ops unless the socket is open, so there is nothing to pre-check here.
        const currentWs = owningState.ws
        if (!wasPending && currentWs !== null) {
          sendOnWebSocket(currentWs, serialiseCloseMessage(subId))
        }
        releaseIfIdle(url, owningState)
      }
      closeSubHistory({ subHistory, url, subId, clock })
      invalidateCache()
    }

    return Object.freeze({ active: true, unsubscribe })
  }

  return subscribe
}
