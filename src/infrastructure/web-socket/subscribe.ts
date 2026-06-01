import type { NostrFilter, RelayUrl } from "@innis/nostr-core"
import { hashFilters, serialiseCloseMessage } from "@innis/nostr-core"
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
  readonly connections: ReadonlyMap<RelayUrl, RelayState>
  readonly invalidateCache: () => void
  readonly isDisposed: () => boolean
  readonly getOrCreateConnection: (url: RelayUrl) => RelayState | null
}

export const createSubscribe = (deps: SubscribeDeps) => {
  const {
    clock,
    scheduler,
    pendingSubTimeoutMs,
    subHistory,
    connections,
    invalidateCache,
    isDisposed,
    getOrCreateConnection,
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
      for (const listener of [...wireSub.listeners]) listener.onClosed?.("timeout")
      closeSubHistory({ subHistory, url, subId, clock })
      invalidateCache()
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
      wireSub.listeners.add(listener)
      // Synthetic catch-up EOSE for a late joiner. Re-check membership on the microtask: a caller
      // that unsubscribes synchronously has already left the listener set and must not be signalled.
      if (wireSub.eoseFired) {
        queueMicrotask(() => {
          if (wireSub.listeners.has(listener)) listener.onEose?.()
        })
      }
    } else {
      subId = nextSubId()
      wireSub = { filters, filterHash, listeners: new Set([listener]), eoseFired: false, reqSentAt: 0 }
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
      const currentState = connections.get(url)
      if (!currentState) {
        invalidateCache()
        return
      }
      const currentWireSub = findWireSub(currentState, subId)
      if (!currentWireSub) {
        invalidateCache()
        return
      }
      currentWireSub.listeners.delete(listener)
      if (currentWireSub.listeners.size > 0) {
        invalidateCache()
        return
      }

      const wasPending = currentState.pendingSubs.has(subId) || currentState.pendingAuthSubs.has(subId)
      currentState.subs.delete(subId)
      currentState.pendingSubs.delete(subId)
      currentState.pendingAuthSubs.delete(subId)
      currentState.subIdByFilterHash.delete(currentWireSub.filterHash)
      clearTimerEntry(scheduler, currentState.pendingSubTimeouts, subId)
      // A live (non-pending) sub needs a CLOSE on the wire. sendOnWebSocket is the single send
      // primitive and no-ops unless the socket is open, so there is nothing to pre-check here.
      const currentWs = currentState.ws
      if (!wasPending && currentWs !== null) {
        sendOnWebSocket(currentWs, serialiseCloseMessage(subId))
      }
      closeSubHistory({ subHistory, url, subId, clock })
      invalidateCache()
    }

    return Object.freeze({ active: true, unsubscribe })
  }

  return subscribe
}
