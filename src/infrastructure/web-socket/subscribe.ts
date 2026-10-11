import type { NostrFilter, RelayUrl, SubscriptionId } from "@innis/nostr-core"
import { canFilterMatch, compileFilters, hashFilters, serialiseCloseMessage } from "@innis/nostr-core"
import type { WallClock } from "../../application/port/clock.ts"
import type { RelayInformationLookup } from "../../application/port/relay-information-lookup.ts"
import type { Scheduler } from "../../application/port/scheduler.ts"
import { closeSubHistory, recordSubHistory, type SubHistoryMap } from "../../application/service/relay-history.ts"
import { filtersSentTo, withoutSearch } from "../../domain/service/nip50-search.ts"
import type { RelaySubscribeCallbacks, Subscription } from "../../domain/value-object/subscription.ts"
import type { WireSub } from "./wire-sub.ts"
import type { RelayState } from "./relay-state.ts"
import { cancelPendingSubTimeout, findWireSub, reissueReq } from "./relay-state.ts"
import { isOpen, sendOnWebSocket, subscriptionIdOf } from "./web-socket-helpers.ts"

export const INACTIVE_SUBSCRIPTION: Subscription = Object.freeze({ active: false, unsubscribe: (): void => {} })

// Deliberate: a subscription with nothing to ask the relay is answered here, not sent — see ADR-0006 and ADR-0009
const answeredLocally = (callbacks: RelaySubscribeCallbacks): Subscription => {
  let open = true
  queueMicrotask(() => {
    if (open) callbacks.onEose?.()
  })
  return Object.freeze({
    active: true,
    unsubscribe: (): void => {
      open = false
    },
  })
}

const isStillListening = (wireSub: WireSub, listener: RelaySubscribeCallbacks): boolean =>
  wireSub.listeners.includes(listener)

const sendCatchUpEose = (wireSub: WireSub, lateJoiner: RelaySubscribeCallbacks): void =>
  queueMicrotask(() => {
    if (isStillListening(wireSub, lateJoiner)) lateJoiner.onEose?.()
  })

export interface SubscribeDeps {
  readonly clock: WallClock
  readonly scheduler: Scheduler
  readonly pendingSubTimeoutMs: number
  readonly subHistory: SubHistoryMap
  readonly findOwningState: (url: RelayUrl, subId: SubscriptionId) => RelayState | undefined
  readonly invalidateCache: () => void
  readonly isDisposed: () => boolean
  readonly getOrCreateConnection: (url: RelayUrl) => RelayState | null
  readonly releaseIfIdle: (url: RelayUrl, state: RelayState) => void
  readonly relayInformation: RelayInformationLookup
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
    relayInformation,
  } = deps
  let subCounter = 0
  const nextSubId = (): SubscriptionId => subscriptionIdOf(`pool-${++subCounter}`)

  const expireSubAwaitingSocket = (state: RelayState, url: RelayUrl, subId: SubscriptionId): void => {
    const wireSub = state.pendingSubs.get(subId)
    if (!wireSub) return
    state.pendingSubs.delete(subId)
    state.pendingSubTimeouts.delete(subId)
    if (state.subIdByFilterHash.get(wireSub.filterHash) === subId) {
      state.subIdByFilterHash.delete(wireSub.filterHash)
    }
    for (const listener of wireSub.listeners) listener.onClosed?.("timeout")
    closeSubHistory({ subHistory, url, subId, clock })
    invalidateCache()
    releaseIfIdle(url, state)
  }

  const schedulePendingSubTimeout = (state: RelayState, url: RelayUrl, subId: SubscriptionId): void => {
    const timeoutId = scheduler.setTimer(() => expireSubAwaitingSocket(state, url, subId), pendingSubTimeoutMs)
    state.pendingSubTimeouts.set(subId, timeoutId)
  }

  const subscribe = (
    url: RelayUrl,
    filters: ReadonlyArray<NostrFilter>,
    callbacks: RelaySubscribeCallbacks,
  ): Subscription => {
    if (isDisposed()) return INACTIVE_SUBSCRIPTION
    const sent = filtersSentTo(relayInformation(url), filters)
    if (!sent.some(canFilterMatch)) return answeredLocally(callbacks)
    const state = getOrCreateConnection(url)
    if (!state) return INACTIVE_SUBSCRIPTION

    const filterHash = hashFilters(sent)
    const listener: RelaySubscribeCallbacks = {
      onEvent: callbacks.onEvent,
      onEose: callbacks.onEose,
      onClosed: callbacks.onClosed,
    }

    const existingSubId = state.subIdByFilterHash.get(filterHash)
    const existingWireSub = existingSubId !== undefined ? findWireSub(state, existingSubId) : undefined

    let subId: SubscriptionId
    let wireSub: WireSub

    if (existingWireSub && existingSubId !== undefined) {
      subId = existingSubId
      wireSub = existingWireSub
      wireSub.listeners = [...wireSub.listeners, listener]
      if (wireSub.eoseFired) sendCatchUpEose(wireSub, listener)
    } else {
      subId = nextSubId()
      wireSub = {
        filters: sent,
        filterHash,
        compiledWithoutSearch: compileFilters(sent.map(withoutSearch)),
        listeners: [listener],
        eoseFired: false,
        lastReqSentAt: 0,
        widening: "none",
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

      recordSubHistory({ subHistory, url, subId, filters: sent, openedAt: clock() })
    }

    invalidateCache()

    const unsubscribe = (): void => {
      wireSub.listeners = wireSub.listeners.filter((other) => other !== listener)
      if (wireSub.listeners.length > 0) {
        invalidateCache()
        return
      }

      const owningState = findOwningState(url, subId)
      if (owningState && findWireSub(owningState, subId) === wireSub) {
        const isOpenOnRelay = !owningState.pendingSubs.has(subId) && !owningState.pendingAuthSubs.has(subId)
        owningState.subs.delete(subId)
        owningState.pendingSubs.delete(subId)
        owningState.pendingAuthSubs.delete(subId)
        if (owningState.subIdByFilterHash.get(wireSub.filterHash) === subId) {
          owningState.subIdByFilterHash.delete(wireSub.filterHash)
        }
        cancelPendingSubTimeout(owningState, scheduler, subId)
        const currentWs = owningState.ws
        if (isOpenOnRelay && currentWs !== null) {
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
