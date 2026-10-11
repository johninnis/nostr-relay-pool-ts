import type { RelayUrl } from "@innis/nostr-core"
import type { BackoffTracker } from "../../application/service/backoff-tracker.ts"
import type { WallClock } from "../../application/port/clock.ts"
import type { Scheduler } from "../../application/port/scheduler.ts"
import type { RelayState } from "./relay-state.ts"
import {
  abandonAuth,
  cancelIdleClose,
  cancelPendingSubTimeout,
  cancelPendingSubTimeouts,
  closeIntentionally,
  createRelayState,
  hasAnySubs,
  isIdle,
  reissueAuthParkedSubs,
  reissueReq,
  settleInFlightPublishes,
  stopConnectionTimers,
  transferPendingState,
} from "./relay-state.ts"
import { handleRelayMessage, type MessageContext } from "./relay-message-handler.ts"
import { isOpen, isOpenOrConnecting, sendKeepalive } from "./web-socket-helpers.ts"

export interface PendingReconnect {
  readonly droppedState: RelayState
  readonly cancel: () => void
  readonly fire: () => void
}

export interface SocketManagerDeps {
  readonly clock: WallClock
  readonly scheduler: Scheduler
  readonly backoff: BackoffTracker
  readonly stableConnectionMs: number
  readonly idleSocketTimeoutMs: number
  readonly heartbeatIntervalMs: number
  readonly connections: Map<RelayUrl, RelayState>
  readonly pendingReconnects: Map<RelayUrl, PendingReconnect>
  readonly attemptedRelays: Set<RelayUrl>
  readonly invalidateCache: () => void
  readonly emitConnectionChange: (url: RelayUrl, connected: boolean) => void
  readonly connectionGate: () => (url: RelayUrl) => boolean
  readonly buildMessageContext: (state: RelayState, url: RelayUrl) => MessageContext
}

export interface SocketManager {
  readonly getOrCreateConnection: (url: RelayUrl) => RelayState | null
  readonly cancelPendingReconnect: (url: RelayUrl) => void
  readonly releaseIfIdle: (url: RelayUrl, state: RelayState) => void
}

const reopenSubs = (scheduler: Scheduler, clock: WallClock, state: RelayState): void => {
  const ws = state.ws
  if (!ws) return
  for (const [subId, sub] of state.subs) {
    reissueReq({ ws, subId, sub, clock })
  }
  for (const [subId, sub] of state.pendingSubs) {
    reissueReq({ ws, subId, sub, clock })
    state.subs.set(subId, sub)
    cancelPendingSubTimeout(state, scheduler, subId)
  }
  state.pendingSubs.clear()
  // Deliberate: a sub parked for AUTH when its socket dropped is re-issued on the reconnect like any other — see ADR-0001
  reissueAuthParkedSubs(state, clock)
}

const leaveFailureToOnclose = (): void => {}

export const createSocketManager = (deps: SocketManagerDeps): SocketManager => {
  const {
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
    connectionGate,
    buildMessageContext,
  } = deps

  const reopenSubsIfOpenedBeforeTransfer = (revived: RelayState): void => {
    if (isOpen(revived.ws)) reopenSubs(scheduler, clock, revived)
  }

  const scheduleReconnect = (droppedState: RelayState, url: RelayUrl): void => {
    if (!connectionGate()(url)) return
    pendingReconnects.get(url)?.cancel()
    const until = backoff.disabledUntil(url)
    const delayMs = until !== null ? Math.max(0, until - clock()) : 0
    const fire = (): void => {
      pendingReconnects.delete(url)
      invalidateCache()
      if (!connectionGate()(url)) return
      const revived = getOrCreateConnection(url)
      if (!revived) return
      transferPendingState(droppedState, revived)
      reopenSubsIfOpenedBeforeTransfer(revived)
    }
    const timerId = scheduler.setTimer(fire, delayMs)
    pendingReconnects.set(url, {
      droppedState,
      cancel: () => scheduler.clearTimer(timerId),
      fire: () => {
        scheduler.clearTimer(timerId)
        fire()
      },
    })
  }

  const cancelPendingReconnect = (url: RelayUrl): void => {
    const scheduledReconnect = pendingReconnects.get(url)
    if (!scheduledReconnect) return
    scheduledReconnect.cancel()
    pendingReconnects.delete(url)
    invalidateCache()
  }

  const closeIfStillIdle = (state: RelayState): void => {
    state.idleTimer = null
    if (isIdle(state)) closeIntentionally(state)
  }

  const scheduleIdleClose = (state: RelayState): void => {
    cancelIdleClose(state, scheduler)
    state.idleTimer = scheduler.setTimer(() => closeIfStillIdle(state), idleSocketTimeoutMs)
  }

  const releaseIfIdle = (url: RelayUrl, state: RelayState): void => {
    if (!isIdle(state)) return
    if (pendingReconnects.get(url)?.droppedState === state) cancelPendingReconnect(url)
    else if (connections.get(url) === state) scheduleIdleClose(state)
  }

  // Deliberate: an idle connection is kept alive with a periodic CLOSE, not a WebSocket ping — see ADR-0005
  const scheduleHeartbeat = (state: RelayState, ws: WebSocket): void => {
    if (heartbeatIntervalMs <= 0) return
    state.heartbeatTimer = scheduler.setTimer(() => {
      state.heartbeatTimer = null
      if (state.ws !== ws || !isOpen(ws)) return
      sendKeepalive(ws)
      scheduleHeartbeat(state, ws)
    }, heartbeatIntervalMs)
  }

  const attachSocket = (state: RelayState, ws: WebSocket, url: RelayUrl): void => {
    ws.onopen = (): void => {
      state.connectedAt = clock()
      invalidateCache()
      emitConnectionChange(url, true)
      reopenSubs(scheduler, clock, state)
      scheduleHeartbeat(state, ws)
      state.stabilityTimer = scheduler.setTimer(() => {
        state.stabilityTimer = null
        backoff.recordSuccess(url)
      }, stableConnectionMs)
    }

    const messageContext = buildMessageContext(state, url)
    ws.onmessage = (msg: MessageEvent): void => handleRelayMessage(messageContext, msg)

    ws.onclose = (event: CloseEvent): void => {
      stopConnectionTimers(state, scheduler)
      cancelPendingSubTimeouts(state, scheduler)
      state.connectedAt = null
      state.authed = false
      state.unansweredChallenge = null
      state.authDeclined = false
      abandonAuth(state, scheduler)
      if (connections.get(url) === state) connections.delete(url)
      invalidateCache()
      emitConnectionChange(url, false)
      settleInFlightPublishes(state, "disconnected")

      if (state.intentionalClose) return

      backoff.recordFailure(url, { code: event.code, reason: event.reason })

      if (hasAnySubs(state)) scheduleReconnect(state, url)
    }

    ws.onerror = leaveFailureToOnclose
  }

  const getOrCreateConnection = (url: RelayUrl): RelayState | null => {
    if (!url) return null
    if (!connectionGate()(url)) return null
    if (backoff.isDisabled(url)) return null

    const existing = connections.get(url)
    if (existing && isOpenOrConnecting(existing.ws)) {
      cancelIdleClose(existing, scheduler)
      return existing
    }

    const state = createRelayState()

    try {
      state.ws = new WebSocket(url)
    } catch (err) {
      backoff.recordFailure(url, { code: null, reason: err instanceof Error ? err.message : String(err) })
      return null
    }

    attachSocket(state, state.ws, url)
    connections.set(url, state)
    attemptedRelays.add(url)
    invalidateCache()
    return state
  }

  return Object.freeze({ getOrCreateConnection, cancelPendingReconnect, releaseIfIdle })
}
