import type { RelayUrl } from "@innis/nostr-core"
import { errorMessage } from "@innis/nostr-core"
import type { BackoffTracker } from "../../application/service/backoff-tracker.ts"
import type { WallClock } from "../../application/port/clock.ts"
import type { Scheduler } from "../../application/port/scheduler.ts"
import type { RelayState } from "./relay-state.ts"
import {
  closeIntentionally,
  createRelayState,
  hasAnySubs,
  isIdle,
  promoteAuthedSubs,
  reissueReq,
  transferPendingState,
} from "./relay-state.ts"
import { handleRelayMessage, type MessageContext } from "./relay-message-handler.ts"
import { clearAllTimers, clearTimerEntry, isOpen, isOpenOrConnecting } from "./web-socket-helpers.ts"

export interface PendingReconnect {
  /**
   * The state the dropped socket left behind, still holding its subscriptions until the reconnect
   * transfers them. Exposed so an unsubscribe arriving during the backoff window can remove its sub
   * from here — otherwise the reconnect would resurrect it as a REQ no caller can ever close.
   */
  readonly state: RelayState
  readonly cancel: () => void
  readonly fire: () => void
}

export interface SocketManagerDeps {
  readonly clock: WallClock
  readonly scheduler: Scheduler
  readonly backoff: BackoffTracker
  readonly stableConnectionMs: number
  readonly idleSocketTimeoutMs: number
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
  /** Cancel and discard the queued reconnect for `url`, if one exists. */
  readonly cancelPendingReconnect: (url: RelayUrl) => void
  /**
   * Reconsider whether `state` still needs to exist, after a caller removed its last known piece of
   * activity. An idle state parked on a pending reconnect has its reconnect cancelled outright; an
   * idle live socket gets an idle timer that closes it intentionally (no backoff penalty) unless new
   * activity arrives first. Non-idle states are left untouched.
   */
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
    clearTimerEntry(scheduler, state.pendingSubTimeouts, subId)
  }
  state.pendingSubs.clear()
  // A sub parked awaiting AUTH when the socket dropped must be re-issued too. Re-sending the REQ
  // re-triggers the relay's auth-required CLOSED (which re-parks it) and a fresh AUTH challenge;
  // once AUTH completes sendAuthedQueue flushes it. Without this it strands on every reconnect.
  promoteAuthedSubs(state, clock)
}

export const createSocketManager = (deps: SocketManagerDeps): SocketManager => {
  const {
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
    connectionGate,
    buildMessageContext,
  } = deps

  const scheduleReconnect = (oldState: RelayState, url: RelayUrl): void => {
    if (!connectionGate()(url)) return
    // Cancel any reconnect already queued for this URL before queuing another, so a second schedule
    // (e.g. a race between onclose and clearDisabled) cannot orphan the previous timer.
    pendingReconnects.get(url)?.cancel()
    const until = backoff.disabledUntil(url)
    const delayMs = until !== null ? Math.max(0, until - clock()) : 0
    const fire = (): void => {
      // Removing the queued reconnect changes the `reconnecting` projection, so invalidate up front
      // — otherwise a gate-rejected or disabled fire returns early and leaves a stale `reconnecting`
      // entry in the cached pool state until the next unrelated mutation.
      pendingReconnects.delete(url)
      invalidateCache()
      if (!connectionGate()(url)) return
      const revived = getOrCreateConnection(url)
      if (!revived) return
      transferPendingState(oldState, revived)
      // A real WebSocket never opens synchronously, so its onopen (which re-issues every sub) fires
      // after this transfer. A transport that opened synchronously would already have run onopen
      // against the empty pre-transfer state, so re-issue here to cover that case.
      if (revived.ws && isOpen(revived.ws)) reopenSubs(scheduler, clock, revived)
    }
    const timerId = scheduler.setTimer(fire, delayMs)
    pendingReconnects.set(url, {
      state: oldState,
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

  const releaseIfIdle = (url: RelayUrl, state: RelayState): void => {
    if (!isIdle(state)) return
    if (pendingReconnects.get(url)?.state === state) {
      cancelPendingReconnect(url)
      return
    }
    if (connections.get(url) !== state) return
    if (state.idleTimer !== null) scheduler.clearTimer(state.idleTimer)
    state.idleTimer = scheduler.setTimer(() => {
      state.idleTimer = null
      // Re-check at fire time: activity that arrived through a path that did not clear the timer
      // (e.g. a publish settled and re-queued within the window) must keep the socket alive.
      if (isIdle(state)) closeIntentionally(state)
    }, idleSocketTimeoutMs)
  }

  const attachSocket = (state: RelayState, ws: WebSocket, url: RelayUrl): void => {
    ws.onopen = (): void => {
      state.connectedAt = clock()
      invalidateCache()
      emitConnectionChange(url, true)
      reopenSubs(scheduler, clock, state)
      state.stabilityTimer = scheduler.setTimer(() => {
        state.stabilityTimer = null
        backoff.recordSuccess(url)
      }, stableConnectionMs)
    }

    const messageContext = buildMessageContext(state, url)
    ws.onmessage = (msg: MessageEvent): void => handleRelayMessage(messageContext, msg)

    ws.onclose = (event: CloseEvent): void => {
      if (state.stabilityTimer !== null) {
        scheduler.clearTimer(state.stabilityTimer)
        state.stabilityTimer = null
      }
      if (state.idleTimer !== null) {
        scheduler.clearTimer(state.idleTimer)
        state.idleTimer = null
      }
      // The pending-sub watchdogs belong to this now-dead socket. Left armed they would fire against
      // the shared subscription history and prematurely close a sub the reconnect re-establishes; a
      // revived socket re-sends every pending sub on open, so there is nothing left to time out here.
      clearAllTimers(scheduler, state.pendingSubTimeouts)
      state.connectedAt = null
      state.authed = false
      state.authingChallenge = null
      if (connections.get(url) === state) connections.delete(url)
      invalidateCache()
      emitConnectionChange(url, false)

      // Settle every publish still awaiting an ack on this dead socket. The event is not re-sent on
      // reconnect, so without this the promise would hang for the full publishTimeoutMs against an
      // orphaned state. dispose() settles its own queue first, so this loop is empty in that path.
      for (const inFlight of [...state.inFlightPublishes.values()]) {
        inFlight.settle({ ok: false, message: "disconnected" })
      }

      // A close we initiated (disconnect / dispose / connection gate) is not a relay failure:
      // recording one would penalise the relay with backoff and persist a spurious disabled state.
      if (state.intentionalClose) return

      backoff.recordFailure(url, { code: event.code, reason: event.reason })

      if (hasAnySubs(state)) scheduleReconnect(state, url)
    }

    // onerror is intentionally swallowed — onclose carries the diagnostic info.
    ws.onerror = (): void => {}
  }

  const getOrCreateConnection = (url: RelayUrl): RelayState | null => {
    if (!url) return null
    if (!connectionGate()(url)) return null
    if (backoff.isDisabled(url)) return null

    const existing = connections.get(url)
    if (existing && isOpenOrConnecting(existing.ws)) {
      // New activity on a socket awaiting its idle close: the reuse path is the one funnel every
      // subscribe and publish passes through, so disarming here keeps the socket alive for it.
      if (existing.idleTimer !== null) {
        scheduler.clearTimer(existing.idleTimer)
        existing.idleTimer = null
      }
      return existing
    }

    const state = createRelayState()

    try {
      state.ws = new WebSocket(url)
    } catch (err) {
      backoff.recordFailure(url, { code: null, reason: errorMessage(err) })
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
