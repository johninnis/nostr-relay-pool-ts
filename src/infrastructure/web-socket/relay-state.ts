import type { AuthChallenge, NostrEvent, RelayUrl, SubscriptionId } from "@innis/nostr-core"
import { serialiseReqMessage } from "@innis/nostr-core"
import type { WallClock } from "../../application/port/clock.ts"
import type { Scheduler, TimerHandle } from "../../application/port/scheduler.ts"
import { closeSubHistory, type SubHistoryMap } from "../../application/service/relay-history.ts"
import type { PublishResponse } from "../../domain/value-object/publish-history.ts"
import type { Widening, WireSub } from "./wire-sub.ts"
import { sendOnWebSocket } from "./web-socket-helpers.ts"

export type PublishAck = Omit<PublishResponse, "from">

export interface InFlightPublish {
  readonly event: NostrEvent
  readonly resolvers: Set<(response: PublishResponse) => void>
  readonly settle: (ack: PublishAck) => void
  readonly suspendTimeout: () => void
  readonly restartTimeout: () => void
}

export interface RelayState {
  ws: WebSocket | null
  subs: Map<SubscriptionId, WireSub>
  pendingSubs: Map<SubscriptionId, WireSub>
  pendingSubTimeouts: Map<SubscriptionId, TimerHandle>
  subIdByFilterHash: Map<string, SubscriptionId>
  intentionalClose: boolean
  authed: boolean
  unansweredChallenge: AuthChallenge | null
  authDeclined: boolean
  authAttempt: symbol | null
  authTimer: TimerHandle | null
  authEventId: string | null
  connectedAt: number | null
  stabilityTimer: TimerHandle | null
  idleTimer: TimerHandle | null
  heartbeatTimer: TimerHandle | null
  pendingAuthPublish: Set<string>
  pendingAuthSubs: Map<SubscriptionId, WireSub>
  inFlightPublishes: Map<string, InFlightPublish>
}

export const createRelayState = (): RelayState => ({
  ws: null,
  subs: new Map(),
  pendingSubs: new Map(),
  pendingSubTimeouts: new Map(),
  subIdByFilterHash: new Map(),
  intentionalClose: false,
  authed: false,
  unansweredChallenge: null,
  authDeclined: false,
  authAttempt: null,
  authTimer: null,
  authEventId: null,
  connectedAt: null,
  stabilityTimer: null,
  idleTimer: null,
  heartbeatTimer: null,
  pendingAuthPublish: new Set(),
  pendingAuthSubs: new Map(),
  inFlightPublishes: new Map(),
})

export const transferPendingState = (from: RelayState, to: RelayState): void => {
  for (const [subId, sub] of from.subs) to.subs.set(subId, sub)
  for (const [subId, sub] of from.pendingAuthSubs) to.pendingAuthSubs.set(subId, sub)
  for (const [subId, sub] of from.pendingSubs) to.pendingSubs.set(subId, sub)
  for (const [hash, subId] of from.subIdByFilterHash) to.subIdByFilterHash.set(hash, subId)
}

export const findWireSub = (state: RelayState, subId: SubscriptionId): WireSub | undefined =>
  state.subs.get(subId) ?? state.pendingSubs.get(subId) ?? state.pendingAuthSubs.get(subId)

export interface ReissueReqInput {
  readonly ws: WebSocket
  readonly subId: SubscriptionId
  readonly sub: WireSub
  readonly clock: WallClock
}

const sendReq = ({ ws, subId, sub, clock }: ReissueReqInput, widening: Widening): void => {
  sub.eoseFired = false
  sub.lastReqSentAt = clock()
  sub.widening = widening
  const req = serialiseReqMessage(subId, sub.filters)
  if (req !== null) sendOnWebSocket(ws, req)
}

export const reissueReq = (input: ReissueReqInput): void => sendReq(input, "none")

export const widenNarrowedSubs = (state: RelayState, clock: WallClock): void => {
  const ws = state.ws
  if (!ws) return
  for (const [subId, sub] of state.subs) {
    if (sub.widening === "awaiting-auth") sendReq({ ws, subId, sub, clock }, "sent")
  }
}

export const reissueAuthParkedSubs = (state: RelayState, clock: WallClock): void => {
  const ws = state.ws
  if (!ws) return
  for (const [subId, sub] of state.pendingAuthSubs) {
    reissueReq({ ws, subId, sub, clock })
    state.subs.set(subId, sub)
  }
  state.pendingAuthSubs.clear()
}

export const settleInFlightPublishes = (state: RelayState, message: string): void => {
  for (const inFlight of [...state.inFlightPublishes.values()]) inFlight.settle({ ok: false, message })
}

export const hasAnySubs = (state: RelayState): boolean =>
  state.subs.size > 0 || state.pendingAuthSubs.size > 0 || state.pendingSubs.size > 0

export const isIdle = (state: RelayState): boolean =>
  !hasAnySubs(state) && state.inFlightPublishes.size === 0 && state.pendingAuthPublish.size === 0

export const closeIntentionally = (state: RelayState): void => {
  state.intentionalClose = true
  state.ws?.close()
}

export interface TearDownSubsInput {
  readonly state: RelayState
  readonly url: RelayUrl
  readonly subHistory: SubHistoryMap
  readonly clock: WallClock
  readonly scheduler: Scheduler
  readonly reason: string
}

export const tearDownSubs = ({ state, url, subHistory, clock, scheduler, reason }: TearDownSubsInput): void => {
  for (const [subId, wireSub] of state.subs) {
    for (const listener of wireSub.listeners) listener.onClosed?.(reason)
    closeSubHistory({ subHistory, url, subId, clock })
  }
  for (const [subId] of state.pendingSubs) closeSubHistory({ subHistory, url, subId, clock })
  for (const [subId] of state.pendingAuthSubs) closeSubHistory({ subHistory, url, subId, clock })
  state.subs.clear()
  state.pendingSubs.clear()
  state.pendingAuthSubs.clear()
  state.subIdByFilterHash.clear()
  cancelPendingSubTimeouts(state, scheduler)
}

export const releaseAuthAttempt = (state: RelayState): void => {
  state.authAttempt = null
  state.authEventId = null
}

export const cancelPendingSubTimeout = (state: RelayState, scheduler: Scheduler, subId: SubscriptionId): void => {
  const timer = state.pendingSubTimeouts.get(subId)
  if (timer !== undefined) scheduler.clearTimer(timer)
  state.pendingSubTimeouts.delete(subId)
}

export const cancelPendingSubTimeouts = (state: RelayState, scheduler: Scheduler): void => {
  for (const timer of state.pendingSubTimeouts.values()) scheduler.clearTimer(timer)
  state.pendingSubTimeouts.clear()
}

export const cancelAuthTimer = (state: RelayState, scheduler: Scheduler): void => {
  if (state.authTimer !== null) scheduler.clearTimer(state.authTimer)
  state.authTimer = null
}

export const cancelIdleClose = (state: RelayState, scheduler: Scheduler): void => {
  if (state.idleTimer !== null) scheduler.clearTimer(state.idleTimer)
  state.idleTimer = null
}

export const stopConnectionTimers = (state: RelayState, scheduler: Scheduler): void => {
  if (state.stabilityTimer !== null) scheduler.clearTimer(state.stabilityTimer)
  state.stabilityTimer = null
  cancelIdleClose(state, scheduler)
  if (state.heartbeatTimer !== null) scheduler.clearTimer(state.heartbeatTimer)
  state.heartbeatTimer = null
}

export const abandonAuth = (state: RelayState, scheduler: Scheduler): void => {
  releaseAuthAttempt(state)
  cancelAuthTimer(state, scheduler)
}
