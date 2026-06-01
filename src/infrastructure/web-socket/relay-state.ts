import type { NostrEvent, RelayUrl } from "@innis/nostr-core"
import { serialiseReqMessage } from "@innis/nostr-core"
import type { WallClock } from "../../application/port/clock.ts"
import type { Scheduler, TimerHandle } from "../../application/port/scheduler.ts"
import { closeSubHistory, type SubHistoryMap } from "../../application/service/relay-history.ts"
import type { PublishResponse } from "../../domain/value-object/publish-history.ts"
import type { WireSub } from "./wire-sub.ts"
import { clearAllTimers, sendOnWebSocket } from "./web-socket-helpers.ts"

/** Internal publish-resolver shape — the per-relay `OK` reply, without the `from` URL the pool adds. */
export type PublishAck = Omit<PublishResponse, "from">

/**
 * One publish awaiting its relay `OK` (or timeout). Keyed by event id in `RelayState.inFlightPublishes`,
 * so the event, its awaiting resolvers, its timeout timer, and the ack-settlement closure live as one
 * record rather than three maps mutated in lockstep. `resolvers` is a set because concurrent
 * `publish` calls for the identical event to one relay deduplicate onto a single in-flight send —
 * the same listener-set dedup the wire-sub layer uses — and all share its single outcome.
 */
export interface InFlightPublish {
  readonly event: NostrEvent
  readonly resolvers: Set<(response: PublishResponse) => void>
  readonly settle: (ack: PublishAck) => void
  readonly timeoutId: TimerHandle
}

export interface RelayState {
  ws: WebSocket | null
  subs: Map<string, WireSub>
  pendingSubs: Map<string, WireSub>
  pendingSubTimeouts: Map<string, TimerHandle>
  subIdByFilterHash: Map<string, string>
  intentionalClose: boolean
  authed: boolean
  authingChallenge: string | null
  connectedAt: number | null
  stabilityTimer: TimerHandle | null
  pendingAuthPublish: Array<NostrEvent>
  pendingAuthSubs: Map<string, WireSub>
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
  authingChallenge: null,
  connectedAt: null,
  stabilityTimer: null,
  pendingAuthPublish: [],
  pendingAuthSubs: new Map(),
  inFlightPublishes: new Map(),
})

export const transferPendingState = (from: RelayState, to: RelayState): void => {
  for (const [subId, sub] of from.subs) to.subs.set(subId, sub)
  for (const [subId, sub] of from.pendingAuthSubs) to.pendingAuthSubs.set(subId, sub)
  for (const [subId, sub] of from.pendingSubs) to.pendingSubs.set(subId, sub)
  for (const [hash, subId] of from.subIdByFilterHash) to.subIdByFilterHash.set(hash, subId)
}

export const findWireSub = (state: RelayState, subId: string): WireSub | undefined =>
  state.subs.get(subId) ?? state.pendingSubs.get(subId) ?? state.pendingAuthSubs.get(subId)

export interface ReissueReqInput {
  readonly ws: WebSocket
  readonly subId: string
  readonly sub: WireSub
  readonly clock: WallClock
}

/**
 * Put a wire subscription's REQ on the socket and reset its round-trip clock: clear `eoseFired`
 * and re-stamp `reqSentAt` so EOSE latency is measured against this fresh request, not the
 * original subscribe. The single way the pool issues a REQ — initial send, reconnect reopen, and
 * post-AUTH flush all route through here so there is exactly one definition of "send this sub".
 */
export const reissueReq = ({ ws, subId, sub, clock }: ReissueReqInput): void => {
  sub.eoseFired = false
  sub.reqSentAt = clock()
  sendOnWebSocket(ws, serialiseReqMessage(subId, sub.filters))
}

/**
 * Promote every auth-parked sub onto the live socket: re-issue its REQ and move it into `subs`.
 * Shared by the reconnect path (socket reopen) and the post-AUTH flush so the parked-sub promotion
 * lives in exactly one place.
 */
export const promoteAuthedSubs = (state: RelayState, clock: WallClock): void => {
  const ws = state.ws
  if (!ws) return
  for (const [subId, sub] of state.pendingAuthSubs) {
    reissueReq({ ws, subId, sub, clock })
    state.subs.set(subId, sub)
  }
  state.pendingAuthSubs.clear()
}

export const hasAnySubs = (state: RelayState): boolean =>
  state.subs.size > 0 || state.pendingAuthSubs.size > 0 || state.pendingSubs.size > 0

export interface TearDownSubsInput {
  readonly state: RelayState
  readonly url: RelayUrl
  readonly subHistory: SubHistoryMap
  readonly clock: WallClock
  readonly scheduler: Scheduler
  readonly reason: string
}

/**
 * Close out every subscription on `state` — fires the terminal `onClosed(reason)` for active
 * listeners (a pool-initiated teardown ends the stream; it is *not* an EOSE, which means "stored
 * events done, stream continues"), stamps closedAt on history entries, and clears every pending-sub
 * timer. Mutates `state` in place; the caller still owns whether to close the underlying socket.
 */
export const tearDownSubs = ({ state, url, subHistory, clock, scheduler, reason }: TearDownSubsInput): void => {
  for (const [subId, wireSub] of state.subs) {
    for (const listener of [...wireSub.listeners]) listener.onClosed?.(reason)
    closeSubHistory({ subHistory, url, subId, clock })
  }
  for (const [subId] of state.pendingSubs) closeSubHistory({ subHistory, url, subId, clock })
  for (const [subId] of state.pendingAuthSubs) closeSubHistory({ subHistory, url, subId, clock })
  state.subs.clear()
  state.pendingSubs.clear()
  state.pendingAuthSubs.clear()
  state.subIdByFilterHash.clear()
  clearAllTimers(scheduler, state.pendingSubTimeouts)
}
