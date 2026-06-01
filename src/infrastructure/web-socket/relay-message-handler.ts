import type { NostrEvent, RelayUrl } from "@innis/nostr-core"
import {
  matchesAnyFilter,
  parseRelayMessage,
  reportUnhandledError,
  serialiseAuthMessage,
  serialiseEventMessage,
} from "@innis/nostr-core"
import type { AuthHandler } from "../../application/port/auth-handler.ts"
import type { WallClock } from "../../application/port/clock.ts"
import type { Scheduler } from "../../application/port/scheduler.ts"
import { closeSubHistory, type SubHistoryRecord } from "../../application/service/relay-history.ts"
import type { PublishAck, RelayState } from "./relay-state.ts"
import { promoteAuthedSubs } from "./relay-state.ts"
import { clearTimerEntry, sendOnWebSocket } from "./web-socket-helpers.ts"

export interface MessageContext {
  readonly state: RelayState
  readonly url: RelayUrl
  readonly subHistory: ReadonlyMap<RelayUrl, Map<string, SubHistoryRecord>>
  readonly authHandler: () => AuthHandler | null
  readonly clock: WallClock
  readonly scheduler: Scheduler
  readonly onEventReceived: (url: RelayUrl) => void
  readonly onEoseLatency: (url: RelayUrl, ms: number) => void
  readonly onStateChange: () => void
}

const sendAuthedQueue = (state: RelayState, clock: WallClock): void => {
  if (!state.ws) return
  promoteAuthedSubs(state, clock)
  for (const event of state.pendingAuthPublish) {
    sendOnWebSocket(state.ws, serialiseEventMessage(event))
  }
  // Empty in place rather than rebind: every other RelayState array field is mutated in place, and
  // the publish-settle closure reads `state.pendingAuthPublish` by reference to splice itself out.
  state.pendingAuthPublish.length = 0
}

const handleAuthChallenge = async (ctx: MessageContext, challenge: string): Promise<void> => {
  if (ctx.state.authed || ctx.state.authingChallenge === challenge) return
  const handler = ctx.authHandler()
  if (!handler || !ctx.state.ws) return
  ctx.state.authingChallenge = challenge
  try {
    const signed = await handler(ctx.url, challenge)
    if (!signed || !ctx.state.ws) return
    // Optimistic by necessity: NIP-42 makes the relay's OK for the AUTH event optional, and many
    // relays simply start honouring requests once AUTH is sent. So flush the parked queue now. If
    // the auth was in fact bad the relay re-CLOSEs the subs with auth-required, and handleClosed
    // clears `authed` so the paired re-challenge can try again rather than the sub stranding.
    sendOnWebSocket(ctx.state.ws, serialiseAuthMessage(signed))
    ctx.state.authed = true
    ctx.onStateChange()
    sendAuthedQueue(ctx.state, ctx.clock)
  } catch (err) {
    // The pool keeps running; surfacing the error as an unhandled rejection makes the auth
    // failure visible to the host process instead of leaving the relay silently un-authed.
    ctx.state.authed = false
    reportUnhandledError(err)
  } finally {
    ctx.state.authingChallenge = null
  }
}

const handleEvent = (ctx: MessageContext, subId: string, event: NostrEvent): void => {
  const wireSub = ctx.state.subs.get(subId)
  if (!wireSub) return
  if (!matchesAnyFilter(event, wireSub.filters)) return
  ctx.onEventReceived(ctx.url)
  const historyEntry = ctx.subHistory.get(ctx.url)?.get(subId)
  if (historyEntry) historyEntry.eventCount++
  // Snapshot the listener set before dispatch: an onEvent that re-enters the pool (subscribe or
  // unsubscribe on this same filter hash) mutates `wireSub.listeners` mid-iteration otherwise.
  // Mirrors the [...connectionChangeListeners] guard the adapter applies to its own listener set.
  for (const listener of [...wireSub.listeners]) listener.onEvent(event, ctx.url)
}

const handleEose = (ctx: MessageContext, subId: string): void => {
  const wireSub = ctx.state.subs.get(subId)
  if (!wireSub || wireSub.eoseFired) return
  wireSub.eoseFired = true
  // Measure against the live REQ, not the original subscribe time: reqSentAt is re-stamped on every
  // reconnect/auth re-issue, so a long backoff window never leaks into the latency sample.
  if (wireSub.reqSentAt > 0) ctx.onEoseLatency(ctx.url, ctx.clock() - wireSub.reqSentAt)
  for (const listener of [...wireSub.listeners]) listener.onEose?.()
}

const handleClosed = (ctx: MessageContext, subId: string, message: string): void => {
  const { state } = ctx
  const wireSub = state.subs.get(subId) ?? state.pendingSubs.get(subId)
  if (!wireSub) return

  state.subs.delete(subId)
  state.pendingSubs.delete(subId)
  clearTimerEntry(ctx.scheduler, state.pendingSubTimeouts, subId)

  if (message.startsWith("auth-required")) {
    // A relay demanding auth for a sub we believed was authed means our optimistic auth was stale
    // or rejected; clear `authed` so the paired AUTH re-challenge drives a fresh handshake rather
    // than being short-circuited by the `authed` guard — which would otherwise strand this sub.
    state.authed = false
    state.pendingAuthSubs.set(subId, wireSub)
    // The sub is re-parked, not closed: it lives on in pendingAuthSubs awaiting the AUTH handshake,
    // so its history entry stays open. Stamping closedAt here would mislabel a still-pending sub as
    // closed (masked while live, but wrong the moment it later genuinely closes).
  } else {
    state.subIdByFilterHash.delete(wireSub.filterHash)
    // A relay-initiated CLOSED terminates the subscription — unlike a socket drop it will not be
    // revived. Fire the distinct onClosed terminal signal: CLOSED is not EOSE, so a persistent
    // listener learns the stream is dead rather than that the backlog merely ended, and an
    // EOSE-or-timeout fan-in settles now instead of hanging until its hard timeout.
    for (const listener of [...wireSub.listeners]) listener.onClosed?.(message)
    closeSubHistory({ subHistory: ctx.subHistory, url: ctx.url, subId, clock: ctx.clock })
  }

  ctx.onStateChange()
}

const handleAuthRequiredOk = (ctx: MessageContext, eventId: string): void => {
  // Park the in-flight event for resend once AUTH completes. The in-flight record stays put (timer
  // and resolvers intact): the publish must still settle on its original deadline if AUTH never
  // succeeds (no handler, or a handler that fails), and a genuine OK after the resend settles it
  // via handleOkResult. Guard against a repeat auth-required OK queueing the same event twice.
  const inFlight = ctx.state.inFlightPublishes.get(eventId)
  if (inFlight && !ctx.state.pendingAuthPublish.includes(inFlight.event)) {
    ctx.state.pendingAuthPublish.push(inFlight.event)
  }
}

const handleOkResult = (ctx: MessageContext, eventId: string, ack: PublishAck): void => {
  // The publish settlement (createPublish's `settle`) owns all teardown — timer, bookkeeping,
  // and resolution. Dispatch the ack to it; do not duplicate that cleanup here.
  ctx.state.inFlightPublishes.get(eventId)?.settle(ack)
}

export const handleRelayMessage = (ctx: MessageContext, msg: MessageEvent): void => {
  if (typeof msg.data !== "string") return
  const message = parseRelayMessage(msg.data)
  if (message === null) return

  switch (message.type) {
    case "AUTH":
      handleAuthChallenge(ctx, message.challenge)
      return
    case "EVENT":
      handleEvent(ctx, message.subscriptionId, message.event)
      return
    case "EOSE":
      handleEose(ctx, message.subscriptionId)
      return
    case "CLOSED":
      handleClosed(ctx, message.subscriptionId, message.message)
      return
    case "OK":
      if (!message.accepted && message.message.startsWith("auth-required")) {
        handleAuthRequiredOk(ctx, message.eventId)
      } else {
        handleOkResult(ctx, message.eventId, { ok: message.accepted, message: message.message })
      }
      return
      // NOTICE and COUNT are not actioned by the pool.
  }
}
