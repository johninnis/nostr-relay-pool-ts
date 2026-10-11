import type { AuthChallenge, EoseMessage, NostrEvent, RelayMessage, RelayUrl, SubscriptionId } from "@innis/nostr-core"
import { eoseHasHint, parseRelayMessage, serialiseAuthMessage, serialiseEventMessage } from "@innis/nostr-core"
import type { AuthHandler } from "../../application/port/auth-handler.ts"
import type { WallClock } from "../../application/port/clock.ts"
import type { Scheduler } from "../../application/port/scheduler.ts"
import { closeSubHistory, type SubHistoryRecord } from "../../application/service/relay-history.ts"
import type { RelayState } from "./relay-state.ts"
import {
  cancelAuthTimer,
  cancelPendingSubTimeout,
  reissueAuthParkedSubs,
  releaseAuthAttempt,
  widenNarrowedSubs,
} from "./relay-state.ts"
import { sendKeepalive, sendOnWebSocket } from "./web-socket-helpers.ts"
import type { WireSub } from "./wire-sub.ts"

export interface MessageContext {
  readonly state: RelayState
  readonly url: RelayUrl
  readonly subHistory: ReadonlyMap<RelayUrl, Map<SubscriptionId, SubHistoryRecord>>
  readonly authHandler: () => AuthHandler | null
  readonly authTimeoutMs: number
  readonly maxMessageBytes: number
  readonly clock: WallClock
  readonly scheduler: Scheduler
  readonly onEventReceived: (url: RelayUrl) => void
  readonly onEoseLatency: (url: RelayUrl, ms: number) => void
  readonly onStateChange: () => void
}

const textEncoder = new TextEncoder()

// Deliberate: a ping is surrounded only by space, tab, line feed, carriage return, vertical tab or NUL, never other whitespace — see shared ADR-0035 and ADR-0004
const PING_NOTICE = /^[ \t\n\r\v\0]*ping[ \t\n\r\v\0]*$/i
const AUTH_REJECTED = "auth-required: auth rejected: "
const AUTH_DECLINED = "auth-required: auth declined"
const AUTH_TIMED_OUT = "auth-required: auth timed out"

type OkMessage = Extract<RelayMessage, { readonly type: "OK" }>
type ClosedMessage = Extract<RelayMessage, { readonly type: "CLOSED" }>

// Deliberate: auth-required work is parked only while a handler can still authenticate this connection — see ADR-0001
const canAwaitAuth = (ctx: MessageContext): boolean =>
  !ctx.state.authed && !ctx.state.authDeclined && ctx.authHandler() !== null

const hasAuthParked = (state: RelayState): boolean =>
  state.pendingAuthPublish.size > 0 || state.pendingAuthSubs.size > 0

// Deliberate: one auth timer runs while an answer is in flight or work is parked, and its expiry settles the parked work — see ADR-0001
const timeOutAuth = (ctx: MessageContext): void => {
  ctx.state.authTimer = null
  releaseAuthAttempt(ctx.state)
  releaseAuthParked(ctx, AUTH_TIMED_OUT)
  ctx.onStateChange()
}

const restartAuthTimer = (ctx: MessageContext): void => {
  cancelAuthTimer(ctx.state, ctx.scheduler)
  ctx.state.authTimer = ctx.scheduler.setTimer(() => timeOutAuth(ctx), ctx.authTimeoutMs)
}

const ensureAuthTimer = (ctx: MessageContext): void => {
  if (ctx.state.authTimer === null) restartAuthTimer(ctx)
}

const stopAuthTimerWhenSettled = (ctx: MessageContext): void => {
  if (ctx.state.authAttempt === null && !hasAuthParked(ctx.state)) cancelAuthTimer(ctx.state, ctx.scheduler)
}

const endAuthAttempt = (ctx: MessageContext): void => {
  releaseAuthAttempt(ctx.state)
  stopAuthTimerWhenSettled(ctx)
}

const answerChallenge = async (ctx: MessageContext, challenge: AuthChallenge): Promise<void> => {
  const { state } = ctx
  const handler = ctx.authHandler()
  if (handler === null || state.ws === null) return
  const attempt = Symbol("auth attempt")
  state.authAttempt = attempt
  state.unansweredChallenge = null
  restartAuthTimer(ctx)
  try {
    const signed = await handler(ctx.url, challenge)
    if (state.authAttempt !== attempt) return
    if (signed === null) {
      // Deliberate: a decline holds for the challenge declined, not one the relay sent while the handler decided — see shared ADR-0030 and ADR-0002
      state.authDeclined = state.unansweredChallenge === null
      releaseAuthParked(ctx, AUTH_DECLINED)
      endAuthAttempt(ctx)
      ctx.onStateChange()
      return
    }
    if (state.ws === null) {
      endAuthAttempt(ctx)
      return
    }
    state.authEventId = signed.id
    sendOnWebSocket(state.ws, serialiseAuthMessage(signed))
  } catch (err) {
    if (state.authAttempt !== attempt) return
    endAuthAttempt(ctx)
    queueMicrotask(() => {
      throw err
    })
  }
}

const answerStoredChallenge = (ctx: MessageContext): void => {
  const { state } = ctx
  if (state.authed || state.authAttempt !== null || state.unansweredChallenge === null) return
  answerChallenge(ctx, state.unansweredChallenge)
}

// Deliberate: every challenge frame is a new challenge, even one repeating the string whose answer is in flight — see shared ADR-0030 and ADR-0002
const handleAuthChallenge = (ctx: MessageContext, challenge: AuthChallenge): void => {
  ctx.state.unansweredChallenge = challenge
  ctx.state.authDeclined = false
  answerStoredChallenge(ctx)
}

const endSub = (ctx: MessageContext, wireSub: WireSub, reason: string): void => {
  ctx.state.subIdByFilterHash.delete(wireSub.filterHash)
  for (const listener of wireSub.listeners) listener.onClosed?.(reason)
}

const resumeAfterAuth = (ctx: MessageContext): void => {
  const { state } = ctx
  state.authed = true
  if (state.ws === null) return
  reissueAuthParkedSubs(state, ctx.clock)
  widenNarrowedSubs(state, ctx.clock)
  for (const eventId of state.pendingAuthPublish) {
    const inFlight = state.inFlightPublishes.get(eventId)
    if (!inFlight) continue
    inFlight.restartTimeout()
    sendOnWebSocket(state.ws, serialiseEventMessage(inFlight.event))
  }
  state.pendingAuthPublish.clear()
}

const releaseAuthParked = (ctx: MessageContext, reason: string): void => {
  const { state } = ctx
  for (const eventId of [...state.pendingAuthPublish]) {
    state.inFlightPublishes.get(eventId)?.settle({ ok: false, message: reason })
  }
  state.pendingAuthPublish.clear()
  for (const [subId, wireSub] of state.pendingAuthSubs) {
    endSub(ctx, wireSub, reason)
    closeSubHistory({ subHistory: ctx.subHistory, url: ctx.url, subId, clock: ctx.clock })
  }
  state.pendingAuthSubs.clear()
}

// Deliberate: an auth-required answer to our AUTH asks for the fresh challenge, not a final refusal — see ADR-0001
const handleAuthOk = (ctx: MessageContext, message: OkMessage): void => {
  releaseAuthAttempt(ctx.state)
  if (message.accepted) resumeAfterAuth(ctx)
  else if (message.reason === "auth-required") answerStoredChallenge(ctx)
  else releaseAuthParked(ctx, AUTH_REJECTED + message.message)
  stopAuthTimerWhenSettled(ctx)
  ctx.onStateChange()
}

const handleOk = (ctx: MessageContext, message: OkMessage): void => {
  const { state } = ctx
  if (message.eventId === state.authEventId) {
    handleAuthOk(ctx, message)
    return
  }
  const inFlight = state.inFlightPublishes.get(message.eventId)
  if (!inFlight) return
  if (!message.accepted && message.reason === "auth-required" && canAwaitAuth(ctx)) {
    inFlight.suspendTimeout()
    state.pendingAuthPublish.add(message.eventId)
    answerStoredChallenge(ctx)
    ensureAuthTimer(ctx)
    return
  }
  inFlight.settle({ ok: message.accepted, message: message.message })
}

// Deliberate: a ping NOTICE is a liveness probe answered with a keepalive CLOSE, not a message — see ADR-0004
const handleNotice = (ctx: MessageContext, notice: string): void => {
  if (!PING_NOTICE.test(notice) || ctx.state.ws === null) return
  sendKeepalive(ctx.state.ws)
}

const handleEvent = (ctx: MessageContext, subId: SubscriptionId, event: NostrEvent): void => {
  const wireSub = ctx.state.subs.get(subId)
  if (!wireSub) return
  if (!wireSub.compiledWithoutSearch.matches(event)) return
  ctx.onEventReceived(ctx.url)
  const historyEntry = ctx.subHistory.get(ctx.url)?.get(subId)
  if (historyEntry) historyEntry.eventCount++
  for (const listener of wireSub.listeners) listener.onEvent(event, ctx.url)
}

// Deliberate: a read the relay narrowed until AUTH stays open, and is sent again once the relay accepts our AUTH, before or after its EOSE — see shared ADR-0030 and ADR-0010
const awaitWidening = (ctx: MessageContext, wireSub: WireSub): void => {
  if (wireSub.widening === "sent") return
  wireSub.widening = "awaiting-auth"
  if (ctx.state.authed) widenNarrowedSubs(ctx.state, ctx.clock)
}

const handleEose = (ctx: MessageContext, eose: EoseMessage): void => {
  const wireSub = ctx.state.subs.get(eose.subscriptionId)
  if (!wireSub || wireSub.eoseFired) return
  wireSub.eoseFired = true
  if (wireSub.lastReqSentAt > 0) ctx.onEoseLatency(ctx.url, ctx.clock() - wireSub.lastReqSentAt)
  for (const listener of wireSub.listeners) listener.onEose?.()
  if (eoseHasHint(eose, "auth")) awaitWidening(ctx, wireSub)
}

const handleClosed = (ctx: MessageContext, { subscriptionId: subId, message, reason }: ClosedMessage): void => {
  const { state } = ctx
  const wireSub = state.subs.get(subId) ?? state.pendingSubs.get(subId)
  if (!wireSub) return

  state.subs.delete(subId)
  state.pendingSubs.delete(subId)
  cancelPendingSubTimeout(state, ctx.scheduler, subId)

  if (reason === "auth-required" && canAwaitAuth(ctx)) {
    state.pendingAuthSubs.set(subId, wireSub)
    answerStoredChallenge(ctx)
    ensureAuthTimer(ctx)
  } else {
    endSub(ctx, wireSub, message)
    closeSubHistory({ subHistory: ctx.subHistory, url: ctx.url, subId, clock: ctx.clock })
  }

  ctx.onStateChange()
}

const exceedsUtf8Bytes = (text: string, limit: number): boolean =>
  text.length > limit || (text.length * 3 > limit && textEncoder.encode(text).length > limit)

// Deliberate: an oversized frame is dropped like a malformed one, never closing the connection — see ADR-0008
export const handleRelayMessage = (ctx: MessageContext, msg: MessageEvent): void => {
  if (typeof msg.data !== "string" || exceedsUtf8Bytes(msg.data, ctx.maxMessageBytes)) return
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
      handleEose(ctx, message)
      return
    case "CLOSED":
      handleClosed(ctx, message)
      return
    case "OK":
      handleOk(ctx, message)
      return
    case "NOTICE":
      handleNotice(ctx, message.message)
      return
  }
}
