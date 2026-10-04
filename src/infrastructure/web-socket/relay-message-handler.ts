import type { AuthChallenge, NostrEvent, RelayMessage, RelayUrl, SubscriptionId } from "@innis/nostr-core"
import { authChallengesEqual, parseRelayMessage, serialiseAuthMessage, serialiseEventMessage } from "@innis/nostr-core"
import type { AuthHandler } from "../../application/port/auth-handler.ts"
import type { WallClock } from "../../application/port/clock.ts"
import type { Scheduler } from "../../application/port/scheduler.ts"
import { closeSubHistory, type SubHistoryRecord } from "../../application/service/relay-history.ts"
import type { RelayState } from "./relay-state.ts"
import { cancelAuthTimer, cancelPendingSubTimeout, promoteAuthedSubs, releaseAuthAttempt } from "./relay-state.ts"
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

const PING_NOTICE = "ping"
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
  state.answeredChallenge = challenge
  restartAuthTimer(ctx)
  try {
    const signed = await handler(ctx.url, challenge)
    if (state.authAttempt !== attempt) return
    if (signed === null) {
      state.authDeclined = true
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

const handleAuthChallenge = (ctx: MessageContext, challenge: AuthChallenge): void => {
  ctx.state.challenge = challenge
  ctx.state.authDeclined = false
  if (ctx.state.authed || ctx.state.authAttempt !== null) return
  answerChallenge(ctx, challenge)
}

const isAnswered = (challenge: AuthChallenge, answered: AuthChallenge | null): boolean =>
  answered !== null && authChallengesEqual(challenge, answered)

const answerStoredChallenge = (ctx: MessageContext): void => {
  const { state } = ctx
  if (state.authed || state.authAttempt !== null) return
  if (state.challenge === null || isAnswered(state.challenge, state.answeredChallenge)) return
  answerChallenge(ctx, state.challenge)
}

const endSub = (ctx: MessageContext, wireSub: WireSub, reason: string): void => {
  ctx.state.subIdByFilterHash.delete(wireSub.filterHash)
  for (const listener of wireSub.listeners) listener.onClosed?.(reason)
}

const resumeAuthParked = (ctx: MessageContext): void => {
  const { state } = ctx
  state.authed = true
  if (state.ws === null) return
  promoteAuthedSubs(state, ctx.clock)
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
  if (message.accepted) resumeAuthParked(ctx)
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
  if (notice.trim().toLowerCase() !== PING_NOTICE || ctx.state.ws === null) return
  sendKeepalive(ctx.state.ws)
}

const handleEvent = (ctx: MessageContext, subId: SubscriptionId, event: NostrEvent): void => {
  const wireSub = ctx.state.subs.get(subId)
  if (!wireSub) return
  if (!wireSub.compiled.matches(event)) return
  ctx.onEventReceived(ctx.url)
  const historyEntry = ctx.subHistory.get(ctx.url)?.get(subId)
  if (historyEntry) historyEntry.eventCount++
  // Listener arrays are copy-on-write: never mutated in place, only replaced. `for...of` evaluates
  // `wireSub.listeners` once, so an onEvent that re-enters the pool (subscribe or unsubscribe on
  // this same filter hash) swaps in a new array without affecting the in-flight iteration.
  for (const listener of wireSub.listeners) listener.onEvent(event, ctx.url)
}

const handleEose = (ctx: MessageContext, subId: SubscriptionId): void => {
  const wireSub = ctx.state.subs.get(subId)
  if (!wireSub || wireSub.eoseFired) return
  wireSub.eoseFired = true
  // Measure against the live REQ, not the original subscribe time: reqSentAt is re-stamped on every
  // reconnect/auth re-issue, so a long backoff window never leaks into the latency sample.
  if (wireSub.reqSentAt > 0) ctx.onEoseLatency(ctx.url, ctx.clock() - wireSub.reqSentAt)
  for (const listener of wireSub.listeners) listener.onEose?.()
}

const handleClosed = (ctx: MessageContext, { subscriptionId: subId, message, reason }: ClosedMessage): void => {
  const { state } = ctx
  const wireSub = state.subs.get(subId) ?? state.pendingSubs.get(subId)
  if (!wireSub) return

  state.subs.delete(subId)
  state.pendingSubs.delete(subId)
  cancelPendingSubTimeout(state, ctx.scheduler, subId)

  if (reason === "auth-required" && canAwaitAuth(ctx)) {
    // The sub is re-parked, not closed: it lives on in pendingAuthSubs awaiting the AUTH handshake,
    // so its history entry stays open. Stamping closedAt here would mislabel a still-pending sub as
    // closed (masked while live, but wrong the moment it later genuinely closes).
    state.pendingAuthSubs.set(subId, wireSub)
    answerStoredChallenge(ctx)
    ensureAuthTimer(ctx)
  } else {
    // A relay-initiated CLOSED terminates the subscription — unlike a socket drop it will not be
    // revived. Fire the distinct onClosed terminal signal: CLOSED is not EOSE, so a persistent
    // listener learns the stream is dead rather than that the backlog merely ended, and an
    // EOSE-or-timeout fan-in settles now instead of hanging until its hard timeout.
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
      handleEose(ctx, message.subscriptionId)
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
