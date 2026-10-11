import { assertEquals } from "@std/assert"
import { subscriptionIdFixture } from "@innis/nostr-core/testing"
import { type NostrEvent, parseReasonPrefix } from "@innis/nostr-core"
import type { MessageContext } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import { handleRelayMessage } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import type { PublishAck, RelayState } from "../src/infrastructure/web-socket/relay-state.ts"
import { stubInFlightPublish, stubRelayState } from "./_helpers/relay-state.ts"
import { createManualTime, type ManualTime } from "./_helpers/scheduler.ts"
import {
  authEvent,
  context,
  event,
  flushMicrotasks,
  hangingHandler,
  message,
  openSocket,
  wireSubWith,
} from "./_helpers/message.ts"

const SUB_1 = subscriptionIdFixture("sub-1")
const AUTH_TIMEOUT_MS = 60_000
const AUTH_TIMED_OUT = "auth-required: auth timed out"

interface TimedAuth {
  readonly time: ManualTime
  readonly state: RelayState
  readonly ctx: MessageContext
}

const timedContext = (state: RelayState, authHandler: MessageContext["authHandler"]): TimedAuth => {
  const time = createManualTime()
  return { time, state, ctx: context(state, { clock: time.clock, scheduler: time.scheduler, authHandler }) }
}

interface HungHandler extends TimedAuth {
  readonly handlerCalls: () => number
}

const hungHandlerTimedOut = async (): Promise<HungHandler> => {
  let handlerCalls = 0
  const timed = timedContext(stubRelayState({ ws: openSocket([]) }), () => () => {
    handlerCalls++
    return new Promise<NostrEvent | null>(() => {})
  })
  handleRelayMessage(timed.ctx, message(["AUTH", "challenge-1"]))
  timed.time.tick(AUTH_TIMEOUT_MS)
  await flushMicrotasks()
  return { ...timed, handlerCalls: () => handlerCalls }
}

const authSentUnanswered = async (): Promise<TimedAuth> => {
  const timed = timedContext(stubRelayState({ ws: openSocket([]) }), () => () => Promise.resolve(authEvent("d")))
  handleRelayMessage(timed.ctx, message(["AUTH", "challenge-1"]))
  await flushMicrotasks()
  return timed
}

interface TimedOutWork extends TimedAuth {
  readonly sent: ReadonlyArray<string>
  readonly settled: PublishAck | null
  readonly closedReasons: ReadonlyArray<string>
  readonly signed: NostrEvent
}

const parkedWorkTimedOut = async (): Promise<TimedOutWork> => {
  const sent: string[] = []
  let settled: PublishAck | null = null
  const closedReasons: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  const pending = event()
  const signed = authEvent("d")
  state.inFlightPublishes.set(pending.id, stubInFlightPublish(pending, { settle: (ack) => (settled = ack) }))
  state.subs.set(SUB_1, wireSubWith([{ onEvent: () => {}, onClosed: (reason) => closedReasons.push(reason) }]))
  const timed = timedContext(state, () => () => Promise.resolve(signed))
  handleRelayMessage(timed.ctx, message(["AUTH", "challenge-1"]))
  handleRelayMessage(timed.ctx, message(["OK", pending.id, false, "auth-required: please AUTH"]))
  handleRelayMessage(timed.ctx, message(["CLOSED", SUB_1, "auth-required: sign in"]))
  await flushMicrotasks()
  timed.time.tick(AUTH_TIMEOUT_MS)
  return { ...timed, sent, settled, closedReasons, signed }
}

interface ParkedSubscription extends TimedAuth {
  readonly closedReasons: ReadonlyArray<string>
}

const subscriptionParkedBeforeAnyChallengeTimedOut = (): ParkedSubscription => {
  const closedReasons: string[] = []
  const state = stubRelayState({ ws: openSocket([]) })
  state.subs.set(SUB_1, wireSubWith([{ onEvent: () => {}, onClosed: (reason) => closedReasons.push(reason) }]))
  const timed = timedContext(state, hangingHandler())
  handleRelayMessage(timed.ctx, message(["CLOSED", SUB_1, "auth-required: sign in"]))
  timed.time.tick(AUTH_TIMEOUT_MS)
  return { ...timed, closedReasons }
}

const subscriptionParkedThroughAnAuthRequiredAnswer = async (): Promise<ParkedSubscription> => {
  const closedReasons: string[] = []
  const state = stubRelayState({ ws: openSocket([]) })
  state.subs.set(SUB_1, wireSubWith([{ onEvent: () => {}, onClosed: (reason) => closedReasons.push(reason) }]))
  const signed = authEvent("d")
  const timed = timedContext(state, () => () => Promise.resolve(signed))
  handleRelayMessage(timed.ctx, message(["AUTH", "challenge-1"]))
  handleRelayMessage(timed.ctx, message(["CLOSED", SUB_1, "auth-required: sign in"]))
  await flushMicrotasks()
  timed.time.tick(AUTH_TIMEOUT_MS / 2)
  handleRelayMessage(timed.ctx, message(["OK", signed.id, false, "auth-required: challenge issued, please retry"]))
  return { ...timed, closedReasons }
}

const authAccepted = async (): Promise<TimedAuth> => {
  const signed = authEvent("d")
  const timed = timedContext(stubRelayState({ ws: openSocket([]) }), () => () => Promise.resolve(signed))
  handleRelayMessage(timed.ctx, message(["AUTH", "challenge-1"]))
  await flushMicrotasks()
  handleRelayMessage(timed.ctx, message(["OK", signed.id, true, ""]))
  return timed
}

Deno.test("handleRelayMessage - abandons a hung auth handler after authTimeoutMs, clearing the auth timer", async () => {
  const { state } = await hungHandlerTimedOut()
  assertEquals(state.authTimer, null)
})

Deno.test("handleRelayMessage - a hung auth handler leaves the connection unauthenticated after authTimeoutMs", async () => {
  const { state } = await hungHandlerTimedOut()
  assertEquals(state.authed, false)
})

Deno.test("handleRelayMessage - a re-challenge after a hung auth handler times out invokes the handler again", async () => {
  const { ctx, handlerCalls } = await hungHandlerTimedOut()
  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  assertEquals(handlerCalls(), 2)
})

Deno.test("handleRelayMessage - tracks an AUTH awaiting the relay's answer by its event id", async () => {
  const { state } = await authSentUnanswered()
  assertEquals(state.authEventId, authEvent("d").id)
})

Deno.test("handleRelayMessage - abandons an AUTH the relay never answers after authTimeoutMs", async () => {
  const { state, time } = await authSentUnanswered()
  time.tick(AUTH_TIMEOUT_MS)
  assertEquals(state.authEventId, null)
})

Deno.test("handleRelayMessage - clears the auth timer once an AUTH the relay never answers times out", async () => {
  const { state, time } = await authSentUnanswered()
  time.tick(AUTH_TIMEOUT_MS)
  assertEquals(state.authTimer, null)
})

Deno.test("handleRelayMessage - the auth timeout settles a parked publish as auth timed out", async () => {
  const { settled } = await parkedWorkTimedOut()
  assertEquals(settled, { ok: false, message: AUTH_TIMED_OUT })
})

Deno.test("handleRelayMessage - the auth timeout closes a parked subscription as auth timed out", async () => {
  const { closedReasons } = await parkedWorkTimedOut()
  assertEquals(closedReasons, [AUTH_TIMED_OUT])
})

Deno.test("handleRelayMessage - the auth timed out reason reads as auth-required", async () => {
  const { closedReasons } = await parkedWorkTimedOut()
  assertEquals(parseReasonPrefix(closedReasons[0] ?? ""), "auth-required")
})

Deno.test("handleRelayMessage - the auth timeout empties the parked publishes", async () => {
  const { state } = await parkedWorkTimedOut()
  assertEquals(state.pendingAuthPublish.size, 0)
})

Deno.test("handleRelayMessage - the auth timeout empties the parked subscriptions", async () => {
  const { state } = await parkedWorkTimedOut()
  assertEquals(state.pendingAuthSubs.size, 0)
})

Deno.test("handleRelayMessage - the auth timeout abandons the AUTH in flight while work is parked", async () => {
  const { state } = await parkedWorkTimedOut()
  assertEquals(state.authEventId, null)
})

Deno.test("handleRelayMessage - the auth timeout clears its own timer while work is parked", async () => {
  const { state } = await parkedWorkTimedOut()
  assertEquals(state.authTimer, null)
})

Deno.test("handleRelayMessage - a late OK for an AUTH the auth timeout abandoned re-issues nothing", async () => {
  const { ctx, sent, signed } = await parkedWorkTimedOut()
  handleRelayMessage(ctx, message(["OK", signed.id, true, ""]))
  assertEquals(sent, [JSON.stringify(["AUTH", signed])])
})

Deno.test("handleRelayMessage - work parked before any challenge arrives is settled by the auth timeout", () => {
  const { closedReasons } = subscriptionParkedBeforeAnyChallengeTimedOut()
  assertEquals(closedReasons, [AUTH_TIMED_OUT])
})

Deno.test("handleRelayMessage - the auth timeout empties work parked before any challenge arrives", () => {
  const { state } = subscriptionParkedBeforeAnyChallengeTimedOut()
  assertEquals(state.pendingAuthSubs.size, 0)
})

Deno.test("handleRelayMessage - the auth timeout leaves no timer pending once it settles work parked before any challenge", () => {
  const { time } = subscriptionParkedBeforeAnyChallengeTimedOut()
  assertEquals(time.pendingCount(), 0)
})

Deno.test("handleRelayMessage - an auth-required answer to our AUTH keeps a subscription parked", async () => {
  const { state } = await subscriptionParkedThroughAnAuthRequiredAnswer()
  assertEquals(state.pendingAuthSubs.size, 1)
})

Deno.test("handleRelayMessage - the auth timeout keeps running while work stays parked after an auth-required answer to our AUTH", async () => {
  const { closedReasons, time } = await subscriptionParkedThroughAnAuthRequiredAnswer()
  time.tick(AUTH_TIMEOUT_MS / 2)
  assertEquals(closedReasons, [AUTH_TIMED_OUT])
})

Deno.test("handleRelayMessage - the auth timeout leaves no timer pending once it settles work parked through an auth-required answer", async () => {
  const { time } = await subscriptionParkedThroughAnAuthRequiredAnswer()
  time.tick(AUTH_TIMEOUT_MS / 2)
  assertEquals(time.pendingCount(), 0)
})

Deno.test("handleRelayMessage - clears the auth timeout timer once the relay accepts the AUTH event", async () => {
  const { time } = await authAccepted()
  assertEquals(time.pendingCount(), 0)
})
