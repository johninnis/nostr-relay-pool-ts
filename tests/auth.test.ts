import { assertEquals } from "@std/assert"
import { subscriptionIdFixture } from "@innis/nostr-core/testing"
import { type NostrEvent, parseReasonPrefix } from "@innis/nostr-core"
import type { MessageContext } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import { handleRelayMessage } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import type { PublishAck } from "../src/infrastructure/web-socket/relay-state.ts"
import { stubInFlightPublish, stubRelayState, stubWireSub } from "./_helpers/relay-state.ts"
import { createManualTime } from "./_helpers/scheduler.ts"
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

Deno.test("handleRelayMessage - parks an auth-required publish when an auth handler can answer", () => {
  const state = stubRelayState()
  const pending = event()
  state.inFlightPublishes.set(pending.id, stubInFlightPublish(pending))
  handleRelayMessage(
    context(state, { authHandler: () => () => new Promise<NostrEvent | null>(() => {}) }),
    message(["OK", pending.id, false, "auth-required: please AUTH"]),
  )
  assertEquals([...state.pendingAuthPublish], [pending.id])
  assertEquals(state.inFlightPublishes.size, 1)
})

Deno.test("handleRelayMessage - returns an auth-required refusal at once when no auth handler is configured", () => {
  let result: PublishAck | null = null
  const state = stubRelayState()
  const pending = event()
  state.inFlightPublishes.set(pending.id, stubInFlightPublish(pending, { settle: (ack) => (result = ack) }))
  handleRelayMessage(context(state), message(["OK", pending.id, false, "auth-required: please AUTH"]))
  assertEquals(result, { ok: false, message: "auth-required: please AUTH" })
  assertEquals(state.pendingAuthPublish.size, 0)
})

Deno.test("handleRelayMessage - returns an auth-required refusal at once when the relay already accepted our AUTH", () => {
  let result: PublishAck | null = null
  const state = stubRelayState({ authed: true })
  const pending = event()
  state.inFlightPublishes.set(pending.id, stubInFlightPublish(pending, { settle: (ack) => (result = ack) }))
  handleRelayMessage(
    context(state, { authHandler: () => () => Promise.resolve(authEvent("d")) }),
    message(["OK", pending.id, false, "auth-required: please AUTH"]),
  )
  assertEquals(result, { ok: false, message: "auth-required: please AUTH" })
})

Deno.test("handleRelayMessage - resends a parked publish only once the relay accepts the AUTH event", async () => {
  const sent: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  const pending = event()
  const signed = authEvent("d")
  state.inFlightPublishes.set(pending.id, stubInFlightPublish(pending))
  const ctx = context(state, { authHandler: () => () => Promise.resolve(signed) })

  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  handleRelayMessage(ctx, message(["OK", pending.id, false, "auth-required: please AUTH"]))
  await flushMicrotasks()
  assertEquals(sent, [JSON.stringify(["AUTH", signed])])
  assertEquals(state.authed, false)

  handleRelayMessage(ctx, message(["OK", signed.id, true, ""]))
  assertEquals(state.authed, true)
  assertEquals(sent.slice(1), [JSON.stringify(["EVENT", pending])])
  assertEquals(state.pendingAuthPublish.size, 0)
})

Deno.test("handleRelayMessage - an auth-required OK for the AUTH event answers the fresh challenge and keeps publishes parked", async () => {
  const sent: string[] = []
  const challenges: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  const pending = event()
  const first = authEvent("d")
  const second = authEvent("e")
  state.inFlightPublishes.set(pending.id, stubInFlightPublish(pending))
  const ctx = context(state, {
    authHandler: () => (_url, challenge) => {
      challenges.push(challenge)
      return Promise.resolve(challenges.length === 1 ? first : second)
    },
  })

  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  handleRelayMessage(ctx, message(["OK", pending.id, false, "auth-required: please AUTH"]))
  await flushMicrotasks()
  handleRelayMessage(ctx, message(["AUTH", "challenge-2"]))
  handleRelayMessage(ctx, message(["OK", first.id, false, "auth-required: challenge issued, please retry"]))
  await flushMicrotasks()

  assertEquals(challenges, ["challenge-1", "challenge-2"])
  assertEquals([...state.pendingAuthPublish], [pending.id])

  handleRelayMessage(ctx, message(["OK", second.id, true, ""]))
  assertEquals(sent.at(-1), JSON.stringify(["EVENT", pending]))
})

Deno.test("handleRelayMessage - an AUTH event refused for another reason resolves parked work with an auth-required reason", async () => {
  let result: PublishAck | null = null
  const reasons: string[] = []
  const state = stubRelayState({ ws: openSocket([]) })
  const pending = event()
  const signed = authEvent("d")
  state.inFlightPublishes.set(pending.id, stubInFlightPublish(pending, { settle: (ack) => (result = ack) }))
  state.pendingAuthPublish.add(pending.id)
  state.pendingAuthSubs.set(SUB_1, wireSubWith([{ onEvent: () => {}, onClosed: (reason) => reasons.push(reason) }]))
  const ctx = context(state, { authHandler: () => () => Promise.resolve(signed) })

  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  await flushMicrotasks()
  handleRelayMessage(ctx, message(["OK", signed.id, false, "restricted: key not allowed"]))

  const expected = "auth-required: auth rejected: restricted: key not allowed"
  assertEquals(result, { ok: false, message: expected })
  assertEquals(reasons, [expected])
  assertEquals(parseReasonPrefix(expected), "auth-required")
  assertEquals(state.pendingAuthSubs.size, 0)
  assertEquals(state.authed, false)
})

Deno.test("handleRelayMessage - an auth-required refusal answers a stored challenge no handler has answered yet", async () => {
  const challenges: string[] = []
  const state = stubRelayState({ ws: openSocket([]) })
  let handler: MessageContext["authHandler"] = () => null
  const ctx = context(state, { authHandler: () => handler() })

  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  handler = () => (_url, challenge) => {
    challenges.push(challenge)
    return Promise.resolve(authEvent("d"))
  }
  state.subs.set(SUB_1, stubWireSub())
  handleRelayMessage(ctx, message(["CLOSED", SUB_1, "auth-required: sign in"]))
  await flushMicrotasks()

  assertEquals(challenges, ["challenge-1"])
})

Deno.test("handleRelayMessage - a ping NOTICE is answered with a keepalive CLOSE", () => {
  const sent: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  handleRelayMessage(context(state), message(["NOTICE", " PING "]))
  assertEquals(sent, [JSON.stringify(["CLOSE", "keepalive"])])
})

Deno.test("handleRelayMessage - any other NOTICE sends nothing", () => {
  const sent: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  handleRelayMessage(context(state), message(["NOTICE", "ping me later"]))
  assertEquals(sent, [])
})

Deno.test("handleRelayMessage - an auth-required CLOSED fires neither onEose nor onClosed when an auth handler can answer", () => {
  let eoseCalls = 0
  let closedCalls = 0
  const state = stubRelayState()
  const sub = wireSubWith([{ onEvent: () => {}, onEose: () => eoseCalls++, onClosed: () => closedCalls++ }])
  state.subs.set(SUB_1, sub)
  handleRelayMessage(
    context(state, { authHandler: hangingHandler() }),
    message(["CLOSED", SUB_1, "auth-required: restricted"]),
  )
  assertEquals(eoseCalls, 0)
  assertEquals(closedCalls, 0)
  assertEquals(state.subs.has(SUB_1), false)
  assertEquals(state.pendingAuthSubs.has(SUB_1), true)
})

Deno.test("handleRelayMessage - an auth-required CLOSED is terminal when no auth handler is configured", () => {
  const reasons: string[] = []
  const state = stubRelayState()
  state.subs.set(SUB_1, wireSubWith([{ onEvent: () => {}, onClosed: (reason) => reasons.push(reason) }]))
  handleRelayMessage(context(state), message(["CLOSED", SUB_1, "auth-required: sign in"]))
  assertEquals(reasons, ["auth-required: sign in"])
  assertEquals(state.pendingAuthSubs.size, 0)
})

Deno.test("handleRelayMessage - an auth-required CLOSED is terminal once the relay has accepted our AUTH", () => {
  const reasons: string[] = []
  const state = stubRelayState({ authed: true })
  state.subs.set(SUB_1, wireSubWith([{ onEvent: () => {}, onClosed: (reason) => reasons.push(reason) }]))
  handleRelayMessage(
    context(state, { authHandler: hangingHandler() }),
    message(["CLOSED", SUB_1, "auth-required: sign in"]),
  )
  assertEquals(reasons, ["auth-required: sign in"])
  assertEquals(state.authed, true)
})

Deno.test("handleRelayMessage - re-issues an auth-parked subscription only once the relay accepts the AUTH event", async () => {
  const sent: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  const signed = authEvent("d")
  const sub = stubWireSub()
  state.subs.set(SUB_1, sub)
  const ctx = context(state, { authHandler: () => () => Promise.resolve(signed) })

  handleRelayMessage(ctx, message(["CLOSED", SUB_1, "auth-required: sign in"]))
  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  await flushMicrotasks()
  assertEquals(sent, [JSON.stringify(["AUTH", signed])])

  handleRelayMessage(ctx, message(["OK", signed.id, true, ""]))
  assertEquals(sent.slice(1), [JSON.stringify(["REQ", SUB_1, ...sub.filters])])
  assertEquals(state.subs.get(SUB_1), sub)
  assertEquals(state.pendingAuthSubs.size, 0)
})

Deno.test("handleRelayMessage - abandons a hung auth handler after authTimeoutMs so a re-challenge can retry", async () => {
  const time = createManualTime()
  const state = stubRelayState({ ws: openSocket([]) })
  let handlerCalls = 0
  const ctx = context(state, {
    clock: time.clock,
    scheduler: time.scheduler,
    authHandler: () => () => {
      handlerCalls++
      return new Promise<NostrEvent | null>(() => {})
    },
  })

  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  assertEquals(handlerCalls, 1)

  time.tick(60_000)
  await flushMicrotasks()

  assertEquals(state.authTimer, null, "the timed-out challenge must be released")
  assertEquals(state.authed, false)

  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  assertEquals(handlerCalls, 2, "a re-challenge after the timeout must invoke the handler again")
  time.tick(60_000)
})

Deno.test("handleRelayMessage - abandons an AUTH the relay never answers after authTimeoutMs", async () => {
  const time = createManualTime()
  const state = stubRelayState({ ws: openSocket([]) })
  const ctx = context(state, {
    clock: time.clock,
    scheduler: time.scheduler,
    authHandler: () => () => Promise.resolve(authEvent("d")),
  })

  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  await flushMicrotasks()
  assertEquals(state.authEventId, authEvent("d").id)

  time.tick(60_000)

  assertEquals(state.authEventId, null)
  assertEquals(state.authTimer, null)
})

Deno.test("handleRelayMessage - the auth timeout settles parked publishes and subscriptions as auth timed out", async () => {
  const time = createManualTime()
  const sent: string[] = []
  let result: PublishAck | null = null
  const reasons: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  const pending = event()
  state.inFlightPublishes.set(pending.id, stubInFlightPublish(pending, { settle: (ack) => (result = ack) }))
  state.subs.set(SUB_1, wireSubWith([{ onEvent: () => {}, onClosed: (reason) => reasons.push(reason) }]))
  const signed = authEvent("d")
  const ctx = context(state, {
    clock: time.clock,
    scheduler: time.scheduler,
    authHandler: () => () => Promise.resolve(signed),
  })

  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  handleRelayMessage(ctx, message(["OK", pending.id, false, "auth-required: please AUTH"]))
  handleRelayMessage(ctx, message(["CLOSED", SUB_1, "auth-required: sign in"]))
  await flushMicrotasks()
  time.tick(60_000)

  const expected = "auth-required: auth timed out"
  assertEquals(result, { ok: false, message: expected })
  assertEquals(reasons, [expected])
  assertEquals(parseReasonPrefix(expected), "auth-required")
  assertEquals([state.pendingAuthPublish.size, state.pendingAuthSubs.size], [0, 0])
  assertEquals([state.authEventId, state.authTimer], [null, null])

  handleRelayMessage(ctx, message(["OK", signed.id, true, ""]))
  assertEquals(sent, [JSON.stringify(["AUTH", signed])], "a late OK for the abandoned AUTH must re-issue nothing")
})

Deno.test("handleRelayMessage - work parked before any challenge arrives is settled by the auth timeout", () => {
  const time = createManualTime()
  const reasons: string[] = []
  const state = stubRelayState({ ws: openSocket([]) })
  state.subs.set(SUB_1, wireSubWith([{ onEvent: () => {}, onClosed: (reason) => reasons.push(reason) }]))
  const ctx = context(state, { clock: time.clock, scheduler: time.scheduler, authHandler: hangingHandler() })

  handleRelayMessage(ctx, message(["CLOSED", SUB_1, "auth-required: sign in"]))
  assertEquals(state.pendingAuthSubs.size, 1)
  time.tick(60_000)

  assertEquals(reasons, ["auth-required: auth timed out"])
  assertEquals(state.pendingAuthSubs.size, 0)
  assertEquals(time.pendingCount(), 0)
})

Deno.test("handleRelayMessage - the auth timeout keeps running while work stays parked after an auth-required answer to our AUTH", async () => {
  const time = createManualTime()
  const reasons: string[] = []
  const state = stubRelayState({ ws: openSocket([]) })
  state.subs.set(SUB_1, wireSubWith([{ onEvent: () => {}, onClosed: (reason) => reasons.push(reason) }]))
  const signed = authEvent("d")
  const ctx = context(state, {
    clock: time.clock,
    scheduler: time.scheduler,
    authHandler: () => () => Promise.resolve(signed),
  })

  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  handleRelayMessage(ctx, message(["CLOSED", SUB_1, "auth-required: sign in"]))
  await flushMicrotasks()
  time.tick(30_000)
  handleRelayMessage(ctx, message(["OK", signed.id, false, "auth-required: challenge issued, please retry"]))
  assertEquals(state.pendingAuthSubs.size, 1)
  time.tick(30_000)

  assertEquals(reasons, ["auth-required: auth timed out"])
  assertEquals(time.pendingCount(), 0)
})

Deno.test("handleRelayMessage - clears the auth timeout timer once the relay accepts the AUTH event", async () => {
  const time = createManualTime()
  const sent: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  const signed = authEvent("d")
  const ctx = context(state, {
    clock: time.clock,
    scheduler: time.scheduler,
    authHandler: () => () => Promise.resolve(signed),
  })

  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  await flushMicrotasks()
  assertEquals(sent.length, 1, "the signed AUTH event must be sent")
  assertEquals(state.authed, false, "sending AUTH is not being authenticated")

  handleRelayMessage(ctx, message(["OK", signed.id, true, ""]))
  assertEquals(state.authed, true)
  assertEquals(time.pendingCount(), 0, "the timeout timer must be cleared once the relay answers")
})

Deno.test("handleRelayMessage - a declined challenge releases the auth attempt without sending", async () => {
  const time = createManualTime()
  const sent: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  const ctx = context(state, {
    clock: time.clock,
    scheduler: time.scheduler,
    authHandler: () => () => Promise.resolve(null),
  })

  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  await flushMicrotasks()

  assertEquals(sent, [])
  assertEquals(state.authTimer, null)
  assertEquals(time.pendingCount(), 0)
})

Deno.test("handleRelayMessage - a declined challenge resolves parked work with an auth-required reason", async () => {
  let result: PublishAck | null = null
  const reasons: string[] = []
  const state = stubRelayState({ ws: openSocket([]) })
  const pending = event()
  state.inFlightPublishes.set(pending.id, stubInFlightPublish(pending, { settle: (ack) => (result = ack) }))
  state.subs.set(SUB_1, wireSubWith([{ onEvent: () => {}, onClosed: (reason) => reasons.push(reason) }]))
  const ctx = context(state, { authHandler: () => () => Promise.resolve(null) })

  handleRelayMessage(ctx, message(["OK", pending.id, false, "auth-required: please AUTH"]))
  handleRelayMessage(ctx, message(["CLOSED", SUB_1, "auth-required: sign in"]))
  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  await flushMicrotasks()

  assertEquals(result, { ok: false, message: "auth-required: auth declined" })
  assertEquals(reasons, ["auth-required: auth declined"])
  assertEquals(state.pendingAuthPublish.size, 0)
  assertEquals(state.pendingAuthSubs.size, 0)
})

Deno.test("handleRelayMessage - after a declined challenge an auth-required refusal is the outcome until a new challenge", async () => {
  const reasons: string[] = []
  const state = stubRelayState({ ws: openSocket([]) })
  const ctx = context(state, { authHandler: () => () => Promise.resolve(null) })

  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  await flushMicrotasks()
  state.subs.set(SUB_1, wireSubWith([{ onEvent: () => {}, onClosed: (reason) => reasons.push(reason) }]))
  handleRelayMessage(ctx, message(["CLOSED", SUB_1, "auth-required: sign in"]))
  assertEquals(reasons, ["auth-required: sign in"])

  handleRelayMessage(ctx, message(["AUTH", "challenge-2"]))
  assertEquals(state.authDeclined, false)
  await flushMicrotasks()
})
