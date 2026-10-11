import { assertEquals } from "@std/assert"
import { subscriptionIdFixture } from "@innis/nostr-core/testing"
import type { NostrEvent } from "@innis/nostr-core"
import type { MessageContext } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import { handleRelayMessage } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import { reissueReq, type RelayState } from "../src/infrastructure/web-socket/relay-state.ts"
import type { WireSub } from "../src/infrastructure/web-socket/wire-sub.ts"
import { stubRelayState, stubWireSub } from "./_helpers/relay-state.ts"
import { authEvent, context, flushMicrotasks, message, openSocket } from "./_helpers/message.ts"

const SUB_1 = subscriptionIdFixture("sub-1")

interface OpenSubscription {
  readonly sent: Array<string>
  readonly state: RelayState
  readonly ctx: MessageContext
  readonly sub: WireSub
  readonly signed: NostrEvent
  readonly eoseCalls: () => number
  readonly closedReasons: ReadonlyArray<string>
}

const openSubscription = (overrides: Partial<RelayState> = {}): OpenSubscription => {
  const sent: string[] = []
  const closedReasons: string[] = []
  let eoseCalls = 0
  const state = stubRelayState({ ws: openSocket(sent), ...overrides })
  const signed = authEvent("d")
  const sub = stubWireSub({
    listeners: [{ onEvent: () => {}, onEose: () => eoseCalls++, onClosed: (reason) => closedReasons.push(reason) }],
  })
  state.subs.set(SUB_1, sub)
  const ctx = context(state, { authHandler: () => () => Promise.resolve(signed) })
  return { sent, state, ctx, sub, signed, eoseCalls: () => eoseCalls, closedReasons }
}

const reqFrame = (sub: WireSub): string => JSON.stringify(["REQ", SUB_1, ...sub.filters])

const eoseBeforeAuthIsAccepted = async (hints: ReadonlyArray<string>): Promise<OpenSubscription> => {
  const run = openSubscription()
  handleRelayMessage(run.ctx, message(["AUTH", "challenge-1"]))
  await flushMicrotasks()
  handleRelayMessage(run.ctx, message(["EOSE", SUB_1, hints]))
  return run
}

const acceptAuth = (run: OpenSubscription): OpenSubscription => {
  handleRelayMessage(run.ctx, message(["OK", run.signed.id, true, ""]))
  return run
}

const eoseAfterAuthIsAccepted = (hints: ReadonlyArray<string>): OpenSubscription => {
  const run = openSubscription({ authed: true })
  handleRelayMessage(run.ctx, message(["EOSE", SUB_1, hints]))
  return run
}

const framesAfterTheAuth = (run: OpenSubscription): ReadonlyArray<string> => run.sent.slice(1)

Deno.test("handleRelayMessage - an EOSE with the auth hint sends nothing before the relay accepts the AUTH", async () => {
  const run = await eoseBeforeAuthIsAccepted(["auth"])
  assertEquals(framesAfterTheAuth(run), [])
})

Deno.test("handleRelayMessage - an EOSE with the auth hint leaves its subscription open", async () => {
  const { state, sub } = await eoseBeforeAuthIsAccepted(["auth"])
  assertEquals(state.subs.get(SUB_1), sub)
})

Deno.test("handleRelayMessage - an EOSE with the auth hint parks nothing", async () => {
  const { state } = await eoseBeforeAuthIsAccepted(["auth"])
  assertEquals(state.pendingAuthSubs.size, 0)
})

Deno.test("handleRelayMessage - an EOSE with the auth hint fires onEose", async () => {
  const { eoseCalls } = await eoseBeforeAuthIsAccepted(["auth"])
  assertEquals(eoseCalls(), 1)
})

Deno.test("handleRelayMessage - a subscription whose EOSE carried the auth hint is sent again on its id once the relay accepts the AUTH", async () => {
  const run = acceptAuth(await eoseBeforeAuthIsAccepted(["auth"]))
  assertEquals(framesAfterTheAuth(run), [reqFrame(run.sub)])
})

Deno.test("handleRelayMessage - a subscription whose EOSE carried no hint is not sent again when the relay accepts the AUTH", async () => {
  const run = acceptAuth(await eoseBeforeAuthIsAccepted([]))
  assertEquals(framesAfterTheAuth(run), [])
})

Deno.test("handleRelayMessage - a subscription whose EOSE carried only other hints is not sent again when the relay accepts the AUTH", async () => {
  const run = acceptAuth(await eoseBeforeAuthIsAccepted(["finish", "Auth"]))
  assertEquals(framesAfterTheAuth(run), [])
})

Deno.test("handleRelayMessage - the relay's EOSE for a subscription sent again fires onEose again", async () => {
  const run = acceptAuth(await eoseBeforeAuthIsAccepted(["auth"]))
  handleRelayMessage(run.ctx, message(["EOSE", SUB_1]))
  assertEquals(run.eoseCalls(), 2)
})

Deno.test("handleRelayMessage - a subscription sent again keeps its listeners", async () => {
  const { state, sub } = acceptAuth(await eoseBeforeAuthIsAccepted(["auth"]))
  assertEquals(state.subs.get(SUB_1)?.listeners, sub.listeners)
})

Deno.test("handleRelayMessage - a subscription sent again whose EOSE carries the auth hint again is not sent a third time", async () => {
  const run = acceptAuth(await eoseBeforeAuthIsAccepted(["auth"]))
  handleRelayMessage(run.ctx, message(["EOSE", SUB_1, ["auth"]]))
  assertEquals(framesAfterTheAuth(run), [reqFrame(run.sub)])
})

Deno.test("handleRelayMessage - an AUTH the relay refuses leaves a subscription whose EOSE carried the auth hint open", async () => {
  const run = await eoseBeforeAuthIsAccepted(["auth"])
  handleRelayMessage(run.ctx, message(["OK", run.signed.id, false, "restricted: key not allowed"]))
  assertEquals(run.closedReasons, [])
})

Deno.test("handleRelayMessage - a subscription with the auth hint later parked by an auth-required CLOSED is sent once when the relay accepts the AUTH", async () => {
  const run = await eoseBeforeAuthIsAccepted(["auth"])
  handleRelayMessage(run.ctx, message(["CLOSED", SUB_1, "auth-required: sign in"]))
  acceptAuth(run)
  assertEquals(framesAfterTheAuth(run), [reqFrame(run.sub)])
})

Deno.test("handleRelayMessage - an EOSE with the auth hint on a connection whose AUTH the relay accepted sends the subscription again at once", () => {
  const run = eoseAfterAuthIsAccepted(["auth"])
  assertEquals(run.sent, [reqFrame(run.sub)])
})

Deno.test("handleRelayMessage - an EOSE without the auth hint on a connection whose AUTH the relay accepted sends nothing", () => {
  const run = eoseAfterAuthIsAccepted([])
  assertEquals(run.sent, [])
})

Deno.test("handleRelayMessage - a subscription closed by its listener at its hinted EOSE is not sent again", () => {
  const sent: string[] = []
  const state = stubRelayState({ ws: openSocket(sent), authed: true })
  state.subs.set(SUB_1, stubWireSub({ listeners: [{ onEvent: () => {}, onEose: () => state.subs.delete(SUB_1) }] }))
  handleRelayMessage(context(state), message(["EOSE", SUB_1, ["auth"]]))
  assertEquals(sent, [])
})

Deno.test("handleRelayMessage - a subscription sent again on one connection is sent again on the next connection's hinted EOSE", () => {
  const widened = eoseAfterAuthIsAccepted(["auth"])
  const sent: string[] = []
  const ws = openSocket(sent)
  const reconnected = stubRelayState({ ws, authed: true })
  reconnected.subs.set(SUB_1, widened.sub)
  reissueReq({ ws, subId: SUB_1, sub: widened.sub, clock: () => 0 })
  handleRelayMessage(context(reconnected), message(["EOSE", SUB_1, ["auth"]]))
  assertEquals(sent, [reqFrame(widened.sub), reqFrame(widened.sub)])
})
