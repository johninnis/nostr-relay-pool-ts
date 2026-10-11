import { assertEquals } from "@std/assert"
import { subscriptionIdFixture } from "@innis/nostr-core/testing"
import type { NostrEvent } from "@innis/nostr-core"
import type { MessageContext } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import { handleRelayMessage } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import type { RelayState } from "../src/infrastructure/web-socket/relay-state.ts"
import type { WireSub } from "../src/infrastructure/web-socket/wire-sub.ts"
import { stubRelayState, stubWireSub } from "./_helpers/relay-state.ts"
import {
  authEvent,
  context,
  flushMicrotasks,
  hangingHandler,
  message,
  openSocket,
  wireSubWith,
} from "./_helpers/message.ts"

const SUB_1 = subscriptionIdFixture("sub-1")
const SIGN_IN = "auth-required: sign in"

interface ClosedSubscription {
  readonly state: RelayState
  readonly eoseCalls: number
  readonly closedReasons: ReadonlyArray<string>
}

const closeWithAuthRequired = (state: RelayState, authHandler: MessageContext["authHandler"]): ClosedSubscription => {
  let eoseCalls = 0
  const closedReasons: string[] = []
  state.subs.set(
    SUB_1,
    wireSubWith([{
      onEvent: () => {},
      onEose: () => eoseCalls++,
      onClosed: (reason) => closedReasons.push(reason),
    }]),
  )
  handleRelayMessage(context(state, { authHandler }), message(["CLOSED", SUB_1, SIGN_IN]))
  return { state, eoseCalls, closedReasons }
}

interface ParkedSubscription {
  readonly sent: ReadonlyArray<string>
  readonly state: RelayState
  readonly ctx: MessageContext
  readonly sub: WireSub
  readonly signed: NostrEvent
}

const subscriptionParkedWhileAuthIsSent = async (): Promise<ParkedSubscription> => {
  const sent: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  const signed = authEvent("d")
  const sub = stubWireSub()
  state.subs.set(SUB_1, sub)
  const ctx = context(state, { authHandler: () => () => Promise.resolve(signed) })
  handleRelayMessage(ctx, message(["CLOSED", SUB_1, SIGN_IN]))
  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  await flushMicrotasks()
  return { sent, state, ctx, sub, signed }
}

const acceptAuth = (parked: ParkedSubscription): ParkedSubscription => {
  handleRelayMessage(parked.ctx, message(["OK", parked.signed.id, true, ""]))
  return parked
}

Deno.test("handleRelayMessage - an auth-required CLOSED fires no onEose when an auth handler can answer", () => {
  const { eoseCalls } = closeWithAuthRequired(stubRelayState(), hangingHandler())
  assertEquals(eoseCalls, 0)
})

Deno.test("handleRelayMessage - an auth-required CLOSED fires no onClosed when an auth handler can answer", () => {
  const { closedReasons } = closeWithAuthRequired(stubRelayState(), hangingHandler())
  assertEquals(closedReasons, [])
})

Deno.test("handleRelayMessage - an auth-required CLOSED takes the subscription off the live ones when an auth handler can answer", () => {
  const { state } = closeWithAuthRequired(stubRelayState(), hangingHandler())
  assertEquals(state.subs.has(SUB_1), false)
})

Deno.test("handleRelayMessage - an auth-required CLOSED parks the subscription for AUTH when an auth handler can answer", () => {
  const { state } = closeWithAuthRequired(stubRelayState(), hangingHandler())
  assertEquals(state.pendingAuthSubs.has(SUB_1), true)
})

Deno.test("handleRelayMessage - an auth-required CLOSED is terminal when no auth handler is configured", () => {
  const { closedReasons } = closeWithAuthRequired(stubRelayState(), () => null)
  assertEquals(closedReasons, [SIGN_IN])
})

Deno.test("handleRelayMessage - an auth-required CLOSED parks nothing when no auth handler is configured", () => {
  const { state } = closeWithAuthRequired(stubRelayState(), () => null)
  assertEquals(state.pendingAuthSubs.size, 0)
})

Deno.test("handleRelayMessage - an auth-required CLOSED is terminal once the relay has accepted our AUTH", () => {
  const { closedReasons } = closeWithAuthRequired(stubRelayState({ authed: true }), hangingHandler())
  assertEquals(closedReasons, [SIGN_IN])
})

Deno.test("handleRelayMessage - an auth-required CLOSED after the relay accepted our AUTH leaves the connection authenticated", () => {
  const { state } = closeWithAuthRequired(stubRelayState({ authed: true }), hangingHandler())
  assertEquals(state.authed, true)
})

Deno.test("handleRelayMessage - does not re-issue an auth-parked subscription before the relay accepts the AUTH event", async () => {
  const { sent, signed } = await subscriptionParkedWhileAuthIsSent()
  assertEquals(sent, [JSON.stringify(["AUTH", signed])])
})

Deno.test("handleRelayMessage - re-issues an auth-parked subscription once the relay accepts the AUTH event", async () => {
  const { sent, sub } = acceptAuth(await subscriptionParkedWhileAuthIsSent())
  assertEquals(sent.slice(1), [JSON.stringify(["REQ", SUB_1, ...sub.filters])])
})

Deno.test("handleRelayMessage - returns an auth-parked subscription to the live ones once the relay accepts the AUTH event", async () => {
  const { state, sub } = acceptAuth(await subscriptionParkedWhileAuthIsSent())
  assertEquals(state.subs.get(SUB_1), sub)
})

Deno.test("handleRelayMessage - empties the parked subscriptions once the relay accepts the AUTH event", async () => {
  const { state } = acceptAuth(await subscriptionParkedWhileAuthIsSent())
  assertEquals(state.pendingAuthSubs.size, 0)
})
