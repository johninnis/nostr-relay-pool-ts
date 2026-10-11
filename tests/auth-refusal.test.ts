import { assertEquals } from "@std/assert"
import { subscriptionIdFixture } from "@innis/nostr-core/testing"
import { parseReasonPrefix } from "@innis/nostr-core"
import { handleRelayMessage } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import type { PublishAck, RelayState } from "../src/infrastructure/web-socket/relay-state.ts"
import { stubInFlightPublish, stubRelayState } from "./_helpers/relay-state.ts"
import { authEvent, context, event, flushMicrotasks, message, openSocket, wireSubWith } from "./_helpers/message.ts"

const SUB_1 = subscriptionIdFixture("sub-1")
const AUTH_REJECTED = "auth-required: auth rejected: restricted: key not allowed"
const AUTH_DECLINED = "auth-required: auth declined"

interface ReleasedWork {
  readonly state: RelayState
  readonly settled: PublishAck | null
  readonly closedReasons: ReadonlyArray<string>
}

const authRefusedForAnotherReason = async (): Promise<ReleasedWork> => {
  let settled: PublishAck | null = null
  const closedReasons: string[] = []
  const state = stubRelayState({ ws: openSocket([]) })
  const pending = event()
  const signed = authEvent("d")
  state.inFlightPublishes.set(pending.id, stubInFlightPublish(pending, { settle: (ack) => (settled = ack) }))
  state.pendingAuthPublish.add(pending.id)
  state.pendingAuthSubs.set(
    SUB_1,
    wireSubWith([{ onEvent: () => {}, onClosed: (reason) => closedReasons.push(reason) }]),
  )
  const ctx = context(state, { authHandler: () => () => Promise.resolve(signed) })
  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  await flushMicrotasks()
  handleRelayMessage(ctx, message(["OK", signed.id, false, "restricted: key not allowed"]))
  return { state, settled, closedReasons }
}

const challengeDeclinedWithWorkParked = async (): Promise<ReleasedWork> => {
  let settled: PublishAck | null = null
  const closedReasons: string[] = []
  const state = stubRelayState({ ws: openSocket([]) })
  const pending = event()
  state.inFlightPublishes.set(pending.id, stubInFlightPublish(pending, { settle: (ack) => (settled = ack) }))
  state.subs.set(SUB_1, wireSubWith([{ onEvent: () => {}, onClosed: (reason) => closedReasons.push(reason) }]))
  const ctx = context(state, { authHandler: () => () => Promise.resolve(null) })
  handleRelayMessage(ctx, message(["OK", pending.id, false, "auth-required: please AUTH"]))
  handleRelayMessage(ctx, message(["CLOSED", SUB_1, "auth-required: sign in"]))
  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  await flushMicrotasks()
  return { state, settled, closedReasons }
}

Deno.test("handleRelayMessage - an AUTH event refused for another reason resolves a parked publish as auth rejected with the relay's message", async () => {
  const { settled } = await authRefusedForAnotherReason()
  assertEquals(settled, { ok: false, message: AUTH_REJECTED })
})

Deno.test("handleRelayMessage - an AUTH event refused for another reason closes a parked subscription as auth rejected with the relay's message", async () => {
  const { closedReasons } = await authRefusedForAnotherReason()
  assertEquals(closedReasons, [AUTH_REJECTED])
})

Deno.test("handleRelayMessage - the reason an AUTH event refused for another reason gives parked work reads as auth-required", async () => {
  const { closedReasons } = await authRefusedForAnotherReason()
  assertEquals(parseReasonPrefix(closedReasons[0] ?? ""), "auth-required")
})

Deno.test("handleRelayMessage - an AUTH event refused for another reason empties the parked subscriptions", async () => {
  const { state } = await authRefusedForAnotherReason()
  assertEquals(state.pendingAuthSubs.size, 0)
})

Deno.test("handleRelayMessage - an AUTH event refused for another reason leaves the connection unauthenticated", async () => {
  const { state } = await authRefusedForAnotherReason()
  assertEquals(state.authed, false)
})

Deno.test("handleRelayMessage - a declined challenge resolves a parked publish as auth declined", async () => {
  const { settled } = await challengeDeclinedWithWorkParked()
  assertEquals(settled, { ok: false, message: AUTH_DECLINED })
})

Deno.test("handleRelayMessage - a declined challenge closes a parked subscription as auth declined", async () => {
  const { closedReasons } = await challengeDeclinedWithWorkParked()
  assertEquals(closedReasons, [AUTH_DECLINED])
})

Deno.test("handleRelayMessage - a declined challenge empties the parked publishes", async () => {
  const { state } = await challengeDeclinedWithWorkParked()
  assertEquals(state.pendingAuthPublish.size, 0)
})

Deno.test("handleRelayMessage - a declined challenge empties the parked subscriptions", async () => {
  const { state } = await challengeDeclinedWithWorkParked()
  assertEquals(state.pendingAuthSubs.size, 0)
})
