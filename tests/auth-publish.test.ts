import { assertEquals } from "@std/assert"
import type { NostrEvent } from "@innis/nostr-core"
import type { MessageContext } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import { handleRelayMessage } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import type { PublishAck, RelayState } from "../src/infrastructure/web-socket/relay-state.ts"
import { stubInFlightPublish, stubRelayState } from "./_helpers/relay-state.ts"
import { authEvent, context, event, flushMicrotasks, hangingHandler, message, openSocket } from "./_helpers/message.ts"

const AUTH_REQUIRED = "auth-required: please AUTH"

interface RefusedPublish {
  readonly state: RelayState
  readonly settled: PublishAck | null
  readonly pending: NostrEvent
}

const refusePublish = (state: RelayState, authHandler: MessageContext["authHandler"]): RefusedPublish => {
  let settled: PublishAck | null = null
  const pending = event()
  state.inFlightPublishes.set(pending.id, stubInFlightPublish(pending, { settle: (ack) => (settled = ack) }))
  handleRelayMessage(context(state, { authHandler }), message(["OK", pending.id, false, AUTH_REQUIRED]))
  return { state, settled, pending }
}

interface ParkedPublish {
  readonly sent: ReadonlyArray<string>
  readonly state: RelayState
  readonly ctx: MessageContext
  readonly pending: NostrEvent
  readonly signed: NostrEvent
}

const publishParkedWhileAuthIsSent = async (): Promise<ParkedPublish> => {
  const sent: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  const pending = event()
  const signed = authEvent("d")
  state.inFlightPublishes.set(pending.id, stubInFlightPublish(pending))
  const ctx = context(state, { authHandler: () => () => Promise.resolve(signed) })
  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  handleRelayMessage(ctx, message(["OK", pending.id, false, AUTH_REQUIRED]))
  await flushMicrotasks()
  return { sent, state, ctx, pending, signed }
}

const acceptAuth = (parked: ParkedPublish): ParkedPublish => {
  handleRelayMessage(parked.ctx, message(["OK", parked.signed.id, true, ""]))
  return parked
}

interface ParkedAcrossChallenges extends ParkedPublish {
  readonly challenges: ReadonlyArray<string>
}

const publishParkedAcrossAFreshChallenge = async (): Promise<ParkedAcrossChallenges> => {
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
  handleRelayMessage(ctx, message(["OK", pending.id, false, AUTH_REQUIRED]))
  await flushMicrotasks()
  handleRelayMessage(ctx, message(["AUTH", "challenge-2"]))
  handleRelayMessage(ctx, message(["OK", first.id, false, "auth-required: challenge issued, please retry"]))
  await flushMicrotasks()
  return { sent, challenges, state, ctx, pending, signed: second }
}

Deno.test("handleRelayMessage - parks an auth-required publish when an auth handler can answer", () => {
  const { state, pending } = refusePublish(stubRelayState(), hangingHandler())
  assertEquals(Array.from(state.pendingAuthPublish), [pending.id])
})

Deno.test("handleRelayMessage - keeps a publish parked for AUTH in flight", () => {
  const { state } = refusePublish(stubRelayState(), hangingHandler())
  assertEquals(state.inFlightPublishes.size, 1)
})

Deno.test("handleRelayMessage - returns an auth-required refusal at once when no auth handler is configured", () => {
  const { settled } = refusePublish(stubRelayState(), () => null)
  assertEquals(settled, { ok: false, message: AUTH_REQUIRED })
})

Deno.test("handleRelayMessage - does not park an auth-required publish when no auth handler is configured", () => {
  const { state } = refusePublish(stubRelayState(), () => null)
  assertEquals(state.pendingAuthPublish.size, 0)
})

Deno.test("handleRelayMessage - returns an auth-required refusal at once when the relay already accepted our AUTH", () => {
  const { settled } = refusePublish(stubRelayState({ authed: true }), () => () => Promise.resolve(authEvent("d")))
  assertEquals(settled, { ok: false, message: AUTH_REQUIRED })
})

Deno.test("handleRelayMessage - does not resend a parked publish before the relay accepts the AUTH event", async () => {
  const { sent, signed } = await publishParkedWhileAuthIsSent()
  assertEquals(sent, [JSON.stringify(["AUTH", signed])])
})

Deno.test("handleRelayMessage - resends a parked publish once the relay accepts the AUTH event", async () => {
  const { sent, pending } = acceptAuth(await publishParkedWhileAuthIsSent())
  assertEquals(sent.slice(1), [JSON.stringify(["EVENT", pending])])
})

Deno.test("handleRelayMessage - empties the parked publishes once the relay accepts the AUTH event", async () => {
  const { state } = acceptAuth(await publishParkedWhileAuthIsSent())
  assertEquals(state.pendingAuthPublish.size, 0)
})

Deno.test("handleRelayMessage - an auth-required OK for the AUTH event answers the fresh challenge", async () => {
  const { challenges } = await publishParkedAcrossAFreshChallenge()
  assertEquals(challenges, ["challenge-1", "challenge-2"])
})

Deno.test("handleRelayMessage - an auth-required OK for the AUTH event keeps publishes parked", async () => {
  const { state, pending } = await publishParkedAcrossAFreshChallenge()
  assertEquals(Array.from(state.pendingAuthPublish), [pending.id])
})

Deno.test("handleRelayMessage - a publish parked across a fresh challenge is resent once the relay accepts the next AUTH event", async () => {
  const { sent, pending } = acceptAuth(await publishParkedAcrossAFreshChallenge())
  assertEquals(sent.at(-1), JSON.stringify(["EVENT", pending]))
})
