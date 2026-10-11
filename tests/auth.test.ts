import { assertEquals } from "@std/assert"
import { subscriptionIdFixture } from "@innis/nostr-core/testing"
import type { NostrEvent } from "@innis/nostr-core"
import type { MessageContext } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import { handleRelayMessage } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import type { RelayState } from "../src/infrastructure/web-socket/relay-state.ts"
import { stubRelayState, stubWireSub } from "./_helpers/relay-state.ts"
import { createManualTime, type ManualTime } from "./_helpers/scheduler.ts"
import { authEvent, context, flushMicrotasks, message, openSocket, wireSubWith } from "./_helpers/message.ts"

const SUB_1 = subscriptionIdFixture("sub-1")

interface AnsweredChallenge {
  readonly sent: ReadonlyArray<string>
  readonly state: RelayState
  readonly ctx: MessageContext
  readonly signed: NostrEvent
}

const challengeAnswered = async (): Promise<AnsweredChallenge> => {
  const sent: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  const signed = authEvent("d")
  const ctx = context(state, { authHandler: () => () => Promise.resolve(signed) })
  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  await flushMicrotasks()
  return { sent, state, ctx, signed }
}

interface DeclinedChallenge {
  readonly sent: ReadonlyArray<string>
  readonly state: RelayState
  readonly ctx: MessageContext
  readonly time: ManualTime
}

const challengeDeclined = async (): Promise<DeclinedChallenge> => {
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
  return { sent, state, ctx, time }
}

Deno.test("handleRelayMessage - answers a challenge on arrival with the signed AUTH event", async () => {
  const { sent, signed } = await challengeAnswered()
  assertEquals(sent, [JSON.stringify(["AUTH", signed])])
})

Deno.test("handleRelayMessage - sending the AUTH event does not mark the connection authenticated", async () => {
  const { state } = await challengeAnswered()
  assertEquals(state.authed, false)
})

Deno.test("handleRelayMessage - the relay's OK for the AUTH event marks the connection authenticated", async () => {
  const { state, ctx, signed } = await challengeAnswered()
  handleRelayMessage(ctx, message(["OK", signed.id, true, ""]))
  assertEquals(state.authed, true)
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

Deno.test("handleRelayMessage - a declined challenge sends nothing", async () => {
  const { sent } = await challengeDeclined()
  assertEquals(sent, [])
})

Deno.test("handleRelayMessage - a declined challenge clears the auth timer", async () => {
  const { state } = await challengeDeclined()
  assertEquals(state.authTimer, null)
})

Deno.test("handleRelayMessage - a declined challenge leaves no timer pending", async () => {
  const { time } = await challengeDeclined()
  assertEquals(time.pendingCount(), 0)
})

Deno.test("handleRelayMessage - after a declined challenge an auth-required CLOSED is terminal", async () => {
  const reasons: string[] = []
  const { state, ctx } = await challengeDeclined()
  state.subs.set(SUB_1, wireSubWith([{ onEvent: () => {}, onClosed: (reason) => reasons.push(reason) }]))
  handleRelayMessage(ctx, message(["CLOSED", SUB_1, "auth-required: sign in"]))
  assertEquals(reasons, ["auth-required: sign in"])
})

Deno.test("handleRelayMessage - a new challenge clears an earlier decline", async () => {
  const { state, ctx } = await challengeDeclined()
  handleRelayMessage(ctx, message(["AUTH", "challenge-2"]))
  const declinedOnArrival = state.authDeclined
  await flushMicrotasks()
  assertEquals(declinedOnArrival, false)
})

interface RepeatedChallenge {
  readonly answeredWhileInFlight: ReadonlyArray<string>
  readonly answeredAfterAuthRequired: ReadonlyArray<string>
}

const challengeRepeatedWhileItsAnswerIsInFlight = async (): Promise<RepeatedChallenge> => {
  const challenges: string[] = []
  const state = stubRelayState({ ws: openSocket([]) })
  const first = authEvent("d")
  const ctx = context(state, {
    authHandler: () => (_url, challenge) => {
      challenges.push(challenge)
      return Promise.resolve(challenges.length === 1 ? first : authEvent("e"))
    },
  })
  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  await flushMicrotasks()
  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  const answeredWhileInFlight = [...challenges]
  handleRelayMessage(ctx, message(["OK", first.id, false, "auth-required: challenge issued, please retry"]))
  await flushMicrotasks()
  return { answeredWhileInFlight, answeredAfterAuthRequired: challenges }
}

Deno.test("handleRelayMessage - a challenge frame repeating the one whose answer is in flight waits for that answer", async () => {
  const { answeredWhileInFlight } = await challengeRepeatedWhileItsAnswerIsInFlight()
  assertEquals(answeredWhileInFlight, ["challenge-1"])
})

Deno.test("handleRelayMessage - a challenge frame repeating the one whose answer is in flight is answered when the relay answers that AUTH with auth-required", async () => {
  const { answeredAfterAuthRequired } = await challengeRepeatedWhileItsAnswerIsInFlight()
  assertEquals(answeredAfterAuthRequired, ["challenge-1", "challenge-1"])
})

const challengeArrivingWhileTheHandlerDeclinesTheLast = async (): Promise<RelayState> => {
  let decline: (signed: NostrEvent | null) => void = () => {}
  const state = stubRelayState({ ws: openSocket([]) })
  const ctx = context(state, {
    authHandler: () => () =>
      new Promise<NostrEvent | null>((resolve) => {
        decline = resolve
      }),
  })
  handleRelayMessage(ctx, message(["AUTH", "challenge-1"]))
  handleRelayMessage(ctx, message(["AUTH", "challenge-2"]))
  decline(null)
  await flushMicrotasks()
  state.subs.set(SUB_1, stubWireSub())
  handleRelayMessage(ctx, message(["CLOSED", SUB_1, "auth-required: sign in"]))
  return state
}

Deno.test("handleRelayMessage - a challenge arriving while the handler declines the last one is not declined", async () => {
  const state = await challengeArrivingWhileTheHandlerDeclinesTheLast()
  assertEquals(state.pendingAuthSubs.has(SUB_1), true)
})
