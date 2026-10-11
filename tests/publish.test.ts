import { assertEquals } from "@std/assert"
import { type NostrEvent, type RelayUrl, serialiseEventMessage } from "@innis/nostr-core"
import type { PublishResponse } from "../src/domain/value-object/publish-history.ts"
import { buildEventFixture, relayUrlFixture } from "@innis/nostr-core/testing"
import type { Scheduler } from "../src/application/port/scheduler.ts"
import { createPublish } from "../src/infrastructure/web-socket/publish.ts"
import { handleRelayMessage } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import { createRelayState, type RelayState } from "../src/infrastructure/web-socket/relay-state.ts"
import type { PublishHistoryRecord } from "../src/application/service/relay-history.ts"
import { createManualTime, type ManualTime } from "./_helpers/scheduler.ts"
import { openSocket } from "./_helpers/message.ts"
import { createRelayPool } from "../src/infrastructure/web-socket/web-socket-relay-pool.ts"
import { createInMemoryRelay } from "../testing.ts"

const URL = relayUrlFixture("wss://relay.example.com")

const socketlessState = (): RelayState => createRelayState()

const okMessage = (eventId: string, ok: boolean, message: string): MessageEvent =>
  new MessageEvent("message", { data: JSON.stringify(["OK", eventId, ok, message]) })

const messageContext = (state: RelayState, scheduler: Scheduler) => ({
  state,
  url: URL,
  subHistory: new Map(),
  authHandler: (): null => null,
  authTimeoutMs: 60_000,
  maxMessageBytes: 262_144,
  clock: () => 0,
  scheduler,
  onEventReceived: (): void => {},
  onEoseLatency: (): void => {},
  onStateChange: (): void => {},
})

const publishWith = (state: RelayState, time: ManualTime, publishTimeoutMs: number) =>
  createPublish({
    clock: time.clock,
    scheduler: time.scheduler,
    publishTimeoutMs,
    publishHistory: new Map<RelayUrl, Array<PublishHistoryRecord>>(),
    invalidateCache: (): void => {},
    onPublishInitiated: (): void => {},
    getOrCreateConnection: (): RelayState => state,
    releaseIfIdle: (): void => {},
  })

const ACCEPTED: PublishResponse = { from: URL, ok: true, message: "accepted" }

interface PublishInFlight {
  readonly state: RelayState
  readonly time: ManualTime
  readonly pending: Promise<PublishResponse>
}

const ackedPublish = async (): Promise<{ readonly time: ManualTime; readonly result: PublishResponse }> => {
  const state = socketlessState()
  const time = createManualTime()
  const event = buildEventFixture()
  const pending = publishWith(state, time, 1_000)(URL, event)
  handleRelayMessage(messageContext(state, time.scheduler), okMessage(event.id, true, "accepted"))
  return { time, result: await pending }
}

interface DuplicatePublish {
  readonly state: RelayState
  readonly time: ManualTime
  readonly outcomes: Promise<ReadonlyArray<PublishResponse>>
  readonly ack: () => void
}

const publishedTwiceBeforeAck = (): DuplicatePublish => {
  const state = socketlessState()
  const time = createManualTime()
  const publish = publishWith(state, time, 1_000)
  const event = buildEventFixture()
  const outcomes = Promise.all([publish(URL, event), publish(URL, event)])
  const ack = (): void =>
    handleRelayMessage(messageContext(state, time.scheduler), okMessage(event.id, true, "accepted"))
  return { state, time, outcomes, ack }
}

const refusedForAuthWithNoHandler = (): PublishInFlight => {
  const state = socketlessState()
  const time = createManualTime()
  const event = buildEventFixture()
  const pending = publishWith(state, time, 200)(URL, event)
  handleRelayMessage(messageContext(state, time.scheduler), okMessage(event.id, false, "auth-required: please AUTH"))
  return { state, time, pending }
}

const parkedForAuth = (): PublishInFlight & { readonly event: NostrEvent } => {
  const state = socketlessState()
  const time = createManualTime()
  const event = buildEventFixture()
  const pending = publishWith(state, time, 200)(URL, event)
  handleRelayMessage(
    { ...messageContext(state, time.scheduler), authHandler: () => () => new Promise<NostrEvent | null>(() => {}) },
    okMessage(event.id, false, "auth-required: please AUTH"),
  )
  return { state, time, pending, event }
}

const parkedPastAuthTimeout = (): PublishInFlight => {
  const parked = parkedForAuth()
  parked.time.tick(60_000)
  return parked
}

interface ResentAfterAuth {
  readonly sent: ReadonlyArray<string>
  readonly event: NostrEvent
  readonly time: ManualTime
  readonly pending: Promise<PublishResponse>
}

const resentAfterAuth = async (): Promise<ResentAfterAuth> => {
  const sent: string[] = []
  const state = createRelayState()
  state.ws = openSocket(sent)
  const time = createManualTime()
  const event = buildEventFixture()
  const authEvent = buildEventFixture({ kind: 22242, content: "auth" })
  const ctx = { ...messageContext(state, time.scheduler), authHandler: () => () => Promise.resolve(authEvent) }
  const pending = publishWith(state, time, 200)(URL, event)
  handleRelayMessage(ctx, new MessageEvent("message", { data: JSON.stringify(["AUTH", "challenge-1"]) }))
  handleRelayMessage(ctx, okMessage(event.id, false, "auth-required: please AUTH"))
  await new Promise((resolve) => setTimeout(resolve, 0))
  handleRelayMessage(ctx, okMessage(authEvent.id, true, ""))
  return { sent, event, time, pending }
}

const resentPastPublishTimeout = async (): Promise<ResentAfterAuth> => {
  const resent = await resentAfterAuth()
  resent.time.tick(200)
  return resent
}

Deno.test("publish - resolves with the relay OK ack", async () => {
  assertEquals((await ackedPublish()).result, ACCEPTED)
})

Deno.test("publish - the OK ack clears the publish timeout", async () => {
  assertEquals((await ackedPublish()).time.pendingCount(), 0)
})

Deno.test("publish - a duplicate in-flight publish to the same relay joins the one in-flight record", () => {
  assertEquals(publishedTwiceBeforeAck().state.inFlightPublishes.size, 1)
})

Deno.test("publish - a duplicate in-flight publish to the same relay arms no second timeout", () => {
  assertEquals(publishedTwiceBeforeAck().time.pendingCount(), 1)
})

Deno.test("publish - a duplicate in-flight publish to the same relay shares one outcome", async () => {
  const duplicate = publishedTwiceBeforeAck()
  duplicate.ack()
  assertEquals(await duplicate.outcomes, [ACCEPTED, ACCEPTED])
})

Deno.test("publish - the shared OK ack clears the one publish timeout", () => {
  const duplicate = publishedTwiceBeforeAck()
  duplicate.ack()
  assertEquals(duplicate.time.pendingCount(), 0)
})

Deno.test("publish - the shared OK ack drops the in-flight record", () => {
  const duplicate = publishedTwiceBeforeAck()
  duplicate.ack()
  assertEquals(duplicate.state.inFlightPublishes.size, 0)
})

Deno.test("publish - resolves with the relay's auth-required refusal when no auth handler can answer", async () => {
  assertEquals(await refusedForAuthWithNoHandler().pending, {
    from: URL,
    ok: false,
    message: "auth-required: please AUTH",
  })
})

Deno.test("publish - an auth-required refusal with no auth handler leaves no timeout pending", () => {
  assertEquals(refusedForAuthWithNoHandler().time.pendingCount(), 0)
})

Deno.test("publish - an auth-required refusal parks the publish while an auth handler can answer", () => {
  const parked = parkedForAuth()
  assertEquals(parked.state.pendingAuthPublish, new Set([parked.event.id]))
})

Deno.test("publish - the publish timeout does not settle a publish parked for AUTH", () => {
  const parked = parkedForAuth()
  parked.time.tick(59_999)
  assertEquals(parked.state.pendingAuthPublish, new Set([parked.event.id]))
})

Deno.test("publish - a publish parked for AUTH is settled by the auth timeout, not the publish timeout", async () => {
  assertEquals(await parkedPastAuthTimeout().pending, {
    from: URL,
    ok: false,
    message: "auth-required: auth timed out",
  })
})

Deno.test("publish - the auth timeout clears the parked publish", () => {
  assertEquals(parkedPastAuthTimeout().state.pendingAuthPublish.size, 0)
})

Deno.test("publish - the auth timeout leaves no timer pending", () => {
  assertEquals(parkedPastAuthTimeout().time.pendingCount(), 0)
})

Deno.test("publish - a publish parked for AUTH is resent once the relay accepts the AUTH", async () => {
  const resent = await resentAfterAuth()
  assertEquals(resent.sent.at(-1), serialiseEventMessage(resent.event))
})

Deno.test("publish - a publish resent after AUTH is bounded by the publish timeout again", async () => {
  assertEquals(await (await resentPastPublishTimeout()).pending, { from: URL, ok: false, message: "timeout" })
})

Deno.test("publish - the publish timeout of a resent publish leaves no timer pending", async () => {
  assertEquals((await resentPastPublishTimeout()).time.pendingCount(), 0)
})

Deno.test("publish - a relay's refusal resolves as a non-accepting outcome carrying its message", async () => {
  const state = socketlessState()
  const time = createManualTime()
  const publish = publishWith(state, time, 1_000)
  const event = buildEventFixture()
  const pending = publish(URL, event)
  handleRelayMessage(messageContext(state, time.scheduler), okMessage(event.id, false, "blocked: not on the list"))
  assertEquals(await pending, { from: URL, ok: false, message: "blocked: not on the list" })
})

Deno.test("publish - resolves, never rejects, when no connection can be opened", async () => {
  const time = createManualTime()
  const publish = createPublish({
    clock: time.clock,
    scheduler: time.scheduler,
    publishTimeoutMs: 1_000,
    publishHistory: new Map<RelayUrl, Array<PublishHistoryRecord>>(),
    invalidateCache: (): void => {},
    onPublishInitiated: (): void => {},
    getOrCreateConnection: (): null => null,
    releaseIfIdle: (): void => {},
  })
  assertEquals(await publish(URL, buildEventFixture()), { from: URL, ok: false, message: "failed to connect" })
})

Deno.test("publish resolves, never rejects, for a relay URL that does not parse", async () => {
  const pool = createRelayPool()
  try {
    assertEquals(await pool.publish("not a relay", buildEventFixture()), {
      from: null,
      ok: false,
      message: "invalid url",
    })
  } finally {
    pool.dispose()
  }
})

Deno.test("publish resolves, never rejects, for a relay the connection gate refuses", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    pool.setConnectionGate(() => false)
    assertEquals(await pool.publish(relay.url, buildEventFixture()), {
      from: relay.url,
      ok: false,
      message: "failed to connect",
    })
  } finally {
    pool.dispose()
    await relay.stop()
  }
})
