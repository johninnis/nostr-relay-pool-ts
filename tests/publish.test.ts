import { assertEquals } from "@std/assert"
import { type NostrEvent, type RelayUrl, serialiseEventMessage } from "@innis/nostr-core"
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

// The publish path registers its OK resolver before it touches the socket, so these unit tests
// drive settlement purely through handleRelayMessage — no live socket on `state` is required.
const openState = (): RelayState => createRelayState()

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

Deno.test("publish - resolves with the relay OK ack", async () => {
  const state = openState()
  const time = createManualTime()
  const publish = publishWith(state, time, 1_000)
  const event = buildEventFixture()
  const pending = publish(URL, event)
  handleRelayMessage(messageContext(state, time.scheduler), okMessage(event.id, true, "accepted"))
  const result = await pending
  assertEquals(result, { from: URL, ok: true, message: "accepted" })
  assertEquals(time.pendingCount(), 0, "the OK ack should have cleared the publish timeout")
})

Deno.test("publish - a duplicate in-flight publish to the same relay shares one outcome", async () => {
  const state = openState()
  const time = createManualTime()
  const publish = publishWith(state, time, 1_000)
  const event = buildEventFixture()
  const first = publish(URL, event)
  const second = publish(URL, event)
  // Both calls join a single in-flight record and a single timeout — no second timer to orphan.
  assertEquals(state.inFlightPublishes.size, 1)
  assertEquals(time.pendingCount(), 1)
  handleRelayMessage(messageContext(state, time.scheduler), okMessage(event.id, true, "accepted"))
  assertEquals(await first, { from: URL, ok: true, message: "accepted" })
  assertEquals(await second, { from: URL, ok: true, message: "accepted" })
  assertEquals(time.pendingCount(), 0, "the shared OK ack should have cleared the one publish timeout")
  assertEquals(state.inFlightPublishes.size, 0)
})

Deno.test("publish - resolves with the relay's auth-required refusal when no auth handler can answer", async () => {
  const state = openState()
  const time = createManualTime()
  const publish = publishWith(state, time, 200)
  const event = buildEventFixture()
  const pending = publish(URL, event)
  handleRelayMessage(messageContext(state, time.scheduler), okMessage(event.id, false, "auth-required: please AUTH"))
  assertEquals(await pending, { from: URL, ok: false, message: "auth-required: please AUTH" })
  assertEquals(time.pendingCount(), 0)
})

Deno.test("publish - a publish parked for AUTH is settled by the auth timeout, not the publish timeout", async () => {
  const state = openState()
  const time = createManualTime()
  const publish = publishWith(state, time, 200)
  const event = buildEventFixture()
  const pending = publish(URL, event)
  handleRelayMessage(
    { ...messageContext(state, time.scheduler), authHandler: () => () => new Promise<NostrEvent | null>(() => {}) },
    okMessage(event.id, false, "auth-required: please AUTH"),
  )
  assertEquals([...state.pendingAuthPublish], [event.id])
  time.tick(59_999)
  assertEquals([...state.pendingAuthPublish], [event.id], "the publish timeout must not settle parked work")
  time.tick(1)
  assertEquals(await pending, { from: URL, ok: false, message: "auth-required: auth timed out" })
  assertEquals(state.pendingAuthPublish.size, 0)
  assertEquals(time.pendingCount(), 0)
})

Deno.test("publish - a publish resent after AUTH is bounded by the publish timeout again", async () => {
  const sent: string[] = []
  const ws = openSocket(sent)
  const state = createRelayState()
  state.ws = ws
  const time = createManualTime()
  const publish = publishWith(state, time, 200)
  const event = buildEventFixture()
  const authEvent = buildEventFixture({ kind: 22242, content: "auth" })
  const ctx = { ...messageContext(state, time.scheduler), authHandler: () => () => Promise.resolve(authEvent) }
  const pending = publish(URL, event)
  handleRelayMessage(ctx, new MessageEvent("message", { data: JSON.stringify(["AUTH", "challenge-1"]) }))
  handleRelayMessage(ctx, okMessage(event.id, false, "auth-required: please AUTH"))
  await new Promise((resolve) => setTimeout(resolve, 0))
  handleRelayMessage(ctx, okMessage(authEvent.id, true, ""))
  assertEquals(sent.at(-1), serialiseEventMessage(event))
  time.tick(200)
  assertEquals(await pending, { from: URL, ok: false, message: "timeout" })
  assertEquals(time.pendingCount(), 0)
})

Deno.test("publish - a relay's refusal resolves as a non-accepting outcome carrying its message", async () => {
  const state = openState()
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
    const result = await pool.publish(relay.url, buildEventFixture())
    assertEquals([result.ok, result.message], [false, "failed to connect"])
  } finally {
    pool.dispose()
    await relay.stop()
  }
})
