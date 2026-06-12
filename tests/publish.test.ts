import { assertEquals } from "@std/assert"
import type { RelayUrl } from "@innis/nostr-core"
import { parseRelayUrl } from "@innis/nostr-core"
import { buildEventFixture } from "@innis/nostr-core/testing"
import type { Scheduler } from "../src/application/port/scheduler.ts"
import { createPublish } from "../src/infrastructure/web-socket/publish.ts"
import { handleRelayMessage } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import { createRelayState, type RelayState } from "../src/infrastructure/web-socket/relay-state.ts"
import type { PublishHistoryRecord } from "../src/application/service/relay-history.ts"
import { createManualTime, type ManualTime } from "./_helpers/scheduler.ts"

const URL = parseRelayUrl("wss://relay.example.com")

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

Deno.test("publish - times out (does not hang) when a relay demands auth and no handler answers", async () => {
  const state = openState()
  const time = createManualTime()
  const publish = publishWith(state, time, 200)
  const event = buildEventFixture()
  const pending = publish(URL, event)
  handleRelayMessage(messageContext(state, time.scheduler), okMessage(event.id, false, "auth-required: please AUTH"))
  time.tick(200)
  const result = await pending
  assertEquals(result, { from: URL, ok: false, message: "timeout" })
})
