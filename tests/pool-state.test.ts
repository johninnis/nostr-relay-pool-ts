import { assertEquals } from "@std/assert"
import { eventIdFixture, relayUrlFixture, subscriptionIdFixture } from "@innis/nostr-core/testing"
import type { SubscriptionId } from "@innis/nostr-core"
import type { PublishHistoryRecord, SubHistoryRecord } from "../src/application/service/relay-history.ts"
import type { BackoffTracker } from "../src/application/service/backoff-tracker.ts"
import {
  buildPoolState,
  buildRelaySubscriptions,
  computeRelayStatus,
} from "../src/infrastructure/web-socket/pool-state-projection.ts"
import { stubRelayState, stubWireSub } from "./_helpers/relay-state.ts"
import { FakeWebSocket } from "./_helpers/fake-web-socket.ts"

const SUB_1_ID = subscriptionIdFixture("s1")
const ACTIVE1 = subscriptionIdFixture("active1")
const PENDING1 = subscriptionIdFixture("pending1")
const ORPHAN = subscriptionIdFixture("orphan")
const CLOSED = subscriptionIdFixture("closed")

const historyEntry = (overrides: Partial<SubHistoryRecord> & { subId: SubscriptionId }): SubHistoryRecord => ({
  filters: [{ kinds: [1] }],
  openedAt: 1000,
  closedAt: null,
  eventCount: 0,
  ...overrides,
})

const url = relayUrlFixture("wss://relay.example.com")

const socketInState = (readyState: number): WebSocket => new FakeWebSocket(readyState)

Deno.test("buildRelaySubscriptions - subs in state.subs render as active", () => {
  const state = stubRelayState({ subs: new Map([[SUB_1_ID, stubWireSub()]]) })
  const subs = buildRelaySubscriptions(url, {
    connections: new Map([[url, state]]),
    subHistory: new Map([[url, new Map([[SUB_1_ID, historyEntry({ subId: SUB_1_ID })]])]]),
  })
  assertEquals(subs.length, 1)
  assertEquals(subs[0]?.status, "active")
})

Deno.test("buildRelaySubscriptions - subs in state.pendingSubs render as pending", () => {
  const state = stubRelayState({ pendingSubs: new Map([[SUB_1_ID, stubWireSub()]]) })
  const subs = buildRelaySubscriptions(url, {
    connections: new Map([[url, state]]),
    subHistory: new Map(),
  })
  assertEquals(subs.length, 1)
  assertEquals(subs[0]?.status, "pending")
})

Deno.test("buildRelaySubscriptions - subs in state.pendingAuthSubs render as pending", () => {
  const state = stubRelayState({ pendingAuthSubs: new Map([[SUB_1_ID, stubWireSub()]]) })
  const subs = buildRelaySubscriptions(url, {
    connections: new Map([[url, state]]),
    subHistory: new Map(),
  })
  assertEquals(subs.length, 1)
  assertEquals(subs[0]?.status, "pending")
})

Deno.test("buildRelaySubscriptions - disconnected relay with open history entries renders them as pending", () => {
  const history = new Map([[SUB_1_ID, historyEntry({ subId: SUB_1_ID, closedAt: null })]])
  const subs = buildRelaySubscriptions(url, {
    connections: new Map(),
    subHistory: new Map([[url, history]]),
  })
  assertEquals(subs.length, 1)
  assertEquals(subs[0]?.status, "pending")
  assertEquals(subs[0]?.closedAt, null)
})

Deno.test("buildRelaySubscriptions - history entries with closedAt render as closed", () => {
  const history = new Map([[SUB_1_ID, historyEntry({ subId: SUB_1_ID, closedAt: 2000 })]])
  const subs = buildRelaySubscriptions(url, {
    connections: new Map(),
    subHistory: new Map([[url, history]]),
  })
  assertEquals(subs.length, 1)
  assertEquals(subs[0]?.status, "closed")
  assertEquals(subs[0]?.closedAt, 2000)
})

const stubBackoff = (overrides: Partial<BackoffTracker> = {}): BackoffTracker => ({
  isDisabled: () => false,
  disabledUntil: () => null,
  getLastFailure: () => null,
  getDisabledUrls: () => [],
  recordFailure: () => {},
  recordSuccess: () => {},
  clear: () => {},
  ...overrides,
})

Deno.test("computeRelayStatus - returns disconnected when there is no socket", () => {
  const state = stubRelayState()
  assertEquals(computeRelayStatus(state), "disconnected")
})

Deno.test("computeRelayStatus - reports connecting while the first socket is still opening", () => {
  const state = stubRelayState({ ws: socketInState(WebSocket.CONNECTING) })
  assertEquals(computeRelayStatus(state), "connecting")
})

Deno.test("computeRelayStatus - reports connected once the socket is open", () => {
  const state = stubRelayState({ ws: socketInState(WebSocket.OPEN) })
  assertEquals(computeRelayStatus(state), "connected")
})

Deno.test("buildPoolState - reports reconnecting for a url with a pending reconnect, ahead of disabled", () => {
  const entries = buildPoolState({
    connections: new Map(),
    attemptedRelays: new Set([url]),
    subHistory: new Map(),
    publishHistory: new Map(),
    relayEventCounts: new Map(),
    relayPublishCounts: new Map(),
    reconnectingUrls: new Set([url]),
    backoff: stubBackoff({
      isDisabled: () => true,
      disabledUntil: () => 99999,
      getDisabledUrls: () => [url],
    }),
  })
  assertEquals(entries[0]?.status, "reconnecting")
  assertEquals(entries[0]?.disabledUntil, 99999)
})

Deno.test("buildPoolState - emits a connected entry for every relay in connections", () => {
  const state = stubRelayState()
  const entries = buildPoolState({
    connections: new Map([[url, state]]),
    attemptedRelays: new Set(),
    subHistory: new Map(),
    publishHistory: new Map(),
    relayEventCounts: new Map(),
    relayPublishCounts: new Map(),
    reconnectingUrls: new Set(),
    backoff: stubBackoff(),
  })
  assertEquals(entries.length, 1)
  assertEquals(entries[0]?.url, url)
})

Deno.test("buildPoolState - emits a disconnected entry for relays only in attemptedRelays", () => {
  const otherUrl = relayUrlFixture("wss://seen-only.example.com")
  const entries = buildPoolState({
    connections: new Map(),
    attemptedRelays: new Set([otherUrl]),
    subHistory: new Map(),
    publishHistory: new Map(),
    relayEventCounts: new Map([[otherUrl, 5]]),
    relayPublishCounts: new Map(),
    reconnectingUrls: new Set(),
    backoff: stubBackoff(),
  })
  assertEquals(entries.length, 1)
  assertEquals(entries[0]?.url, otherUrl)
  assertEquals(entries[0]?.status, "disconnected")
  assertEquals(entries[0]?.eventCount, 5)
})

Deno.test("buildPoolState - emits a disabled disconnected entry for backoff-disabled URLs not in connections or seen", () => {
  const disabled = relayUrlFixture("wss://disabled.example.com")
  const entries = buildPoolState({
    connections: new Map(),
    attemptedRelays: new Set(),
    subHistory: new Map(),
    publishHistory: new Map(),
    relayEventCounts: new Map(),
    relayPublishCounts: new Map(),
    reconnectingUrls: new Set(),
    backoff: stubBackoff({
      isDisabled: (u) => u === disabled,
      disabledUntil: (u) => u === disabled ? 99999 : null,
      getDisabledUrls: () => [disabled],
    }),
  })
  assertEquals(entries.length, 1)
  assertEquals(entries[0]?.url, disabled)
  assertEquals(entries[0]?.status, "disabled")
})

Deno.test("buildPoolState - preserves insertion order and does not rank by event count", () => {
  const u1 = relayUrlFixture("wss://low.example.com")
  const u2 = relayUrlFixture("wss://high.example.com")
  const entries = buildPoolState({
    connections: new Map(),
    attemptedRelays: new Set([u1, u2]),
    subHistory: new Map(),
    publishHistory: new Map(),
    relayEventCounts: new Map([[u1, 1], [u2, 100]]),
    relayPublishCounts: new Map(),
    reconnectingUrls: new Set(),
    backoff: stubBackoff(),
  })
  // Stable, intent-free order: attempt order is honoured even though u2 has the higher event count.
  assertEquals(entries[0]?.url, u1)
  assertEquals(entries[1]?.url, u2)
  assertEquals(entries[0]?.eventCount, 1)
  assertEquals(entries[1]?.eventCount, 100)
})

Deno.test("buildPoolState - publishCount is the lifetime tally, not the capped history length", () => {
  const state = stubRelayState()
  const entries = buildPoolState({
    connections: new Map([[url, state]]),
    attemptedRelays: new Set(),
    subHistory: new Map(),
    // History is a capped ring (here a single retained entry); the count must come from the
    // monotonic tally instead, so a relay past the history limit still reports its true total.
    publishHistory: new Map<typeof url, ReadonlyArray<PublishHistoryRecord>>([
      [url, [{ eventId: eventIdFixture("e".repeat(64)), kind: 1, publishedAt: 1000, result: "ok", message: "" }]],
    ]),
    relayEventCounts: new Map(),
    relayPublishCounts: new Map([[url, 250]]),
    reconnectingUrls: new Set(),
    backoff: stubBackoff(),
  })
  assertEquals(entries[0]?.publishCount, 250)
})

Deno.test("buildRelaySubscriptions - active, pending, and closed can coexist for one relay", () => {
  const state = stubRelayState({
    subs: new Map([[ACTIVE1, stubWireSub()]]),
    pendingSubs: new Map([[PENDING1, stubWireSub()]]),
  })
  const history = new Map([
    [ACTIVE1, historyEntry({ subId: ACTIVE1 })],
    [PENDING1, historyEntry({ subId: PENDING1 })],
    [ORPHAN, historyEntry({ subId: ORPHAN, closedAt: null })],
    [CLOSED, historyEntry({ subId: CLOSED, closedAt: 2000 })],
  ])
  const subs = buildRelaySubscriptions(url, {
    connections: new Map([[url, state]]),
    subHistory: new Map([[url, history]]),
  })
  const byStatus = new Map(subs.map((s) => [s.subId, s.status]))
  assertEquals(byStatus.get(ACTIVE1), "active")
  assertEquals(byStatus.get(PENDING1), "pending")
  assertEquals(byStatus.get(ORPHAN), "pending")
  assertEquals(byStatus.get(CLOSED), "closed")
})
