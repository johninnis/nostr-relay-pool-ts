import { assertEquals } from "@std/assert"
import { parseEventId, parseRelayUrl } from "@innis/nostr-core"
import type { PublishHistoryRecord, SubHistoryRecord } from "../src/application/service/relay-history.ts"
import type { BackoffTracker } from "../src/application/service/backoff-tracker.ts"
import {
  buildPoolState,
  buildRelaySubscriptions,
  computeRelayStatus,
} from "../src/infrastructure/web-socket/pool-state-projection.ts"
import { stubRelayState, stubWireSub } from "./_helpers/relay-state.ts"

const historyEntry = (overrides: Partial<SubHistoryRecord> & { subId: string }): SubHistoryRecord => ({
  filters: [{ kinds: [1] }],
  openedAt: 1000,
  closedAt: null,
  eventCount: 0,
  ...overrides,
})

const url = parseRelayUrl("wss://relay.example.com")

// deno-lint-ignore innis/no-type-assertions -- minimal WebSocket stand-in; only readyState is read.
const socketInState = (readyState: number): WebSocket => ({ readyState } as unknown as WebSocket)

Deno.test("buildRelaySubscriptions - subs in state.subs render as active", () => {
  const state = stubRelayState({ subs: new Map([["s1", stubWireSub()]]) })
  const subs = buildRelaySubscriptions(url, {
    connections: new Map([[url, state]]),
    subHistory: new Map([[url, new Map([["s1", historyEntry({ subId: "s1" })]])]]),
  })
  assertEquals(subs.length, 1)
  assertEquals(subs[0]?.status, "active")
})

Deno.test("buildRelaySubscriptions - subs in state.pendingSubs render as pending", () => {
  const state = stubRelayState({ pendingSubs: new Map([["s1", stubWireSub()]]) })
  const subs = buildRelaySubscriptions(url, {
    connections: new Map([[url, state]]),
    subHistory: new Map(),
  })
  assertEquals(subs.length, 1)
  assertEquals(subs[0]?.status, "pending")
})

Deno.test("buildRelaySubscriptions - subs in state.pendingAuthSubs render as pending", () => {
  const state = stubRelayState({ pendingAuthSubs: new Map([["s1", stubWireSub()]]) })
  const subs = buildRelaySubscriptions(url, {
    connections: new Map([[url, state]]),
    subHistory: new Map(),
  })
  assertEquals(subs.length, 1)
  assertEquals(subs[0]?.status, "pending")
})

Deno.test("buildRelaySubscriptions - disconnected relay with open history entries renders them as pending", () => {
  const history = new Map([["s1", historyEntry({ subId: "s1", closedAt: null })]])
  const subs = buildRelaySubscriptions(url, {
    connections: new Map(),
    subHistory: new Map([[url, history]]),
  })
  assertEquals(subs.length, 1)
  assertEquals(subs[0]?.status, "pending")
  assertEquals(subs[0]?.closedAt, null)
})

Deno.test("buildRelaySubscriptions - history entries with closedAt render as closed", () => {
  const history = new Map([["s1", historyEntry({ subId: "s1", closedAt: 2000 })]])
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
  const otherUrl = parseRelayUrl("wss://seen-only.example.com")
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
  const disabled = parseRelayUrl("wss://disabled.example.com")
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
  const u1 = parseRelayUrl("wss://low.example.com")
  const u2 = parseRelayUrl("wss://high.example.com")
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
      [url, [{ eventId: parseEventId("e".repeat(64)), kind: 1, publishedAt: 1000, result: "ok", message: "" }]],
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
    subs: new Map([["active1", stubWireSub()]]),
    pendingSubs: new Map([["pending1", stubWireSub()]]),
  })
  const history = new Map([
    ["active1", historyEntry({ subId: "active1" })],
    ["pending1", historyEntry({ subId: "pending1" })],
    ["orphan", historyEntry({ subId: "orphan", closedAt: null })],
    ["closed", historyEntry({ subId: "closed", closedAt: 2000 })],
  ])
  const subs = buildRelaySubscriptions(url, {
    connections: new Map([[url, state]]),
    subHistory: new Map([[url, history]]),
  })
  const byStatus = new Map(subs.map((s) => [s.subId, s.status]))
  assertEquals(byStatus.get("active1"), "active")
  assertEquals(byStatus.get("pending1"), "pending")
  assertEquals(byStatus.get("orphan"), "pending")
  assertEquals(byStatus.get("closed"), "closed")
})
