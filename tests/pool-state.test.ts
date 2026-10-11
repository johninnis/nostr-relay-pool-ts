import { assertEquals } from "@std/assert"
import { eventIdFixture, relayUrlFixture, subscriptionIdFixture } from "@innis/nostr-core/testing"
import type { SubscriptionId } from "@innis/nostr-core"
import type { PublishHistoryRecord, SubHistoryRecord } from "../src/application/service/relay-history.ts"
import type { BackoffTracker } from "../src/application/service/backoff-tracker.ts"
import {
  buildPoolState,
  buildRelaySubscriptions,
  computeRelayStatus,
  type PoolSnapshot,
} from "../src/infrastructure/web-socket/pool-state-projection.ts"
import type { RelayState } from "../src/infrastructure/web-socket/relay-state.ts"
import type { RelayPoolStateEntry } from "../src/domain/value-object/relay-pool-state-entry.ts"
import type { RelaySubscriptionEntry } from "../src/domain/value-object/relay-subscription.ts"
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

const subscriptionsOfState = (state: RelayState): ReadonlyArray<RelaySubscriptionEntry> =>
  buildRelaySubscriptions(url, { connections: new Map([[url, state]]), subHistory: new Map() })

const subscriptionsOfHistoryAlone = (entry: SubHistoryRecord): ReadonlyArray<RelaySubscriptionEntry> =>
  buildRelaySubscriptions(url, {
    connections: new Map(),
    subHistory: new Map([[url, new Map([[entry.subId, entry]])]]),
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

const poolStateOf = (snapshot: Partial<PoolSnapshot>): ReadonlyArray<RelayPoolStateEntry> =>
  buildPoolState({
    connections: new Map(),
    attemptedRelays: new Set(),
    subHistory: new Map(),
    publishHistory: new Map(),
    relayEventCounts: new Map(),
    relayPublishCounts: new Map(),
    reconnectingUrls: new Set(),
    backoff: stubBackoff(),
    ...snapshot,
  })

const reconnectingWhileDisabled = (): ReadonlyArray<RelayPoolStateEntry> =>
  poolStateOf({
    attemptedRelays: new Set([url]),
    reconnectingUrls: new Set([url]),
    backoff: stubBackoff({
      isDisabled: () => true,
      disabledUntil: () => 99999,
      getDisabledUrls: () => [url],
    }),
  })

const seenOnly = relayUrlFixture("wss://seen-only.example.com")

const attemptedOnly = (): ReadonlyArray<RelayPoolStateEntry> =>
  poolStateOf({ attemptedRelays: new Set([seenOnly]), relayEventCounts: new Map([[seenOnly, 5]]) })

const disabled = relayUrlFixture("wss://disabled.example.com")

const backoffDisabledOnly = (): ReadonlyArray<RelayPoolStateEntry> =>
  poolStateOf({
    backoff: stubBackoff({
      isDisabled: (u) => u === disabled,
      disabledUntil: (u) => u === disabled ? 99999 : null,
      getDisabledUrls: () => [disabled],
    }),
  })

const fewerEvents = relayUrlFixture("wss://low.example.com")
const moreEvents = relayUrlFixture("wss://high.example.com")

const attemptedInOrderWithRisingEventCounts = (): ReadonlyArray<RelayPoolStateEntry> =>
  poolStateOf({
    attemptedRelays: new Set([fewerEvents, moreEvents]),
    relayEventCounts: new Map([[fewerEvents, 1], [moreEvents, 100]]),
  })

Deno.test("buildRelaySubscriptions - subs in state.subs render as active", () => {
  const state = stubRelayState({ subs: new Map([[SUB_1_ID, stubWireSub()]]) })
  const subs = buildRelaySubscriptions(url, {
    connections: new Map([[url, state]]),
    subHistory: new Map([[url, new Map([[SUB_1_ID, historyEntry({ subId: SUB_1_ID })]])]]),
  })
  assertEquals(subs.map((sub) => sub.status), ["active"])
})

Deno.test("buildRelaySubscriptions - subs in state.pendingSubs render as pending", () => {
  const subs = subscriptionsOfState(stubRelayState({ pendingSubs: new Map([[SUB_1_ID, stubWireSub()]]) }))
  assertEquals(subs.map((sub) => sub.status), ["pending"])
})

Deno.test("buildRelaySubscriptions - subs in state.pendingAuthSubs render as pending", () => {
  const subs = subscriptionsOfState(stubRelayState({ pendingAuthSubs: new Map([[SUB_1_ID, stubWireSub()]]) }))
  assertEquals(subs.map((sub) => sub.status), ["pending"])
})

Deno.test("buildRelaySubscriptions - disconnected relay with open history entries renders them as pending", () => {
  const subs = subscriptionsOfHistoryAlone(historyEntry({ subId: SUB_1_ID, closedAt: null }))
  assertEquals(subs.map((sub) => sub.status), ["pending"])
})

Deno.test("buildRelaySubscriptions - disconnected relay with open history entries renders them with no closedAt", () => {
  const subs = subscriptionsOfHistoryAlone(historyEntry({ subId: SUB_1_ID, closedAt: null }))
  assertEquals(subs.map((sub) => sub.closedAt), [null])
})

Deno.test("buildRelaySubscriptions - history entries with closedAt render as closed", () => {
  const subs = subscriptionsOfHistoryAlone(historyEntry({ subId: SUB_1_ID, closedAt: 2000 }))
  assertEquals(subs.map((sub) => sub.status), ["closed"])
})

Deno.test("buildRelaySubscriptions - history entries with closedAt carry their closedAt", () => {
  const subs = subscriptionsOfHistoryAlone(historyEntry({ subId: SUB_1_ID, closedAt: 2000 }))
  assertEquals(subs.map((sub) => sub.closedAt), [2000])
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
  assertEquals(reconnectingWhileDisabled()[0]?.status, "reconnecting")
})

Deno.test("buildPoolState - keeps the disabledUntil of a url reported as reconnecting", () => {
  assertEquals(reconnectingWhileDisabled()[0]?.disabledUntil, 99999)
})

Deno.test("buildPoolState - emits an entry for every relay in connections", () => {
  const entries = poolStateOf({ connections: new Map([[url, stubRelayState()]]) })
  assertEquals(entries.map((entry) => entry.url), [url])
})

Deno.test("buildPoolState - emits an entry for a relay only in attemptedRelays", () => {
  assertEquals(attemptedOnly().map((entry) => entry.url), [seenOnly])
})

Deno.test("buildPoolState - reports a relay only in attemptedRelays as disconnected", () => {
  assertEquals(attemptedOnly()[0]?.status, "disconnected")
})

Deno.test("buildPoolState - carries the event count of a relay only in attemptedRelays", () => {
  assertEquals(attemptedOnly()[0]?.eventCount, 5)
})

Deno.test("buildPoolState - emits an entry for a backoff-disabled URL not in connections or seen", () => {
  assertEquals(backoffDisabledOnly().map((entry) => entry.url), [disabled])
})

Deno.test("buildPoolState - reports a backoff-disabled URL not in connections or seen as disabled", () => {
  assertEquals(backoffDisabledOnly()[0]?.status, "disabled")
})

Deno.test("buildPoolState - preserves insertion order and does not rank by event count", () => {
  assertEquals(attemptedInOrderWithRisingEventCounts().map((entry) => entry.url), [fewerEvents, moreEvents])
})

Deno.test("buildPoolState - carries each relay's own event count", () => {
  assertEquals(attemptedInOrderWithRisingEventCounts().map((entry) => entry.eventCount), [1, 100])
})

Deno.test("buildPoolState - publishCount is the lifetime tally, not the capped history length", () => {
  const entries = poolStateOf({
    connections: new Map([[url, stubRelayState()]]),
    publishHistory: new Map<typeof url, ReadonlyArray<PublishHistoryRecord>>([
      [url, [{ eventId: eventIdFixture("e".repeat(64)), kind: 1, publishedAt: 1000, result: "ok", message: "" }]],
    ]),
    relayPublishCounts: new Map([[url, 250]]),
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
  assertEquals(
    subs.map((sub) => [sub.subId, sub.status]),
    [[ACTIVE1, "active"], [PENDING1, "pending"], [ORPHAN, "pending"], [CLOSED, "closed"]],
  )
})
