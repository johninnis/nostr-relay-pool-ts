import { assertEquals } from "@std/assert"
import type { RelayUrl, SubscriptionId } from "@innis/nostr-core"
import { eventIdFixture, relayUrlFixture, subscriptionIdFixture } from "@innis/nostr-core/testing"
import type { PublishHistoryRecord, SubHistoryRecord } from "../src/application/service/relay-history.ts"
import { systemWallClock } from "../src/infrastructure/time/system-wall-clock.ts"
import { systemScheduler } from "../src/infrastructure/time/system-scheduler.ts"
import {
  closeSubHistory,
  PUBLISH_HISTORY_LIMIT,
  recordPublishEntry,
  recordSubHistory,
  SUB_HISTORY_LIMIT,
} from "../src/application/service/relay-history.ts"
import { tearDownSubs } from "../src/infrastructure/web-socket/relay-state.ts"
import { stubRelayState, stubWireSub } from "./_helpers/relay-state.ts"

const SUB_1_ID = subscriptionIdFixture("s1")
const SUB_2_ID = subscriptionIdFixture("s2")
const SUB_1 = subscriptionIdFixture("sub-1")
const SUB_2 = subscriptionIdFixture("sub-2")

const URL = relayUrlFixture("wss://relay.example.com")

const publishEntry = (raw: string): PublishHistoryRecord => ({
  eventId: eventIdFixture(raw.padEnd(64, "0")),
  kind: 1,
  publishedAt: 1000,
  result: "pending",
  message: "",
})

Deno.test("recordSubHistory - records a new open subscription entry", () => {
  const subHistory = new Map<RelayUrl, Map<SubscriptionId, SubHistoryRecord>>()
  recordSubHistory({ subHistory, url: URL, subId: SUB_1, filters: [{ kinds: [1] }], openedAt: 42 })
  const entry = subHistory.get(URL)?.get(SUB_1)
  assertEquals(entry?.subId, SUB_1)
  assertEquals(entry?.openedAt, 42)
  assertEquals(entry?.closedAt, null)
  assertEquals(entry?.eventCount, 0)
})

Deno.test("recordSubHistory - keeps multiple entries for the same relay", () => {
  const subHistory = new Map<RelayUrl, Map<SubscriptionId, SubHistoryRecord>>()
  recordSubHistory({ subHistory, url: URL, subId: SUB_1, filters: [{ kinds: [1] }], openedAt: 1 })
  recordSubHistory({ subHistory, url: URL, subId: SUB_2, filters: [{ kinds: [0] }], openedAt: 2 })
  assertEquals(subHistory.get(URL)?.size, 2)
})

Deno.test("recordSubHistory - caps closed entries at SUB_HISTORY_LIMIT", () => {
  const subHistory = new Map<RelayUrl, Map<SubscriptionId, SubHistoryRecord>>()
  for (let i = 0; i < SUB_HISTORY_LIMIT + 50; i++) {
    const subId = subscriptionIdFixture(`sub-${i}`)
    recordSubHistory({ subHistory, url: URL, subId, filters: [{ kinds: [1] }], openedAt: i })
    closeSubHistory({ subHistory, url: URL, subId, clock: () => i })
  }
  assertEquals(subHistory.get(URL)?.size, SUB_HISTORY_LIMIT)
})

Deno.test("recordSubHistory - never evicts still-open subscriptions, even past the cap", () => {
  const subHistory = new Map<RelayUrl, Map<SubscriptionId, SubHistoryRecord>>()
  for (let i = 0; i < SUB_HISTORY_LIMIT + 10; i++) {
    recordSubHistory({
      subHistory,
      url: URL,
      subId: subscriptionIdFixture(`open-${i}`),
      filters: [{ kinds: [1] }],
      openedAt: i,
    })
  }
  assertEquals(subHistory.get(URL)?.size, SUB_HISTORY_LIMIT + 10)
})

Deno.test("closeSubHistory - stamps the closedAt time on an open entry", () => {
  const subHistory = new Map<RelayUrl, Map<SubscriptionId, SubHistoryRecord>>()
  recordSubHistory({ subHistory, url: URL, subId: SUB_1, filters: [{ kinds: [1] }], openedAt: 1 })
  closeSubHistory({ subHistory, url: URL, subId: SUB_1, clock: () => 99 })
  assertEquals(subHistory.get(URL)?.get(SUB_1)?.closedAt, 99)
})

Deno.test("closeSubHistory - is a no-op for an unknown subscription", () => {
  const subHistory = new Map<RelayUrl, Map<SubscriptionId, SubHistoryRecord>>()
  closeSubHistory({ subHistory, url: URL, subId: subscriptionIdFixture("missing"), clock: systemWallClock })
  assertEquals(subHistory.size, 0)
})

Deno.test("recordPublishEntry - appends an entry for the relay", () => {
  const publishHistory = new Map<RelayUrl, Array<PublishHistoryRecord>>()
  recordPublishEntry(publishHistory, URL, publishEntry("e1"))
  assertEquals(publishHistory.get(URL)?.length, 1)
})

Deno.test("recordPublishEntry - caps the history at the publish limit", () => {
  const publishHistory = new Map<RelayUrl, Array<PublishHistoryRecord>>()
  for (let i = 0; i < PUBLISH_HISTORY_LIMIT + 25; i++) {
    recordPublishEntry(publishHistory, URL, publishEntry(`e${i}`))
  }
  const entries = publishHistory.get(URL)
  assertEquals(entries?.length, PUBLISH_HISTORY_LIMIT)
  assertEquals(entries?.[0]?.eventId, eventIdFixture("e25".padEnd(64, "0")))
})

Deno.test("tearDownSubs - clears every subscription map on the state", () => {
  const state = stubRelayState()
  state.subs.set(SUB_1_ID, stubWireSub({ filterHash: "h" }))
  state.pendingSubs.set(SUB_2_ID, stubWireSub({ filters: [{ kinds: [0] }], filterHash: "h2" }))
  state.subIdByFilterHash.set("h", SUB_1_ID)
  const subHistory = new Map<RelayUrl, Map<SubscriptionId, SubHistoryRecord>>()
  recordSubHistory({ subHistory, url: URL, subId: SUB_1_ID, filters: [{ kinds: [1] }], openedAt: 0 })
  tearDownSubs({ state, url: URL, subHistory, clock: systemWallClock, scheduler: systemScheduler, reason: "disposed" })
  assertEquals(state.subs.size, 0)
  assertEquals(state.pendingSubs.size, 0)
  assertEquals(state.subIdByFilterHash.size, 0)
})

Deno.test("tearDownSubs - fires the terminal onClosed (not onEose) for active listeners", () => {
  const state = stubRelayState()
  let eoseCalls = 0
  const closedReasons: Array<string> = []
  state.subs.set(
    SUB_1_ID,
    stubWireSub({
      filterHash: "h",
      listeners: [{
        onEvent: () => {},
        onEose: () => {
          eoseCalls++
        },
        onClosed: (reason) => {
          closedReasons.push(reason)
        },
      }],
    }),
  )
  tearDownSubs({
    state,
    url: URL,
    subHistory: new Map(),
    clock: systemWallClock,
    scheduler: systemScheduler,
    reason: "disposed",
  })
  assertEquals(eoseCalls, 0)
  assertEquals(closedReasons, ["disposed"])
})
