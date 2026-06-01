import { assertEquals } from "@std/assert"
import type { RelayUrl } from "@innis/nostr-core"
import { parseEventId, parseRelayUrl } from "@innis/nostr-core"
import type { PublishHistoryRecord, SubHistoryRecord } from "../src/application/service/relay-history.ts"
import { systemWallClock } from "../src/infrastructure/adapter/system-wall-clock-adapter.ts"
import { systemScheduler } from "../src/infrastructure/adapter/system-scheduler-adapter.ts"
import {
  closeSubHistory,
  PUBLISH_HISTORY_LIMIT,
  recordPublishEntry,
  recordSubHistory,
  SUB_HISTORY_LIMIT,
} from "../src/application/service/relay-history.ts"
import { tearDownSubs } from "../src/infrastructure/web-socket/relay-state.ts"
import { stubRelayState, stubWireSub } from "./_helpers/relay-state.ts"

const URL = parseRelayUrl("wss://relay.example.com")

const publishEntry = (raw: string): PublishHistoryRecord => ({
  eventId: parseEventId(raw.padEnd(64, "0")),
  kind: 1,
  publishedAt: 1000,
  result: "pending",
  message: "",
})

Deno.test("recordSubHistory - records a new open subscription entry", () => {
  const subHistory = new Map<RelayUrl, Map<string, SubHistoryRecord>>()
  recordSubHistory({ subHistory, url: URL, subId: "sub-1", filters: [{ kinds: [1] }], openedAt: 42 })
  const entry = subHistory.get(URL)?.get("sub-1")
  assertEquals(entry?.subId, "sub-1")
  assertEquals(entry?.openedAt, 42)
  assertEquals(entry?.closedAt, null)
  assertEquals(entry?.eventCount, 0)
})

Deno.test("recordSubHistory - keeps multiple entries for the same relay", () => {
  const subHistory = new Map<RelayUrl, Map<string, SubHistoryRecord>>()
  recordSubHistory({ subHistory, url: URL, subId: "sub-1", filters: [{ kinds: [1] }], openedAt: 1 })
  recordSubHistory({ subHistory, url: URL, subId: "sub-2", filters: [{ kinds: [0] }], openedAt: 2 })
  assertEquals(subHistory.get(URL)?.size, 2)
})

Deno.test("recordSubHistory - caps closed entries at SUB_HISTORY_LIMIT", () => {
  const subHistory = new Map<RelayUrl, Map<string, SubHistoryRecord>>()
  for (let i = 0; i < SUB_HISTORY_LIMIT + 50; i++) {
    const subId = `sub-${i}`
    recordSubHistory({ subHistory, url: URL, subId, filters: [{ kinds: [1] }], openedAt: i })
    closeSubHistory({ subHistory, url: URL, subId, clock: () => i })
  }
  assertEquals(subHistory.get(URL)?.size, SUB_HISTORY_LIMIT)
})

Deno.test("recordSubHistory - never evicts still-open subscriptions, even past the cap", () => {
  const subHistory = new Map<RelayUrl, Map<string, SubHistoryRecord>>()
  for (let i = 0; i < SUB_HISTORY_LIMIT + 10; i++) {
    recordSubHistory({ subHistory, url: URL, subId: `open-${i}`, filters: [{ kinds: [1] }], openedAt: i })
  }
  assertEquals(subHistory.get(URL)?.size, SUB_HISTORY_LIMIT + 10)
})

Deno.test("closeSubHistory - stamps the closedAt time on an open entry", () => {
  const subHistory = new Map<RelayUrl, Map<string, SubHistoryRecord>>()
  recordSubHistory({ subHistory, url: URL, subId: "sub-1", filters: [{ kinds: [1] }], openedAt: 1 })
  closeSubHistory({ subHistory, url: URL, subId: "sub-1", clock: () => 99 })
  assertEquals(subHistory.get(URL)?.get("sub-1")?.closedAt, 99)
})

Deno.test("closeSubHistory - is a no-op for an unknown subscription", () => {
  const subHistory = new Map<RelayUrl, Map<string, SubHistoryRecord>>()
  closeSubHistory({ subHistory, url: URL, subId: "missing", clock: systemWallClock })
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
  assertEquals(entries?.[0]?.eventId, parseEventId("e25".padEnd(64, "0")))
})

Deno.test("tearDownSubs - clears every subscription map on the state", () => {
  const state = stubRelayState()
  state.subs.set("s1", stubWireSub({ filterHash: "h" }))
  state.pendingSubs.set("s2", stubWireSub({ filters: [{ kinds: [0] }], filterHash: "h2" }))
  state.subIdByFilterHash.set("h", "s1")
  const subHistory = new Map<RelayUrl, Map<string, SubHistoryRecord>>()
  recordSubHistory({ subHistory, url: URL, subId: "s1", filters: [{ kinds: [1] }], openedAt: 0 })
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
    "s1",
    stubWireSub({
      filterHash: "h",
      listeners: new Set([{
        onEvent: () => {},
        onEose: () => {
          eoseCalls++
        },
        onClosed: (reason) => {
          closedReasons.push(reason)
        },
      }]),
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
