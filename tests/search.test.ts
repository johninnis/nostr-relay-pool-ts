import { assertEquals } from "@std/assert"
import type { NostrFilter } from "@innis/nostr-core"
import { createRelayPool } from "../src/infrastructure/web-socket/web-socket-relay-pool.ts"
import type { RelayPoolConfig } from "../src/application/port/relay-pool-config.ts"
import { startRecordingRelay } from "./_helpers/recording-relay.ts"
import { PLAIN_RELAY_INFORMATION, relayInformationOf, SEARCH_RELAY_INFORMATION } from "./_helpers/relay-information.ts"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

const SEARCH: NostrFilter = { kinds: [1], search: "nostr" }
const PROFILES: NostrFilter = { kinds: [0] }

const filtersRequested = (received: ReadonlyArray<string>): unknown[] =>
  received.map((frame): unknown => JSON.parse(frame)).flatMap((frame) =>
    Array.isArray(frame) && frame[0] === "REQ" ? frame.slice(2) : []
  )

const filtersSent = async (filters: ReadonlyArray<NostrFilter>, config: RelayPoolConfig = {}): Promise<unknown[]> => {
  const relay = await startRecordingRelay()
  const pool = createRelayPool({ heartbeatIntervalMs: 0, ...config })
  try {
    pool.subscribe(relay.url, filters, { onEvent: () => {} })
    await delay(200)
    return filtersRequested(relay.received)
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

Deno.test("search - a filter with a search is sent to a relay whose NIP-11 document lists NIP-50", async () => {
  const sent = await filtersSent([SEARCH, PROFILES], { relayInformation: () => SEARCH_RELAY_INFORMATION })
  assertEquals(sent, [SEARCH, PROFILES])
})

Deno.test("search - a filter with a search is not sent to a relay whose NIP-11 document does not list NIP-50", async () => {
  const sent = await filtersSent([SEARCH, PROFILES], { relayInformation: () => PLAIN_RELAY_INFORMATION })
  assertEquals(sent, [PROFILES])
})

Deno.test("search - a filter with a search is not sent to a relay whose NIP-11 document lists no NIPs", async () => {
  const sent = await filtersSent([SEARCH, PROFILES], { relayInformation: () => relayInformationOf({ name: "relay" }) })
  assertEquals(sent, [PROFILES])
})

Deno.test("search - a filter with a search is not sent to a relay whose NIP-11 document is unknown", async () => {
  const sent = await filtersSent([SEARCH, PROFILES], { relayInformation: () => null })
  assertEquals(sent, [PROFILES])
})

Deno.test("search - a pool given no relay information sends a filter with a search to no relay", async () => {
  assertEquals(await filtersSent([SEARCH, PROFILES]), [PROFILES])
})

interface FiltersSentBySubscribeMany {
  readonly toSearchRelay: unknown[]
  readonly toPlainRelay: unknown[]
}

const filtersSentBySubscribeMany = async (): Promise<FiltersSentBySubscribeMany> => {
  const searchRelay = await startRecordingRelay()
  const plainRelay = await startRecordingRelay()
  const pool = createRelayPool({
    heartbeatIntervalMs: 0,
    relayInformation: (url) => url === searchRelay.url ? SEARCH_RELAY_INFORMATION : PLAIN_RELAY_INFORMATION,
  })
  try {
    pool.subscribeMany([searchRelay.url, plainRelay.url], [SEARCH, PROFILES], { onEvent: () => {} })
    await delay(200)
    return {
      toSearchRelay: filtersRequested(searchRelay.received),
      toPlainRelay: filtersRequested(plainRelay.received),
    }
  } finally {
    pool.dispose()
    await searchRelay.stop()
    await plainRelay.stop()
  }
}

Deno.test("search - subscribeMany sends a search to the relays whose NIP-11 documents list NIP-50", async () => {
  const { toSearchRelay } = await filtersSentBySubscribeMany()
  assertEquals(toSearchRelay, [SEARCH, PROFILES])
})

Deno.test("search - subscribeMany sends no search to the relays whose NIP-11 documents do not list NIP-50", async () => {
  const { toPlainRelay } = await filtersSentBySubscribeMany()
  assertEquals(toPlainRelay, [PROFILES])
})
