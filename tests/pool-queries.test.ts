import { assertEquals } from "@std/assert"
import type { RelayUrl } from "@innis/nostr-core"
import { buildEventFixture } from "@innis/nostr-core/testing"
import type { RelayPool } from "../src/application/port/relay-pool.ts"
import type { RelayPoolStateEntry } from "../src/domain/value-object/relay-pool-state-entry.ts"
import { createRelayPool } from "../src/infrastructure/web-socket/web-socket-relay-pool.ts"
import { createInMemoryRelay, type InMemoryRelay } from "../testing.ts"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

interface Queried<T> {
  readonly relayUrl: RelayUrl
  readonly answer: T
}

const queryAfterSubscribing = async <T>(
  waitMs: number,
  query: (pool: RelayPool, relay: InMemoryRelay) => T,
): Promise<Queried<T>> => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(waitMs)
    return { relayUrl: relay.url, answer: query(pool, relay) }
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

const poolStateEntryOf = (pool: RelayPool, relay: InMemoryRelay): RelayPoolStateEntry | undefined =>
  pool.getRelayPoolState().find((entry) => entry.url === relay.url)

Deno.test("getConnectedRelayUrls returns URLs of open connections", async () => {
  const { relayUrl, answer } = await queryAfterSubscribing(300, (pool) => pool.getConnectedRelayUrls())
  assertEquals(answer, [relayUrl])
})

Deno.test("getAttemptedRelayUrls includes all ever-attempted URLs", async () => {
  const { relayUrl, answer } = await queryAfterSubscribing(200, (pool) => pool.getAttemptedRelayUrls())
  assertEquals(answer, [relayUrl])
})

Deno.test("getRelayPoolState lists every relay the pool connected to", async () => {
  const { relayUrl, answer } = await queryAfterSubscribing(300, (pool) => pool.getRelayPoolState().map((e) => e.url))
  assertEquals(answer, [relayUrl])
})

Deno.test("getRelayPoolState reports a relay with an open subscription as connected", async () => {
  const { answer } = await queryAfterSubscribing(300, (pool, relay) => poolStateEntryOf(pool, relay)?.status)
  assertEquals(answer, "connected")
})

Deno.test("getRelayPoolState counts the relay's active subscription", async () => {
  const { answer } = await queryAfterSubscribing(
    300,
    (pool, relay) => poolStateEntryOf(pool, relay)?.activeSubscriptionCount,
  )
  assertEquals(answer, 1)
})

Deno.test("getRelayPoolState exposes per-relay eventCount", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    relay.inject(buildEventFixture({ kind: 1, content: "first" }))
    relay.inject(buildEventFixture({ kind: 1, content: "second" }))
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(300)
    assertEquals(poolStateEntryOf(pool, relay)?.eventCount, 2)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})
