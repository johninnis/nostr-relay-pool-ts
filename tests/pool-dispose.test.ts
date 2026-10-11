import { assertEquals } from "@std/assert"
import { buildEventFixture } from "@innis/nostr-core/testing"
import type { RelayPool } from "../src/application/port/relay-pool.ts"
import type { PublishResponse } from "../src/domain/value-object/publish-history.ts"
import { createRelayPool } from "../src/infrastructure/web-socket/web-socket-relay-pool.ts"
import { createInMemoryRelay, type InMemoryRelay } from "../testing.ts"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

const withPool = async <T>(act: (relay: InMemoryRelay, pool: RelayPool) => Promise<T>): Promise<T> => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    return await act(relay, pool)
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

const publishAfterDispose = (): Promise<PublishResponse> =>
  withPool((relay, pool) => {
    pool.dispose()
    return pool.publish(relay.url, buildEventFixture())
  })

Deno.test("dispose - rejects new subscriptions", async () => {
  const active = await withPool((relay, pool) => {
    pool.dispose()
    return Promise.resolve(pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} }).active)
  })
  assertEquals(active, false)
})

Deno.test("dispose - a publish after dispose is not ok", async () => {
  const { ok } = await publishAfterDispose()
  assertEquals(ok, false)
})

Deno.test("dispose - a publish after dispose settles as disposed", async () => {
  const { message } = await publishAfterDispose()
  assertEquals(message, "disposed")
})

Deno.test("dispose - closes connections", async () => {
  const connected = await withPool(async (relay, pool) => {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    pool.dispose()
    await delay(100)
    return pool.getConnectedRelayUrls()
  })
  assertEquals(connected, [])
})

Deno.test("dispose - is idempotent", () => {
  const pool = createRelayPool()
  pool.dispose()
  pool.dispose()
  assertEquals(pool.subscribe("wss://relay.example.com", [{ kinds: [1] }], { onEvent: () => {} }).active, false)
})
