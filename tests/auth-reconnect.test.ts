import { assertEquals } from "@std/assert"
import type { NostrEvent } from "@innis/nostr-core"
import { buildEventFixture } from "@innis/nostr-core/testing"
import { createRelayPool } from "../src/infrastructure/adapter/web-socket-relay-pool-adapter.ts"
import { createInMemoryRelay } from "../testing.ts"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

// Regression: a subscription parked awaiting NIP-42 AUTH when the socket drops must be re-issued on
// reconnect. The relay only challenges in response to a REQ, so if the reconnect does not re-send
// the parked sub it never receives a fresh challenge and strands — no event is ever delivered.
Deno.test("auth-parked subscription survives a reconnect and resumes after re-auth", async () => {
  const relay = createInMemoryRelay({ requireAuth: true })
  await relay.start()

  let authCallCount = 0
  let releaseFirstAuth: () => void = () => {}
  const firstAuthGate = new Promise<void>((resolve) => {
    releaseFirstAuth = resolve
  })

  const pool = createRelayPool({
    onAuthChallenge: async (): Promise<NostrEvent> => {
      authCallCount++
      // Hold the first auth open so the sub is still in the auth-parked state when the socket drops.
      if (authCallCount === 1) await firstAuthGate
      return buildEventFixture({ kind: 22242 })
    },
  })

  try {
    const stored = buildEventFixture({ kind: 1 })
    relay.inject(stored)

    const received: NostrEvent[] = []
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (event) => {
        received.push(event)
      },
    })

    await delay(200)
    assertEquals(authCallCount, 1, "the first REQ should have triggered exactly one auth challenge")
    assertEquals(received.length, 0, "nothing should arrive while the first auth is still gated")

    relay.dropConnections()
    await delay(900)

    assertEquals(authCallCount >= 2, true, "reconnect should re-issue the parked sub and earn a fresh challenge")
    assertEquals(received.length, 1, "the event should arrive once the reconnected sub re-authenticates")
    assertEquals(received[0]?.id, stored.id)

    releaseFirstAuth()
    await delay(50)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})
