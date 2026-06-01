import { assertEquals } from "@std/assert"
import type { NostrEvent } from "@innis/nostr-core"
import { buildEventFixture } from "@innis/nostr-core/testing"
import { createRelayPool } from "../src/infrastructure/adapter/web-socket-relay-pool-adapter.ts"
import { createInMemoryRelay } from "../testing.ts"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

// A callback that re-enters the pool during dispatch must not corrupt the listener set being walked.
// Every wireSub.listeners loop snapshots the set first (mirroring the adapter's connection-change
// dispatch), so an add/remove from inside a callback cannot change who receives the current event.

Deno.test("re-entrancy: subscribing from inside onEvent does not replay the in-flight event to the new listener", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const lateReceived: Array<NostrEvent> = []
    let joined = false

    // The first listener, on its first event, subscribes a second listener onto the *same* filter
    // hash — which adds to the wire sub's live listener set. Without a snapshot the new listener is
    // visited in the same dispatch and wrongly replays the in-flight event, breaking the documented
    // "a late joiner sees only events from the moment it joined onward" contract.
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: () => {
        if (joined) return
        joined = true
        pool.subscribe(relay.url, [{ kinds: [1] }], {
          onEvent: (e) => {
            lateReceived.push(e)
          },
        })
      },
    })

    await delay(200)
    relay.inject(buildEventFixture({ kind: 1 }))
    await delay(200)

    assertEquals(joined, true, "first listener should have re-entered subscribe")
    assertEquals(lateReceived.length, 0, "late joiner must not receive the event in flight when it joined")

    const future = buildEventFixture({ kind: 1 })
    relay.inject(future)
    await delay(200)

    assertEquals(lateReceived.length, 1, "late joiner should receive events emitted after it joined")
    assertEquals(lateReceived[0]?.id, future.id)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})
