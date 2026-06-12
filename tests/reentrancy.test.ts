import { assertEquals } from "@std/assert"
import type { NostrEvent } from "@innis/nostr-core"
import { buildEventFixture } from "@innis/nostr-core/testing"
import { createRelayPool } from "../src/infrastructure/adapter/web-socket-relay-pool-adapter.ts"
import { createInMemoryRelay } from "../testing.ts"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

// A callback that re-enters the pool during dispatch must not corrupt the listener array being
// walked. Listener arrays are copy-on-write — never mutated in place, only replaced — and `for...of`
// evaluates `wireSub.listeners` once, so an add/remove from inside a callback swaps in a new array
// without changing who receives the current event.

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

Deno.test("re-entrancy: unsubscribing from inside onEvent still delivers the in-flight event to remaining listeners", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const firstReceived: Array<NostrEvent> = []
    const secondReceived: Array<NostrEvent> = []

    // The first listener removes itself mid-dispatch. The second listener shares the same wire sub
    // and sits after the first in the listener array, so it must still receive the in-flight event.
    const first = pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (e) => {
        firstReceived.push(e)
        first.unsubscribe()
      },
    })
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (e) => {
        secondReceived.push(e)
      },
    })

    await delay(200)
    relay.inject(buildEventFixture({ kind: 1 }))
    await delay(200)

    assertEquals(firstReceived.length, 1)
    assertEquals(secondReceived.length, 1, "remaining listener must still receive the in-flight event")

    relay.inject(buildEventFixture({ kind: 1 }))
    await delay(200)

    assertEquals(firstReceived.length, 1, "unsubscribed listener must receive no further events")
    assertEquals(secondReceived.length, 2)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("re-entrancy: synchronous unsubscribe suppresses the late joiner's synthetic EOSE", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(300)

    // The wire sub has already EOSE'd, so this late joiner is owed a synthetic EOSE on the next
    // microtask — unless it leaves synchronously, in which case the membership re-check must skip it.
    let eose = false
    const sub = pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: () => {},
      onEose: () => {
        eose = true
      },
    })
    sub.unsubscribe()
    await new Promise<void>((resolve) => queueMicrotask(resolve))
    assertEquals(eose, false, "a listener that left synchronously must not be signalled on the microtask")
  } finally {
    pool.dispose()
    await relay.stop()
  }
})
