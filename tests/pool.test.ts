import { assertEquals, assertNotEquals } from "@std/assert"
import { createRelayPool } from "../src/infrastructure/adapter/web-socket-relay-pool-adapter.ts"
import { buildEventFixture } from "@innis/nostr-core/testing"
import { createInMemoryRelay } from "../testing.ts"
import type { NostrEvent } from "@innis/nostr-core"
import { parsePublicKey } from "@innis/nostr-core"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

Deno.test("subscribe connects to relay and receives events matching filter", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const event = buildEventFixture({ kind: 1 })
    relay.inject(event)

    const received: NostrEvent[] = []
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (e) => {
        received.push(e)
      },
    })

    await delay(300)
    assertEquals(received.length, 1)
    const [first] = received
    if (!first) throw new Error("expected one received event")
    assertEquals(first.id, event.id)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("subscribe receives EOSE callback", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    relay.inject(buildEventFixture({ kind: 1 }))

    let eoseReceived = false
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: () => {},
      onEose: () => {
        eoseReceived = true
      },
    })

    await delay(300)
    assertEquals(eoseReceived, true)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("subscribe does not receive events after unsubscribe", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const received: NostrEvent[] = []
    const sub = pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (e) => {
        received.push(e)
      },
    })

    await delay(200)
    sub.unsubscribe()

    relay.inject(buildEventFixture({ kind: 1 }))
    await delay(200)

    assertEquals(received.length, 0)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("publish sends event and receives OK response", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const event = buildEventFixture({ kind: 1 })

    const result = await pool.publish(relay.url, event)
    assertEquals(result.ok, true)

    const stored = relay.getStoredEvents()
    assertEquals(stored.some((e) => e.id === event.id), true)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("publish settles as disconnected when the socket is closed before it is acked", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    // The socket is still CONNECTING when disconnect closes it, so the event never gets an OK. The
    // publish must settle on the close rather than hang for the full publishTimeoutMs.
    const pending = pool.publish(relay.url, buildEventFixture({ kind: 1 }))
    pool.disconnect(relay.url)
    const result = await pending
    assertEquals(result.ok, false)
    assertEquals(result.message, "disconnected")
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("subscribeMany single-URL: delivers events and fires onRelayEose on EOSE", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const event = buildEventFixture({ kind: 1 })
    relay.inject(event)

    const received: NostrEvent[] = []
    let eosedFor: string | null = null

    const sub = pool.subscribeMany([relay.url], [{ kinds: [1] }], {
      onEvent: (e) => {
        received.push(e)
      },
      onRelayEose: (r) => {
        eosedFor = r
      },
    })

    await delay(300)
    sub.unsubscribe()

    assertEquals(received.length, 1)
    const [first] = received
    if (!first) throw new Error("expected one received event")
    assertEquals(first.id, event.id)
    assertEquals(eosedFor !== null, true)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("subscribeMany syncUrls: closes removed and opens added connections", async () => {
  const relayA = createInMemoryRelay()
  const relayB = createInMemoryRelay()
  await relayA.start()
  await relayB.start()
  const pool = createRelayPool()
  try {
    const eventA = buildEventFixture({ kind: 1 })
    const eventB = buildEventFixture({ kind: 1 })
    relayA.inject(eventA)
    relayB.inject(eventB)

    const received: NostrEvent[] = []
    const sub = pool.subscribeMany([relayA.url], [{ kinds: [1] }], {
      onEvent: (e) => {
        received.push(e)
      },
    }, { persistent: true })

    await delay(200)
    assertEquals(received.some((e) => e.id === eventA.id), true)
    assertEquals(received.some((e) => e.id === eventB.id), false)

    sub.syncUrls([relayB.url])
    await delay(300)

    assertEquals(received.some((e) => e.id === eventB.id), true)

    sub.unsubscribe()
  } finally {
    pool.dispose()
    await relayA.stop()
    await relayB.stop()
  }
})

Deno.test("getConnectedRelayUrls returns URLs of open connections", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })

    await delay(300)

    const connected = pool.getConnectedRelayUrls()
    assertEquals(connected.length >= 1, true)
    assertEquals(connected.some((u) => u.includes("127.0.0.1")), true)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("getAttemptedRelayUrls includes all ever-attempted URLs", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })

    await delay(200)

    const seen = pool.getAttemptedRelayUrls()
    assertEquals(seen.length >= 1, true)
    assertEquals(seen.some((u) => u.includes("127.0.0.1")), true)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("getRelayPoolState exposes per-relay eventCount", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    relay.inject(buildEventFixture({ kind: 1 }))
    relay.inject(buildEventFixture({ kind: 1 }))

    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(300)

    const state = pool.getRelayPoolState()
    const entry = state.find((e) => e.url.includes("127.0.0.1"))
    if (!entry) throw new Error("expected a state entry for the relay")
    assertEquals(entry.eventCount >= 2, true)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("subscribe only receives events matching the filter", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    relay.inject(buildEventFixture({ kind: 1 }))
    relay.inject(buildEventFixture({ kind: 3 }))

    const received: NostrEvent[] = []
    pool.subscribe(relay.url, [{ kinds: [3] }], {
      onEvent: (e) => {
        received.push(e)
      },
    })

    await delay(300)
    assertEquals(received.length, 1)
    const [first] = received
    if (!first) throw new Error("expected one received event")
    assertEquals(first.kind, 3)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("subscribe receives live-injected events after EOSE", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const received: NostrEvent[] = []
    let eoseReceived = false
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (e) => {
        received.push(e)
      },
      onEose: () => {
        eoseReceived = true
      },
    })

    await delay(300)
    assertEquals(eoseReceived, true)
    assertEquals(received.length, 0)

    const liveEvent = buildEventFixture({ kind: 1 })
    relay.inject(liveEvent)
    await delay(200)

    assertEquals(received.length, 1)
    const [first] = received
    if (!first) throw new Error("expected one received event")
    assertEquals(first.id, liveEvent.id)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("subscribe deduplicates identical filters on the same relay", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const event = buildEventFixture({ kind: 1 })
    relay.inject(event)

    const received1: NostrEvent[] = []
    const received2: NostrEvent[] = []
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (e) => {
        received1.push(e)
      },
    })
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (e) => {
        received2.push(e)
      },
    })

    await delay(300)

    assertEquals(received1.length, 1)
    assertEquals(received2.length, 1)

    const active = pool.getRelaySubscriptions(relay.url).filter((s) => s.status === "active")
    assertEquals(active.length, 1)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("subscribe dedup: first unsubscribe keeps wire sub alive for remaining listeners", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const received1: NostrEvent[] = []
    const received2: NostrEvent[] = []
    const sub1 = pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (e) => {
        received1.push(e)
      },
    })
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (e) => {
        received2.push(e)
      },
    })

    await delay(200)
    sub1.unsubscribe()

    relay.inject(buildEventFixture({ kind: 1 }))
    await delay(200)

    assertEquals(received1.length, 0)
    assertEquals(received2.length, 1)

    const active = pool.getRelaySubscriptions(relay.url).filter((s) => s.status === "active")
    assertEquals(active.length, 1)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("subscribe dedup: late joiner gets synthetic EOSE on a microtask when wire sub already EOSE'd", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    let eose1 = false
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: () => {},
      onEose: () => {
        eose1 = true
      },
    })
    await delay(300)
    assertEquals(eose1, true, "first subscriber should receive EOSE")

    let eose2 = false
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: () => {},
      onEose: () => {
        eose2 = true
      },
    })
    assertEquals(eose2, false, "synthetic EOSE must not fire synchronously inside subscribe()")
    await new Promise<void>((resolve) => queueMicrotask(resolve))
    assertEquals(eose2, true, "late joiner should receive synthetic EOSE on the next microtask")
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("subscribe dedup: filter key order and array order do not matter", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const author = parsePublicKey("a".repeat(64))
    pool.subscribe(relay.url, [{ kinds: [1, 2], authors: [author] }], { onEvent: () => {} })
    pool.subscribe(relay.url, [{ authors: [author], kinds: [2, 1] }], { onEvent: () => {} })

    await delay(300)

    const active = pool.getRelaySubscriptions(relay.url).filter((s) => s.status === "active")
    assertEquals(active.length, 1)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("getRelayPoolState returns state entries", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })

    await delay(300)

    const state = pool.getRelayPoolState()
    assertEquals(state.length >= 1, true)
    const entry = state.find((s) => s.url.includes("127.0.0.1"))
    assertNotEquals(entry, undefined)
    if (!entry) throw new Error("expected a relay pool state entry for the in-memory relay")
    assertEquals(entry.status, "connected")
    assertEquals(entry.activeSubscriptionCount >= 1, true)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("dispose - rejects new subscriptions and publishes", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    pool.dispose()
    const sub = pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    assertEquals(sub.active, false)
    const result = await pool.publish(relay.url, buildEventFixture())
    assertEquals(result.ok, false)
    assertEquals(result.message, "disposed")
  } finally {
    await relay.stop()
  }
})

Deno.test("dispose - closes connections and cancels reconnects", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
  await delay(200)
  assertEquals(pool.getConnectedRelayUrls().length >= 1, true)
  pool.dispose()
  await delay(100)
  assertEquals(pool.getConnectedRelayUrls().length, 0)
  await relay.stop()
})

Deno.test("dispose - is idempotent", () => {
  const pool = createRelayPool()
  pool.dispose()
  pool.dispose()
})
