import { assertEquals } from "@std/assert"
import type { NostrEvent, NostrFilter } from "@innis/nostr-core"
import { buildEventFixture, publicKeyFixture } from "@innis/nostr-core/testing"
import type { RelayPool } from "../src/application/port/relay-pool.ts"
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

const activeWireSubscriptions = (relay: InMemoryRelay, pool: RelayPool): number =>
  pool.getRelaySubscriptions(relay.url).filter((s) => s.status === "active").length

const storedEventIdsReceived = (
  stored: ReadonlyArray<NostrEvent>,
  filters: ReadonlyArray<NostrFilter>,
): Promise<ReadonlyArray<string>> =>
  withPool(async (relay, pool) => {
    for (const event of stored) relay.inject(event)
    const received: Array<string> = []
    pool.subscribe(relay.url, filters, {
      onEvent: (e) => {
        received.push(e.id)
      },
    })
    await delay(300)
    return received
  })

Deno.test("subscribe connects to relay and receives events matching filter", async () => {
  const event = buildEventFixture({ kind: 1 })
  assertEquals(await storedEventIdsReceived([event], [{ kinds: [1] }]), [event.id])
})

Deno.test("subscribe only receives events matching the filter", async () => {
  const contacts = buildEventFixture({ kind: 3 })
  assertEquals(await storedEventIdsReceived([buildEventFixture({ kind: 1 }), contacts], [{ kinds: [3] }]), [
    contacts.id,
  ])
})

Deno.test("subscribe receives EOSE callback", async () => {
  const eoseReceived = await withPool(async (relay, pool) => {
    relay.inject(buildEventFixture({ kind: 1 }))
    let eose = false
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: () => {},
      onEose: () => {
        eose = true
      },
    })
    await delay(300)
    return eose
  })
  assertEquals(eoseReceived, true)
})

Deno.test("subscribe does not receive events after unsubscribe", async () => {
  const receivedCount = await withPool(async (relay, pool) => {
    const received: Array<NostrEvent> = []
    const sub = pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (e) => {
        received.push(e)
      },
    })
    await delay(200)
    sub.unsubscribe()
    relay.inject(buildEventFixture({ kind: 1 }))
    await delay(200)
    return received.length
  })
  assertEquals(receivedCount, 0)
})

Deno.test("subscribe receives live-injected events after EOSE", async () => {
  const liveEvent = buildEventFixture({ kind: 1 })
  const ids = await withPool(async (relay, pool) => {
    const received: Array<string> = []
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (e) => {
        received.push(e.id)
      },
      onEose: () => relay.inject(liveEvent),
    })
    await delay(500)
    return received
  })
  assertEquals(ids, [liveEvent.id])
})

interface DeduplicatedSubscription {
  readonly firstIds: ReadonlyArray<string>
  readonly secondIds: ReadonlyArray<string>
  readonly activeWireSubs: number
}

const subscribeTwiceToAStoredEvent = (event: NostrEvent): Promise<DeduplicatedSubscription> =>
  withPool(async (relay, pool) => {
    relay.inject(event)
    const firstIds: Array<string> = []
    const secondIds: Array<string> = []
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (e) => {
        firstIds.push(e.id)
      },
    })
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (e) => {
        secondIds.push(e.id)
      },
    })
    await delay(300)
    return { firstIds, secondIds, activeWireSubs: activeWireSubscriptions(relay, pool) }
  })

Deno.test("subscribe deduplicates identical filters on the same relay", async () => {
  const { activeWireSubs } = await subscribeTwiceToAStoredEvent(buildEventFixture({ kind: 1 }))
  assertEquals(activeWireSubs, 1)
})

Deno.test("subscribe dedup: the first listener receives the stored event", async () => {
  const event = buildEventFixture({ kind: 1 })
  const { firstIds } = await subscribeTwiceToAStoredEvent(event)
  assertEquals(firstIds, [event.id])
})

Deno.test("subscribe dedup: the listener that joined receives the stored event too", async () => {
  const event = buildEventFixture({ kind: 1 })
  const { secondIds } = await subscribeTwiceToAStoredEvent(event)
  assertEquals(secondIds, [event.id])
})

interface AfterFirstUnsubscribe {
  readonly unsubscribedCount: number
  readonly remainingCount: number
  readonly activeWireSubs: number
}

const unsubscribeFirstOfTwoListeners = (): Promise<AfterFirstUnsubscribe> =>
  withPool(async (relay, pool) => {
    let unsubscribedCount = 0
    let remainingCount = 0
    const first = pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: () => {
        unsubscribedCount++
      },
    })
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: () => {
        remainingCount++
      },
    })
    await delay(200)
    first.unsubscribe()
    relay.inject(buildEventFixture({ kind: 1 }))
    await delay(200)
    return { unsubscribedCount, remainingCount, activeWireSubs: activeWireSubscriptions(relay, pool) }
  })

Deno.test("subscribe dedup: an unsubscribed listener receives no further events", async () => {
  const { unsubscribedCount } = await unsubscribeFirstOfTwoListeners()
  assertEquals(unsubscribedCount, 0)
})

Deno.test("subscribe dedup: the remaining listener still receives events after another unsubscribes", async () => {
  const { remainingCount } = await unsubscribeFirstOfTwoListeners()
  assertEquals(remainingCount, 1)
})

Deno.test("subscribe dedup: first unsubscribe keeps wire sub alive for remaining listeners", async () => {
  const { activeWireSubs } = await unsubscribeFirstOfTwoListeners()
  assertEquals(activeWireSubs, 1)
})

interface LateJoinerEose {
  readonly insideSubscribe: boolean
  readonly onNextMicrotask: boolean
}

const joinAfterTheWireSubEosed = (): Promise<LateJoinerEose> =>
  withPool(async (relay, pool) => {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(300)
    let eose = false
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: () => {},
      onEose: () => {
        eose = true
      },
    })
    const insideSubscribe = eose
    await new Promise<void>((resolve) => queueMicrotask(resolve))
    return { insideSubscribe, onNextMicrotask: eose }
  })

Deno.test("subscribe dedup: a late joiner's synthetic EOSE does not fire synchronously inside subscribe", async () => {
  const { insideSubscribe } = await joinAfterTheWireSubEosed()
  assertEquals(insideSubscribe, false)
})

Deno.test("subscribe dedup: late joiner gets synthetic EOSE on a microtask when wire sub already EOSE'd", async () => {
  const { onNextMicrotask } = await joinAfterTheWireSubEosed()
  assertEquals(onNextMicrotask, true)
})

Deno.test("subscribe dedup: filter key order and array order do not matter", async () => {
  const author = publicKeyFixture("a".repeat(64))
  const activeWireSubs = await withPool(async (relay, pool) => {
    pool.subscribe(relay.url, [{ kinds: [1, 2], authors: [author] }], { onEvent: () => {} })
    pool.subscribe(relay.url, [{ authors: [author], kinds: [2, 1] }], { onEvent: () => {} })
    await delay(300)
    return activeWireSubscriptions(relay, pool)
  })
  assertEquals(activeWireSubs, 1)
})
