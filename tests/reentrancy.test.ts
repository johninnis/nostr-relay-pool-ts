import { assertEquals } from "@std/assert"
import type { NostrEvent } from "@innis/nostr-core"
import { buildEventFixture } from "@innis/nostr-core/testing"
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

const inFlightEvent = (): NostrEvent => buildEventFixture({ kind: 1, content: "in flight" })

const nextEvent = (): NostrEvent => buildEventFixture({ kind: 1, content: "next" })

const deliverInFlightThenNext = async (relay: InMemoryRelay, inFlight: NostrEvent, next: NostrEvent): Promise<void> => {
  await delay(200)
  relay.inject(inFlight)
  await delay(200)
  relay.inject(next)
  await delay(200)
}

Deno.test("re-entrancy: subscribing from inside onEvent does not replay the in-flight event to the new listener", async () => {
  const next = nextEvent()
  const lateJoinerIds = await withPool(async (relay, pool) => {
    const received: Array<string> = []
    let joined = false
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: () => {
        if (joined) return
        joined = true
        pool.subscribe(relay.url, [{ kinds: [1] }], {
          onEvent: (e) => {
            received.push(e.id)
          },
        })
      },
    })
    await deliverInFlightThenNext(relay, inFlightEvent(), next)
    return received
  })
  assertEquals(lateJoinerIds, [next.id])
})

interface SelfRemovalDuringDispatch {
  readonly leaverIds: ReadonlyArray<string>
  readonly remainingIds: ReadonlyArray<string>
}

const unsubscribeFromInsideOnEvent = (inFlight: NostrEvent, next: NostrEvent): Promise<SelfRemovalDuringDispatch> =>
  withPool(async (relay, pool) => {
    const leaverIds: Array<string> = []
    const remainingIds: Array<string> = []
    const leaver = pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (e) => {
        leaverIds.push(e.id)
        leaver.unsubscribe()
      },
    })
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (e) => {
        remainingIds.push(e.id)
      },
    })
    await deliverInFlightThenNext(relay, inFlight, next)
    return { leaverIds, remainingIds }
  })

Deno.test("re-entrancy: unsubscribing from inside onEvent still delivers the in-flight event to remaining listeners", async () => {
  const inFlight = inFlightEvent()
  const next = nextEvent()
  const { remainingIds } = await unsubscribeFromInsideOnEvent(inFlight, next)
  assertEquals(remainingIds, [inFlight.id, next.id])
})

Deno.test("re-entrancy: a listener that unsubscribes inside onEvent receives no further events", async () => {
  const inFlight = inFlightEvent()
  const { leaverIds } = await unsubscribeFromInsideOnEvent(inFlight, nextEvent())
  assertEquals(leaverIds, [inFlight.id])
})

Deno.test("re-entrancy: synchronous unsubscribe suppresses the late joiner's synthetic EOSE", async () => {
  const eose = await withPool(async (relay, pool) => {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(300)
    let signalled = false
    const sub = pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: () => {},
      onEose: () => {
        signalled = true
      },
    })
    sub.unsubscribe()
    await new Promise<void>((resolve) => queueMicrotask(resolve))
    return signalled
  })
  assertEquals(eose, false)
})
