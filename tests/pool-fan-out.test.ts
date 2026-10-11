import { assertEquals } from "@std/assert"
import type { NostrEvent, RelayUrl } from "@innis/nostr-core"
import { buildEventFixture } from "@innis/nostr-core/testing"
import { createRelayPool } from "../src/infrastructure/web-socket/web-socket-relay-pool.ts"
import { createInMemoryRelay } from "../testing.ts"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

interface OneShotFanOut {
  readonly relayUrl: RelayUrl
  readonly receivedIds: ReadonlyArray<string>
  readonly eosedRelays: ReadonlyArray<RelayUrl>
}

const subscribeManyToOneRelayHolding = async (event: NostrEvent): Promise<OneShotFanOut> => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    relay.inject(event)
    const receivedIds: Array<string> = []
    const eosedRelays: Array<RelayUrl> = []
    const sub = pool.subscribeMany([relay.url], [{ kinds: [1] }], {
      onEvent: (e) => {
        receivedIds.push(e.id)
      },
      onRelayEose: (url) => {
        eosedRelays.push(url)
      },
    })
    await delay(300)
    sub.unsubscribe()
    return { relayUrl: relay.url, receivedIds, eosedRelays }
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

Deno.test("subscribeMany single-URL: delivers the relay's stored events", async () => {
  const event = buildEventFixture({ kind: 1 })
  const { receivedIds } = await subscribeManyToOneRelayHolding(event)
  assertEquals(receivedIds, [event.id])
})

Deno.test("subscribeMany single-URL: fires onRelayEose for the relay on EOSE", async () => {
  const { relayUrl, eosedRelays } = await subscribeManyToOneRelayHolding(buildEventFixture({ kind: 1 }))
  assertEquals(eosedRelays, [relayUrl])
})

interface SyncedFanOut {
  readonly idsBeforeSync: ReadonlyArray<string>
  readonly idsAfterSync: ReadonlyArray<string>
  readonly idsAfterRemovedRelayStored: ReadonlyArray<string>
}

const syncFromRelayAToRelayB = async (
  eventA: NostrEvent,
  eventB: NostrEvent,
  laterEventA: NostrEvent,
): Promise<SyncedFanOut> => {
  const relayA = createInMemoryRelay()
  const relayB = createInMemoryRelay()
  await relayA.start()
  await relayB.start()
  const pool = createRelayPool()
  try {
    relayA.inject(eventA)
    relayB.inject(eventB)
    const received: Array<string> = []
    const sub = pool.subscribeManyLive([relayA.url], [{ kinds: [1] }], {
      onEvent: (e) => {
        received.push(e.id)
      },
    })
    await delay(200)
    const idsBeforeSync = [...received]
    sub.syncUrls([relayB.url])
    await delay(300)
    const idsAfterSync = [...received]
    relayA.inject(laterEventA)
    await delay(200)
    sub.unsubscribe()
    return { idsBeforeSync, idsAfterSync, idsAfterRemovedRelayStored: [...received] }
  } finally {
    pool.dispose()
    await relayA.stop()
    await relayB.stop()
  }
}

const eventOn = (relayName: string): NostrEvent => buildEventFixture({ kind: 1, content: `on relay ${relayName}` })

Deno.test("subscribeManyLive receives only from the relays it is opened on", async () => {
  const eventA = eventOn("A")
  const { idsBeforeSync } = await syncFromRelayAToRelayB(eventA, eventOn("B"), eventOn("A, later"))
  assertEquals(idsBeforeSync, [eventA.id])
})

Deno.test("subscribeManyLive syncUrls: opens added connections", async () => {
  const eventA = eventOn("A")
  const eventB = eventOn("B")
  const { idsAfterSync } = await syncFromRelayAToRelayB(eventA, eventB, eventOn("A, later"))
  assertEquals(idsAfterSync, [eventA.id, eventB.id])
})

Deno.test("subscribeManyLive syncUrls: closes removed connections", async () => {
  const laterEventA = eventOn("A, later")
  const { idsAfterRemovedRelayStored } = await syncFromRelayAToRelayB(eventOn("A"), eventOn("B"), laterEventA)
  assertEquals(idsAfterRemovedRelayStored.includes(laterEventA.id), false)
})
