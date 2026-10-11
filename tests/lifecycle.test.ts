import { assertEquals, assertNotEquals } from "@std/assert"
import type { RelayUrl } from "@innis/nostr-core"
import { buildEventFixture, relayUrlFixture } from "@innis/nostr-core/testing"
import type { RelayPool } from "../src/application/port/relay-pool.ts"
import type { RelayPoolStateEntry } from "../src/domain/value-object/relay-pool-state-entry.ts"
import { createRelayPool } from "../src/infrastructure/web-socket/web-socket-relay-pool.ts"
import { createInMemoryRelay } from "../testing.ts"
import { createManualTime, type ManualTime } from "./_helpers/scheduler.ts"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))
const WITHIN_FIRST_BACKOFF_MS = 100
const PAST_FIRST_BACKOFF_MS = 700

const entryOf = (pool: RelayPool, url: RelayUrl): RelayPoolStateEntry | undefined =>
  pool.getRelayPoolState().find((e) => e.url === url)

const statusOf = (pool: RelayPool, url: RelayUrl): RelayPoolStateEntry["status"] | undefined =>
  entryOf(pool, url)?.status

const signalsOnDisconnect = async () => {
  const relay = createInMemoryRelay({ eoseDelayMs: 50_000 })
  await relay.start()
  const pool = createRelayPool()
  try {
    let eoseCalls = 0
    const closedReasons: Array<string> = []
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: () => {},
      onEose: () => eoseCalls++,
      onClosed: (reason) => closedReasons.push(reason),
    })
    await delay(200)
    pool.disconnect(relay.url)
    await delay(50)
    return { eoseCalls, closedReasons }
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

const afterRelayDropsWithActiveSubscription = async <T>(
  observe: (pool: RelayPool, url: RelayUrl) => Promise<T> | T,
): Promise<T> => {
  const relay = createInMemoryRelay()
  await relay.start()
  const url = relay.url
  const pool = createRelayPool()
  try {
    pool.subscribe(url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    await relay.stop()
    await delay(250)
    return await observe(pool, url)
  } finally {
    pool.dispose()
  }
}

const withRestoredCooldown = <T>(observe: (pool: RelayPool, time: ManualTime, url: RelayUrl) => T): T => {
  const time = createManualTime(1000)
  const url = relayUrlFixture("wss://relay.invalid")
  const pool = createRelayPool({
    clock: time.clock,
    scheduler: time.scheduler,
    initialBackoff: [{ url, disabledUntil: 6000, step: 3 }],
  })
  try {
    return observe(pool, time, url)
  } finally {
    pool.dispose()
  }
}

const unsubscribedWithinReconnectWindow = async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const receivedIds: Array<string> = []
    const handle = pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: (e) => receivedIds.push(e.id) })
    await delay(200)
    relay.dropConnections()
    await delay(WITHIN_FIRST_BACKOFF_MS)
    handle.unsubscribe()
    await delay(PAST_FIRST_BACKOFF_MS)
    relay.inject(buildEventFixture({ kind: 1 }))
    await delay(200)
    return { receivedIds, connectedUrls: pool.getConnectedRelayUrls() }
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

const idleTimeoutAfterLastUnsubscribe = async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const time = createManualTime()
  const pool = createRelayPool({ clock: time.clock, scheduler: time.scheduler })
  try {
    const handle = pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    handle.unsubscribe()
    time.tick(30_000)
    await delay(100)
    return {
      connectedUrls: pool.getConnectedRelayUrls(),
      disabledUntil: entryOf(pool, relay.url)?.disabledUntil ?? null,
    }
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

const idleTimeoutAfterNewActivity = async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const time = createManualTime()
  const pool = createRelayPool({ clock: time.clock, scheduler: time.scheduler })
  try {
    const first = pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    first.unsubscribe()
    const receivedIds: Array<string> = []
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: (e) => receivedIds.push(e.id) })
    await delay(100)
    time.tick(30_000)
    await delay(100)
    const connectedUrls = pool.getConnectedRelayUrls()
    relay.inject(buildEventFixture({ kind: 1 }))
    await delay(200)
    return { relayUrl: relay.url, connectedUrls, receivedIds }
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

const publishOnlySocket = async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const time = createManualTime()
  const pool = createRelayPool({ clock: time.clock, scheduler: time.scheduler })
  try {
    await pool.publish(relay.url, buildEventFixture({ kind: 1 }))
    const connectedBeforeIdleTimeout = pool.getConnectedRelayUrls()
    time.tick(30_000)
    await delay(100)
    return {
      relayUrl: relay.url,
      connectedBeforeIdleTimeout,
      connectedAfterIdleTimeout: pool.getConnectedRelayUrls(),
      disabledUntil: entryOf(pool, relay.url)?.disabledUntil ?? null,
    }
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

const publishInFlightAtDispose = async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const pending = pool.publish(relay.url, buildEventFixture({ kind: 1 }))
    pool.dispose()
    return await pending
  } finally {
    await relay.stop()
  }
}

Deno.test("disconnect - does not put the relay into backoff", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    pool.disconnect(relay.url)
    await delay(100)
    assertEquals(entryOf(pool, relay.url)?.disabledUntil, null)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("disconnect - fires the terminal onClosed on the live subscription", async () => {
  const { closedReasons } = await signalsOnDisconnect()
  assertEquals(closedReasons, ["disconnected"])
})

Deno.test("disconnect - does not fire onEose on the live subscription", async () => {
  const { eoseCalls } = await signalsOnDisconnect()
  assertEquals(eoseCalls, 0)
})

Deno.test("getRelayPoolState - reports reconnecting after a relay drops with an active subscription", async () => {
  assertEquals(await afterRelayDropsWithActiveSubscription(statusOf), "reconnecting")
})

Deno.test("setConnectionGate - cancels a pending reconnect for a now-disallowed relay", async () => {
  const status = await afterRelayDropsWithActiveSubscription((pool, url) => {
    pool.setConnectionGate(() => false)
    return statusOf(pool, url)
  })
  assertNotEquals(status, "reconnecting")
})

Deno.test("getRelayPoolState - reports a relay in a restored cooldown as disabled", () => {
  assertEquals(withRestoredCooldown((pool, _time, url) => statusOf(pool, url)), "disabled")
})

Deno.test("getRelayPoolState - reports when a restored cooldown ends", () => {
  assertEquals(withRestoredCooldown((pool, _time, url) => entryOf(pool, url)?.disabledUntil), 6000)
})

Deno.test("getRelayPoolState - a disabled entry refreshes once its cooldown lapses by time alone", () => {
  const entry = withRestoredCooldown((pool, time, url) => {
    pool.getRelayPoolState()
    time.tick(6000)
    return entryOf(pool, url)
  })
  assertEquals(entry, undefined)
})

Deno.test("disconnect - cancels a pending reconnect so the relay is not revived", async () => {
  const status = await afterRelayDropsWithActiveSubscription(async (pool, url) => {
    pool.disconnect(url)
    await delay(PAST_FIRST_BACKOFF_MS)
    return statusOf(pool, url)
  })
  assertNotEquals(status, "reconnecting")
})

Deno.test("disconnect - leaves no connection to a relay whose pending reconnect it cancelled", async () => {
  const connectedUrls = await afterRelayDropsWithActiveSubscription(async (pool, url) => {
    pool.disconnect(url)
    await delay(PAST_FIRST_BACKOFF_MS)
    return pool.getConnectedRelayUrls()
  })
  assertEquals(connectedUrls, [])
})

Deno.test("unsubscribe - during a reconnect window removes the sub so the reconnect cannot resurrect it", async () => {
  const { receivedIds } = await unsubscribedWithinReconnectWindow()
  assertEquals(receivedIds, [])
})

Deno.test("unsubscribe - during a reconnect window leaves the relay unconnected once the window passes", async () => {
  const { connectedUrls } = await unsubscribedWithinReconnectWindow()
  assertEquals(connectedUrls, [])
})

Deno.test("idle socket - closes after the last unsubscribe", async () => {
  const { connectedUrls } = await idleTimeoutAfterLastUnsubscribe()
  assertEquals(connectedUrls, [])
})

Deno.test("idle socket - an idle close after the last unsubscribe starts no backoff", async () => {
  const { disabledUntil } = await idleTimeoutAfterLastUnsubscribe()
  assertEquals(disabledUntil, null)
})

Deno.test("idle socket - new activity before the idle timeout keeps the socket open", async () => {
  const { relayUrl, connectedUrls } = await idleTimeoutAfterNewActivity()
  assertEquals(connectedUrls, [relayUrl])
})

Deno.test("idle socket - a subscription opened before the idle timeout keeps receiving events", async () => {
  const { receivedIds } = await idleTimeoutAfterNewActivity()
  assertEquals(receivedIds.length, 1)
})

Deno.test("idle socket - a publish-only socket stays open until the idle timeout", async () => {
  const { relayUrl, connectedBeforeIdleTimeout } = await publishOnlySocket()
  assertEquals(connectedBeforeIdleTimeout, [relayUrl])
})

Deno.test("idle socket - a publish-only socket closes after the idle timeout", async () => {
  const { connectedAfterIdleTimeout } = await publishOnlySocket()
  assertEquals(connectedAfterIdleTimeout, [])
})

Deno.test("idle socket - a publish-only socket's idle close starts no backoff", async () => {
  const { disabledUntil } = await publishOnlySocket()
  assertEquals(disabledUntil, null)
})

Deno.test("dispose - settles an in-flight publish as refused instead of leaving the promise hanging", async () => {
  const result = await publishInFlightAtDispose()
  assertEquals(result.ok, false)
})

Deno.test("dispose - settles an in-flight publish with the message disposed", async () => {
  const result = await publishInFlightAtDispose()
  assertEquals(result.message, "disposed")
})

Deno.test("dispose - does not persist backoff failure records", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const writes: Array<unknown> = []
  const pool = createRelayPool({
    backoffPersistence: { write: (record) => writes.push(record), remove: () => {} },
  })
  pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
  await delay(200)
  pool.dispose()
  await delay(100)
  assertEquals(writes.length, 0)
  await relay.stop()
})
