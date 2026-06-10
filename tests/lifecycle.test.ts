import { assertEquals, assertNotEquals } from "@std/assert"
import { parseRelayUrl } from "@innis/nostr-core"
import { buildEventFixture } from "@innis/nostr-core/testing"
import { createRelayPool } from "../src/infrastructure/adapter/web-socket-relay-pool-adapter.ts"
import { createInMemoryRelay } from "../testing.ts"
import { createManualTime } from "./_helpers/scheduler.ts"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

Deno.test("disconnect - does not put the relay into backoff", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    pool.disconnect(relay.url)
    await delay(100)
    const entry = pool.getRelayPoolState().find((e) => e.url === relay.url)
    assertEquals(entry?.disabledUntil, null)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("disconnect - fires the terminal onClosed (not onEose) on the live subscription", async () => {
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
    assertEquals(eoseCalls, 0)
    assertEquals(closedReasons, ["disconnected"])
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("getRelayPoolState - reports reconnecting after a relay drops with an active subscription", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const url = relay.url
  const pool = createRelayPool()
  try {
    pool.subscribe(url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    await relay.stop()
    await delay(250)
    const entry = pool.getRelayPoolState().find((e) => e.url === url)
    assertEquals(entry?.status, "reconnecting")
  } finally {
    pool.dispose()
  }
})

Deno.test("setConnectionGate - cancels a pending reconnect for a now-disallowed relay", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const url = relay.url
  const pool = createRelayPool()
  try {
    pool.subscribe(url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    await relay.stop()
    await delay(250)
    assertEquals(pool.getRelayPoolState().find((e) => e.url === url)?.status, "reconnecting")

    // The relay dropped with a live sub, so it sits in pendingReconnects with no socket. A gate that
    // now rejects it must cancel that queued reconnect rather than leave it reporting "reconnecting".
    pool.setConnectionGate(() => false)
    const status = pool.getRelayPoolState().find((e) => e.url === url)?.status
    assertNotEquals(status, "reconnecting")
  } finally {
    pool.dispose()
  }
})

Deno.test("getRelayPoolState - a disabled entry refreshes once its cooldown lapses by time alone", () => {
  const time = createManualTime(1000)
  const url = parseRelayUrl("wss://relay.invalid")
  const pool = createRelayPool({
    clock: time.clock,
    scheduler: time.scheduler,
    initialBackoff: [{ url, disabledUntil: 6000, step: 3 }],
  })
  try {
    const disabled = pool.getRelayPoolState().find((e) => e.url === url)
    assertEquals(disabled?.status, "disabled")
    assertEquals(disabled?.disabledUntil, 6000)

    // Advance past the window with no intervening mutation. A cache keyed only on mutations would
    // keep serving the stale "disabled" snapshot; the time-aware cache rebuilds and the expired,
    // never-attempted relay correctly drops out.
    time.tick(6000)
    assertEquals(pool.getRelayPoolState().find((e) => e.url === url), undefined)
  } finally {
    pool.dispose()
  }
})

Deno.test("disconnect - cancels a pending reconnect so the relay is not revived", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const url = relay.url
  const pool = createRelayPool()
  try {
    pool.subscribe(url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    await relay.stop()
    await delay(250)
    assertEquals(pool.getRelayPoolState().find((e) => e.url === url)?.status, "reconnecting")

    pool.disconnect(url)
    // Past the first backoff window: a still-scheduled reconnect would have fired by now and put
    // the relay back into "reconnecting". With the reconnect cancelled it must not return.
    await delay(700)
    const status = pool.getRelayPoolState().find((e) => e.url === url)?.status
    assertNotEquals(status, "reconnecting")
    assertEquals(pool.getConnectedRelayUrls().length, 0)
  } finally {
    pool.dispose()
  }
})

Deno.test("unsubscribe - during a reconnect window removes the sub so the reconnect cannot resurrect it", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const received: Array<string> = []
    const handle = pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: (e) => received.push(e.id) })
    await delay(200)

    // Drop the socket (network cut): the pool parks the sub and queues a reconnect in ~500ms.
    relay.dropConnections()
    await delay(100)
    // Unsubscribe inside the backoff window, while no live connection exists for the relay.
    handle.unsubscribe()

    // Past the window: a sub left parked would have been re-issued by the reconnect and would
    // receive this event as an unowned REQ that nothing can ever close.
    await delay(700)
    relay.inject(buildEventFixture({ kind: 1 }))
    await delay(200)

    assertEquals(received.length, 0)
    assertEquals(pool.getConnectedRelayUrls().length, 0)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("idle socket - closes after the last unsubscribe without a backoff penalty", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const time = createManualTime()
  const pool = createRelayPool({ clock: time.clock, scheduler: time.scheduler })
  try {
    const handle = pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    assertEquals(pool.getConnectedRelayUrls(), [relay.url])

    handle.unsubscribe()
    time.tick(30_000)
    await delay(100)

    assertEquals(pool.getConnectedRelayUrls().length, 0)
    // An idle close is pool-initiated, not a relay failure: it must not start a cooldown.
    const entry = pool.getRelayPoolState().find((e) => e.url === relay.url)
    assertEquals(entry?.disabledUntil ?? null, null)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("idle socket - new activity before the idle timeout keeps the socket open", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const time = createManualTime()
  const pool = createRelayPool({ clock: time.clock, scheduler: time.scheduler })
  try {
    const first = pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    first.unsubscribe()

    const received: Array<string> = []
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: (e) => received.push(e.id) })
    await delay(100)
    time.tick(30_000)
    await delay(100)

    assertEquals(pool.getConnectedRelayUrls(), [relay.url])
    relay.inject(buildEventFixture({ kind: 1 }))
    await delay(200)
    assertEquals(received.length, 1)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("idle socket - a publish-only socket closes after the idle timeout", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const time = createManualTime()
  const pool = createRelayPool({ clock: time.clock, scheduler: time.scheduler })
  try {
    const result = await pool.publish(relay.url, buildEventFixture({ kind: 1 }))
    assertEquals(result.ok, true)
    assertEquals(pool.getConnectedRelayUrls(), [relay.url])

    time.tick(30_000)
    await delay(100)

    assertEquals(pool.getConnectedRelayUrls().length, 0)
    const entry = pool.getRelayPoolState().find((e) => e.url === relay.url)
    assertEquals(entry?.disabledUntil ?? null, null)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("dispose - settles an in-flight publish instead of leaving the promise hanging", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    // Begin a publish before the socket finishes connecting, then dispose synchronously: the event
    // is still queued on open, so the only thing that can settle the promise is dispose itself.
    const pending = pool.publish(relay.url, buildEventFixture({ kind: 1 }))
    pool.dispose()
    const result = await pending
    assertEquals(result.ok, false)
    assertEquals(result.message, "disposed")
  } finally {
    await relay.stop()
  }
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
