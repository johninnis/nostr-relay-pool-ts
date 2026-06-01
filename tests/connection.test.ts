import { assertEquals } from "@std/assert"
import type { NostrEvent, NostrFilter } from "@innis/nostr-core"
import { parseRelayUrl } from "@innis/nostr-core"
import { buildEventFixture } from "@innis/nostr-core/testing"
import { createInMemoryRelay } from "../testing.ts"
import type { ConnectionPool } from "../src/application/port/connection-pool.ts"
import { systemScheduler } from "../src/infrastructure/adapter/system-scheduler-adapter.ts"
import type { RelaySubscribeCallbacks } from "../src/domain/value-object/subscription.ts"
import { createRelayConnection } from "../src/application/service/relay-connection.ts"
import { createRelayPool } from "../src/infrastructure/adapter/web-socket-relay-pool-adapter.ts"
import { createManualTime } from "./_helpers/scheduler.ts"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))
const HARD_TIMEOUT_MS = 12_000

Deno.test("createRelayConnection receives events and calls onRelayEose on EOSE", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const event = buildEventFixture({ kind: 1 })
    relay.inject(event)

    const received: NostrEvent[] = []
    let completed = false

    createRelayConnection({
      pool,
      scheduler: createManualTime().scheduler,
      url: relay.url,
      filters: [{ kinds: [1] }],
      hardTimeoutMs: HARD_TIMEOUT_MS,
      callbacks: {
        onEvent: (e) => {
          received.push(e)
        },
        onRelayEose: () => {
          completed = true
        },
      },
    })

    await delay(300)

    assertEquals(received.length, 1)
    const [first] = received
    if (!first) throw new Error("expected one received event")
    assertEquals(first.id, event.id)
    assertEquals(completed, true)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("createRelayConnection fires onRelayEose at soft timeout when EOSE never arrives", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  const time = createManualTime()
  try {
    let completedCount = 0

    const originalSubscribe = pool.subscribe.bind(pool)
    const silentPool: ConnectionPool = {
      suggestedTimeout: pool.suggestedTimeout,
      // Drops onEose/onClosed to simulate a relay that never signals end-of-stream.
      subscribe: (
        url: string,
        filters: ReadonlyArray<NostrFilter>,
        callbacks: RelaySubscribeCallbacks,
      ) => originalSubscribe(url, filters, { onEvent: callbacks.onEvent }),
    }

    createRelayConnection({
      pool: silentPool,
      scheduler: time.scheduler,
      url: relay.url,
      filters: [{ kinds: [99999] }],
      hardTimeoutMs: HARD_TIMEOUT_MS,
      callbacks: {
        onEvent: () => {},
        onRelayEose: () => {
          completedCount++
        },
      },
    })

    // The soft timeout is the default suggested timeout (no latency samples yet).
    time.tick(4000)
    assertEquals(completedCount, 1)

    // Advancing past the hard timeout must not fire completion a second time.
    time.tick(HARD_TIMEOUT_MS)
    assertEquals(completedCount, 1)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("createRelayConnection is idempotent: late EOSE after soft timeout does not double-fire", async () => {
  const relay = createInMemoryRelay({ eoseDelayMs: 400 })
  await relay.start()
  const pool = createRelayPool({ latency: { defaultTimeoutMs: 200, maxTimeoutMs: 500 } })
  const time = createManualTime()
  try {
    let completedCount = 0

    createRelayConnection({
      pool,
      scheduler: time.scheduler,
      url: relay.url,
      filters: [{ kinds: [1] }],
      hardTimeoutMs: HARD_TIMEOUT_MS,
      callbacks: {
        onEvent: () => {},
        onRelayEose: () => {
          completedCount++
        },
      },
    })

    time.tick(200)
    assertEquals(completedCount, 1, "soft timeout should have fired onRelayEose once")

    await delay(600)
    assertEquals(completedCount, 1, "late EOSE must not fire onRelayEose a second time")
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("createRelayConnection records a latency sample on EOSE", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    createRelayConnection({
      pool,
      scheduler: createManualTime().scheduler,
      url: relay.url,
      filters: [{ kinds: [1] }],
      hardTimeoutMs: HARD_TIMEOUT_MS,
      callbacks: {
        onEvent: () => {},
        onRelayEose: () => {},
      },
    })

    await delay(300)

    const suggested = pool.suggestedTimeout(relay.url)
    assertEquals(suggested, 4000, "single sample should not yet adapt (below minSamples)")
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("createRelayConnection persistent mode streams events after EOSE", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const received: NostrEvent[] = []
    let completed = false

    const first = buildEventFixture({ kind: 1 })
    relay.inject(first)

    createRelayConnection({
      pool,
      scheduler: createManualTime().scheduler,
      url: relay.url,
      filters: [{ kinds: [1] }],
      hardTimeoutMs: HARD_TIMEOUT_MS,
      persistent: true,
      callbacks: {
        onEvent: (e) => {
          received.push(e)
        },
        onRelayEose: () => {
          completed = true
        },
      },
    })

    await delay(300)
    assertEquals(completed, true)
    assertEquals(received.length, 1)

    const second = buildEventFixture({ kind: 1 })
    relay.inject(second)
    await delay(300)

    assertEquals(received.length, 2)
    const last = received[1]
    if (!last) throw new Error("expected a second received event")
    assertEquals(last.id, second.id)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("createRelayConnection persistent close stops stream", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const received: NostrEvent[] = []

    const handle = createRelayConnection({
      pool,
      scheduler: createManualTime().scheduler,
      url: relay.url,
      filters: [{ kinds: [1] }],
      hardTimeoutMs: HARD_TIMEOUT_MS,
      persistent: true,
      callbacks: {
        onEvent: (e) => {
          received.push(e)
        },
        onRelayEose: () => {},
      },
    })

    await delay(200)
    handle.unsubscribe()

    relay.inject(buildEventFixture({ kind: 1 }))
    await delay(200)

    assertEquals(received.length, 0)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("createRelayConnection: a relay CLOSED surfaces onRelayClosed and tears down even when persistent", () => {
  const captured: RelaySubscribeCallbacks[] = []
  let unsubscribed = false
  const stubPool: ConnectionPool = {
    suggestedTimeout: () => 4000,
    subscribe: (_url, _filters, callbacks) => {
      captured.push(callbacks)
      return {
        active: true,
        unsubscribe: () => {
          unsubscribed = true
        },
      }
    },
  }

  const closedReasons: string[] = []
  let eoseCount = 0

  createRelayConnection({
    pool: stubPool,
    scheduler: systemScheduler,
    url: parseRelayUrl("wss://relay.example.com"),
    filters: [{ kinds: [1] }],
    hardTimeoutMs: HARD_TIMEOUT_MS,
    persistent: true,
    callbacks: {
      onEvent: () => {},
      onRelayEose: () => {
        eoseCount++
      },
      onRelayClosed: (_url, reason) => {
        closedReasons.push(reason)
      },
    },
  })

  const callbacks = captured[0]
  if (!callbacks) throw new Error("subscribe was not called")
  callbacks.onClosed?.("rate-limited: slow down")

  assertEquals(closedReasons, ["rate-limited: slow down"])
  // The leg completes (onRelayEose fires once for completion tracking) and the persistent
  // connection is torn down because the subscription is dead, not merely past its backlog.
  assertEquals(eoseCount, 1)
  assertEquals(unsubscribed, true)
})

Deno.test("createRelayConnection close stops receiving events", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const received: NostrEvent[] = []

    const handle = createRelayConnection({
      pool,
      scheduler: createManualTime().scheduler,
      url: relay.url,
      filters: [{ kinds: [1] }],
      hardTimeoutMs: HARD_TIMEOUT_MS,
      callbacks: {
        onEvent: (e) => {
          received.push(e)
        },
        onRelayEose: () => {},
      },
    })

    await delay(200)
    handle.unsubscribe()

    relay.inject(buildEventFixture({ kind: 1 }))
    await delay(200)

    assertEquals(received.length, 0)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})
