import { assertEquals } from "@std/assert"
import type { NostrEvent, NostrFilter } from "@innis/nostr-core"
import { buildEventFixture, relayUrlFixture } from "@innis/nostr-core/testing"
import { createInMemoryRelay } from "../testing.ts"
import type { ConnectionPool } from "../src/application/port/connection-pool.ts"
import { systemScheduler } from "../src/infrastructure/time/system-scheduler.ts"
import type { RelaySubscribeCallbacks } from "../src/domain/value-object/subscription.ts"
import { createRelayConnection } from "../src/application/service/relay-connection.ts"
import { createRelayPool } from "../src/infrastructure/web-socket/web-socket-relay-pool.ts"
import { createManualTime } from "./_helpers/scheduler.ts"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))
const HARD_TIMEOUT_MS = 12_000
const CLOCK_STEP_MS = 1_000

const fetchOnce = async (stored: NostrEvent) => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    relay.inject(stored)
    const receivedIds: Array<string> = []
    let completed = false
    createRelayConnection({
      pool,
      scheduler: createManualTime().scheduler,
      url: relay.url,
      filters: [{ kinds: [1] }],
      hardTimeoutMs: HARD_TIMEOUT_MS,
      callbacks: {
        onEvent: (e) => {
          receivedIds.push(e.id)
        },
        onRelayEose: () => {
          completed = true
        },
      },
    })
    await delay(300)
    return { receivedIds, completed }
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

const completionsWithoutEose = async (afterSoftTimeoutMs: number): Promise<number> => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  const time = createManualTime()
  try {
    let completedCount = 0
    const originalSubscribe = pool.subscribe.bind(pool)
    const silentPool: ConnectionPool = {
      suggestedTimeout: pool.suggestedTimeout,
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
    time.tick(silentPool.suggestedTimeout(relay.url))
    time.tick(afterSoftTimeoutMs)
    return completedCount
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

const completionsAroundDelayedEose = async (waitAfterSoftTimeoutMs: number): Promise<number> => {
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
    await delay(waitAfterSoftTimeoutMs)
    return completedCount
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

const persistentStream = async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    const receivedIds: Array<string> = []
    let completed = false
    const stored = buildEventFixture({ kind: 1 })
    relay.inject(stored)
    createRelayConnection({
      pool,
      scheduler: createManualTime().scheduler,
      url: relay.url,
      filters: [{ kinds: [1] }],
      hardTimeoutMs: HARD_TIMEOUT_MS,
      persistent: true,
      callbacks: {
        onEvent: (e) => {
          receivedIds.push(e.id)
        },
        onRelayEose: () => {
          completed = true
        },
      },
    })
    await delay(300)
    const completedByEose = completed
    const receivedIdsByEose = [...receivedIds]
    const live = buildEventFixture({ kind: 1 })
    relay.inject(live)
    await delay(300)
    return { storedId: stored.id, liveId: live.id, completedByEose, receivedIdsByEose, receivedIds: [...receivedIds] }
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

const closedByRelay = () => {
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
    url: relayUrlFixture("wss://relay.example.com"),
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
  return { closedReasons, eoseCount, unsubscribed }
}

const receivedAfterUnsubscribe = async (persistent: boolean): Promise<number> => {
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
      persistent,
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
    return received.length
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

Deno.test("createRelayConnection delivers the stored events its filter matches", async () => {
  const stored = buildEventFixture({ kind: 1 })
  const { receivedIds } = await fetchOnce(stored)
  assertEquals(receivedIds, [stored.id])
})

Deno.test("createRelayConnection calls onRelayEose on EOSE", async () => {
  const { completed } = await fetchOnce(buildEventFixture({ kind: 1 }))
  assertEquals(completed, true)
})

Deno.test("createRelayConnection fires onRelayEose at soft timeout when EOSE never arrives", async () => {
  assertEquals(await completionsWithoutEose(0), 1)
})

Deno.test("createRelayConnection does not fire onRelayEose again when the hard timeout follows the soft timeout", async () => {
  assertEquals(await completionsWithoutEose(HARD_TIMEOUT_MS), 1)
})

Deno.test("createRelayConnection fires onRelayEose at the soft timeout before a delayed EOSE arrives", async () => {
  assertEquals(await completionsAroundDelayedEose(0), 1)
})

Deno.test("createRelayConnection is idempotent: late EOSE after soft timeout does not double-fire", async () => {
  assertEquals(await completionsAroundDelayedEose(600), 1)
})

Deno.test("createRelayConnection records a latency sample on EOSE", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  let clockMs = 0
  const pool = createRelayPool({
    clock: () => (clockMs += CLOCK_STEP_MS),
    latency: { minSamples: 1, defaultTimeoutMs: 1, multiplier: 1, maxTimeoutMs: Number.MAX_SAFE_INTEGER },
  })
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

    assertEquals(pool.suggestedTimeout(relay.url) >= CLOCK_STEP_MS, true)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("createRelayConnection persistent mode calls onRelayEose on EOSE", async () => {
  const { completedByEose } = await persistentStream()
  assertEquals(completedByEose, true)
})

Deno.test("createRelayConnection persistent mode delivers the stored events by EOSE", async () => {
  const { storedId, receivedIdsByEose } = await persistentStream()
  assertEquals(receivedIdsByEose, [storedId])
})

Deno.test("createRelayConnection persistent mode streams events after EOSE", async () => {
  const { storedId, liveId, receivedIds } = await persistentStream()
  assertEquals(receivedIds, [storedId, liveId])
})

Deno.test("createRelayConnection persistent close stops stream", async () => {
  assertEquals(await receivedAfterUnsubscribe(true), 0)
})

Deno.test("createRelayConnection: a relay CLOSED surfaces onRelayClosed with the relay's reason", () => {
  assertEquals(closedByRelay().closedReasons, ["rate-limited: slow down"])
})

Deno.test("createRelayConnection: a relay CLOSED completes the leg through onRelayEose even when persistent", () => {
  assertEquals(closedByRelay().eoseCount, 1)
})

Deno.test("createRelayConnection: a relay CLOSED tears the leg down even when persistent", () => {
  assertEquals(closedByRelay().unsubscribed, true)
})

Deno.test("createRelayConnection close stops receiving events", async () => {
  assertEquals(await receivedAfterUnsubscribe(false), 0)
})
