import { assertEquals } from "@std/assert"
import type { RelayPoolConfig } from "../src/application/port/relay-pool-config.ts"
import { createRelayPool } from "../src/infrastructure/web-socket/web-socket-relay-pool.ts"
import { createManualTime } from "./_helpers/scheduler.ts"
import { startRecordingRelay } from "./_helpers/recording-relay.ts"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))
const KEEPALIVE = JSON.stringify(["CLOSE", "keepalive"])

const keepalivesIn = (received: ReadonlyArray<string>): number => received.filter((frame) => frame === KEEPALIVE).length

const keepalivesAfter = async (
  ticks: ReadonlyArray<number>,
  config: Pick<RelayPoolConfig, "heartbeatIntervalMs"> = {},
): Promise<number> => {
  const relay = await startRecordingRelay()
  const time = createManualTime()
  const pool = createRelayPool({ clock: time.clock, scheduler: time.scheduler, ...config })
  try {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    for (const ms of ticks) {
      time.tick(ms)
      await delay(50)
    }
    return keepalivesIn(relay.received)
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

const afterDisconnect = async () => {
  const relay = await startRecordingRelay()
  const time = createManualTime()
  const pool = createRelayPool({ clock: time.clock, scheduler: time.scheduler })
  try {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    pool.disconnect(relay.url)
    await delay(100)
    time.tick(30_000)
    await delay(50)
    return { keepalives: keepalivesIn(relay.received), pendingTimers: time.pendingCount() }
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

Deno.test("heartbeat - sends no keepalive before the first interval has passed", async () => {
  assertEquals(await keepalivesAfter([29_999]), 0)
})

Deno.test("heartbeat - sends a keepalive CLOSE once the interval has passed", async () => {
  assertEquals(await keepalivesAfter([29_999, 1]), 1)
})

Deno.test("heartbeat - sends a keepalive CLOSE every interval while the socket is open", async () => {
  assertEquals(await keepalivesAfter([29_999, 1, 30_000]), 2)
})

Deno.test("heartbeat - honours a configured interval", async () => {
  assertEquals(await keepalivesAfter([5_000], { heartbeatIntervalMs: 5_000 }), 1)
})

Deno.test("heartbeat - an interval of zero disables it", async () => {
  assertEquals(await keepalivesAfter([120_000], { heartbeatIntervalMs: 0 }), 0)
})

Deno.test("heartbeat - stops once the socket closes", async () => {
  const { keepalives } = await afterDisconnect()
  assertEquals(keepalives, 0)
})

Deno.test("heartbeat - leaves no timer pending once the socket closes", async () => {
  const { pendingTimers } = await afterDisconnect()
  assertEquals(pendingTimers, 0)
})

Deno.test("ping NOTICE - the pool answers a relay's ping NOTICE with a keepalive CLOSE", async () => {
  const relay = await startRecordingRelay()
  const pool = createRelayPool({ heartbeatIntervalMs: 0 })
  try {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    relay.sendToAll(["NOTICE", "ping"])
    await delay(100)
    assertEquals(keepalivesIn(relay.received), 1)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})
