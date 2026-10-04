import { assertEquals } from "@std/assert"
import { createRelayPool } from "../src/infrastructure/web-socket/web-socket-relay-pool.ts"
import { createManualTime } from "./_helpers/scheduler.ts"
import { startRecordingRelay } from "./_helpers/recording-relay.ts"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))
const KEEPALIVE = JSON.stringify(["CLOSE", "keepalive"])

Deno.test("heartbeat - sends a keepalive CLOSE every interval while the socket is open", async () => {
  const relay = await startRecordingRelay()
  const time = createManualTime()
  const pool = createRelayPool({ clock: time.clock, scheduler: time.scheduler })
  try {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    time.tick(29_999)
    await delay(50)
    assertEquals(relay.received.filter((frame) => frame === KEEPALIVE).length, 0)

    time.tick(1)
    await delay(50)
    assertEquals(relay.received.filter((frame) => frame === KEEPALIVE).length, 1)

    time.tick(30_000)
    await delay(50)
    assertEquals(relay.received.filter((frame) => frame === KEEPALIVE).length, 2)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("heartbeat - honours a configured interval", async () => {
  const relay = await startRecordingRelay()
  const time = createManualTime()
  const pool = createRelayPool({ clock: time.clock, scheduler: time.scheduler, heartbeatIntervalMs: 5_000 })
  try {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    time.tick(5_000)
    await delay(50)
    assertEquals(relay.received.filter((frame) => frame === KEEPALIVE).length, 1)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("heartbeat - an interval of zero disables it", async () => {
  const relay = await startRecordingRelay()
  const time = createManualTime()
  const pool = createRelayPool({ clock: time.clock, scheduler: time.scheduler, heartbeatIntervalMs: 0 })
  try {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    time.tick(120_000)
    await delay(50)
    assertEquals(relay.received.filter((frame) => frame === KEEPALIVE).length, 0)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("heartbeat - stops once the socket closes", async () => {
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
    assertEquals(relay.received.filter((frame) => frame === KEEPALIVE).length, 0)
    assertEquals(time.pendingCount(), 0)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})

Deno.test("ping NOTICE - the pool answers a relay's ping NOTICE with a keepalive CLOSE", async () => {
  const relay = await startRecordingRelay()
  const pool = createRelayPool({ heartbeatIntervalMs: 0 })
  try {
    pool.subscribe(relay.url, [{ kinds: [1] }], { onEvent: () => {} })
    await delay(200)
    relay.sendToAll(["NOTICE", "ping"])
    await delay(100)
    assertEquals(relay.received.filter((frame) => frame === KEEPALIVE).length, 1)
  } finally {
    pool.dispose()
    await relay.stop()
  }
})
