import { assertEquals } from "@std/assert"
import type { TimerHandle } from "../src/application/port/scheduler.ts"
import { closeIntentionally, stopConnectionTimers } from "../src/infrastructure/web-socket/relay-state.ts"
import { createInMemoryRelay } from "../testing.ts"
import { stubRelayState } from "./_helpers/relay-state.ts"
import { createManualTime } from "./_helpers/scheduler.ts"

const closed = (ws: WebSocket): Promise<void> =>
  new Promise((resolve) => ws.addEventListener("close", () => resolve(), { once: true }))

const opened = (ws: WebSocket): Promise<void> =>
  new Promise((resolve) => ws.addEventListener("open", () => resolve(), { once: true }))

const readyStateOfOpenSocketAfter = async (act: (ws: WebSocket) => Promise<void>): Promise<number> => {
  const relay = createInMemoryRelay()
  await relay.start()
  try {
    const ws = new WebSocket(relay.url)
    await opened(ws)
    await act(ws)
    return ws.readyState
  } finally {
    await relay.stop()
  }
}

Deno.test("closeIntentionally - leaves a real socket that has already closed closed", async () => {
  const readyState = await readyStateOfOpenSocketAfter(async (ws) => {
    ws.close()
    await closed(ws)
    closeIntentionally(stubRelayState({ ws }))
  })
  assertEquals(readyState, WebSocket.CLOSED)
})

Deno.test("closeIntentionally - a second close while a real socket is closing still ends closed", async () => {
  const readyState = await readyStateOfOpenSocketAfter(async (ws) => {
    const socketClosed = closed(ws)
    const state = stubRelayState({ ws })
    closeIntentionally(state)
    closeIntentionally(state)
    await socketClosed
  })
  assertEquals(readyState, WebSocket.CLOSED)
})

Deno.test("stopConnectionTimers - leaves none of the stability, idle and heartbeat timers pending", () => {
  const time = createManualTime()
  const armed = (): TimerHandle => time.scheduler.setTimer(() => {}, 1_000)
  const state = stubRelayState({ stabilityTimer: armed(), idleTimer: armed(), heartbeatTimer: armed() })
  stopConnectionTimers(state, time.scheduler)
  assertEquals(time.pendingCount(), 0)
})
