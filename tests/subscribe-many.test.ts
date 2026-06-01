import { assertEquals } from "@std/assert"
import type { ConnectionPool } from "../src/application/port/connection-pool.ts"
import type { RelaySubscribeCallbacks, Subscription } from "../src/domain/value-object/subscription.ts"
import { createSubscribeMany } from "../src/application/service/subscribe-many.ts"
import { createManualTime } from "./_helpers/scheduler.ts"

const URL = "wss://relay.example.com"

interface CapturingPool {
  readonly pool: ConnectionPool
  readonly subscribeCount: () => number
  readonly lastCallbacks: () => RelaySubscribeCallbacks | undefined
}

const capturingPool = (): CapturingPool => {
  const captured: RelaySubscribeCallbacks[] = []
  const pool: ConnectionPool = {
    suggestedTimeout: () => 4000,
    subscribe: (_url, _filters, callbacks): Subscription => {
      captured.push(callbacks)
      return { active: true, unsubscribe: () => {} }
    },
  }
  return {
    pool,
    subscribeCount: () => captured.length,
    lastCallbacks: () => captured[captured.length - 1],
  }
}

Deno.test("subscribeMany - a relay CLOSED drops the handle so syncUrls reopens the relay", () => {
  const time = createManualTime()
  const capturing = capturingPool()
  const subscribeMany = createSubscribeMany({
    connectionPool: capturing.pool,
    scheduler: time.scheduler,
    hardTimeoutMs: 12_000,
  })

  const closedReasons: string[] = []
  const handle = subscribeMany([URL], [{ kinds: [1] }], {
    onEvent: () => {},
    onRelayClosed: (_url, reason) => closedReasons.push(reason),
  }, { persistent: true })

  assertEquals(capturing.subscribeCount(), 1)

  // The relay terminates the subscription (e.g. rate-limited). The leg tears itself down.
  capturing.lastCallbacks()?.onClosed?.("rate-limited: slow down")
  assertEquals(closedReasons, ["rate-limited: slow down"])

  // Re-listing the same relay must reopen it rather than treat the dead connection as live.
  handle.syncUrls([URL])
  assertEquals(capturing.subscribeCount(), 2)

  handle.unsubscribe()
})

Deno.test("subscribeMany - a non-persistent relay EOSE drops the handle so syncUrls reopens the relay", () => {
  const time = createManualTime()
  const capturing = capturingPool()
  const subscribeMany = createSubscribeMany({
    connectionPool: capturing.pool,
    scheduler: time.scheduler,
    hardTimeoutMs: 12_000,
  })

  // Default (non-persistent): each leg closes at EOSE once the stored backlog drains.
  const handle = subscribeMany([URL], [{ kinds: [1] }], { onEvent: () => {}, onRelayEose: () => {} })
  assertEquals(capturing.subscribeCount(), 1)

  // The relay signals end-of-stored-events; the leg completes and tears itself down.
  capturing.lastCallbacks()?.onEose?.()

  // Re-listing the same relay must reopen it rather than skip it as still-live.
  handle.syncUrls([URL])
  assertEquals(capturing.subscribeCount(), 2)

  handle.unsubscribe()
})

Deno.test("subscribeMany - syncUrls is a no-op for an unchanged, still-live relay", () => {
  const time = createManualTime()
  const capturing = capturingPool()
  const subscribeMany = createSubscribeMany({
    connectionPool: capturing.pool,
    scheduler: time.scheduler,
    hardTimeoutMs: 12_000,
  })

  const handle = subscribeMany([URL], [{ kinds: [1] }], { onEvent: () => {} }, { persistent: true })
  assertEquals(capturing.subscribeCount(), 1)

  handle.syncUrls([URL])
  assertEquals(capturing.subscribeCount(), 1, "a live relay should not be reopened")

  handle.unsubscribe()
})
