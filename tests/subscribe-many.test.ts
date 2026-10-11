import { assertEquals } from "@std/assert"
import type { ConnectionPool } from "../src/application/port/connection-pool.ts"
import type {
  PoolSubscription,
  RelaySubscribeCallbacks,
  SubscribeManyCallbacks,
  Subscription,
} from "../src/domain/value-object/subscription.ts"
import {
  createSubscribeMany,
  createSubscribeManyLive,
  type SubscribeManyDeps,
} from "../src/application/service/subscribe-many.ts"
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

const fanOutDeps = (capturing: CapturingPool): SubscribeManyDeps => ({
  connectionPool: capturing.pool,
  scheduler: createManualTime().scheduler,
  hardTimeoutMs: 12_000,
})

const liveFanOut = (
  capturing: CapturingPool,
  callbacks: SubscribeManyCallbacks = { onEvent: () => {} },
): PoolSubscription => createSubscribeManyLive(fanOutDeps(capturing))([URL], [{ kinds: [1] }], callbacks)

const oneShotFanOut = (capturing: CapturingPool): PoolSubscription =>
  createSubscribeMany(fanOutDeps(capturing))([URL], [{ kinds: [1] }], { onEvent: () => {}, onRelayEose: () => {} })

Deno.test("subscribeManyLive - opens one leg per relay", () => {
  const capturing = capturingPool()
  liveFanOut(capturing).unsubscribe()
  assertEquals(capturing.subscribeCount(), 1)
})

Deno.test("subscribeManyLive - a relay CLOSED reaches onRelayClosed with its reason", () => {
  const capturing = capturingPool()
  const closedReasons: string[] = []
  const handle = liveFanOut(capturing, {
    onEvent: () => {},
    onRelayClosed: (_url, reason) => closedReasons.push(reason),
  })
  capturing.lastCallbacks()?.onClosed?.("rate-limited: slow down")
  handle.unsubscribe()
  assertEquals(closedReasons, ["rate-limited: slow down"])
})

Deno.test("subscribeManyLive - a relay CLOSED drops the handle so syncUrls reopens the relay", () => {
  const capturing = capturingPool()
  const handle = liveFanOut(capturing)
  capturing.lastCallbacks()?.onClosed?.("rate-limited: slow down")
  handle.syncUrls([URL])
  handle.unsubscribe()
  assertEquals(capturing.subscribeCount(), 2)
})

Deno.test("subscribeMany - opens one leg per relay", () => {
  const capturing = capturingPool()
  oneShotFanOut(capturing).unsubscribe()
  assertEquals(capturing.subscribeCount(), 1)
})

Deno.test("subscribeMany - a relay EOSE drops the handle so syncUrls reopens the relay", () => {
  const capturing = capturingPool()
  const handle = oneShotFanOut(capturing)
  capturing.lastCallbacks()?.onEose?.()
  handle.syncUrls([URL])
  handle.unsubscribe()
  assertEquals(capturing.subscribeCount(), 2)
})

Deno.test("subscribeManyLive - syncUrls is a no-op for an unchanged, still-live relay", () => {
  const capturing = capturingPool()
  const handle = liveFanOut(capturing)
  handle.syncUrls([URL])
  handle.unsubscribe()
  assertEquals(capturing.subscribeCount(), 1, "a live relay should not be reopened")
})

Deno.test("subscribeManyLive - a relay EOSE keeps the handle so syncUrls does not reopen the relay", () => {
  const capturing = capturingPool()
  const handle = liveFanOut(capturing)
  capturing.lastCallbacks()?.onEose?.()
  handle.syncUrls([URL])
  handle.unsubscribe()
  assertEquals(capturing.subscribeCount(), 1, "a live leg stays open past EOSE")
})
