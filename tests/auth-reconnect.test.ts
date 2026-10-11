import { assertEquals } from "@std/assert"
import type { NostrEvent } from "@innis/nostr-core"
import { buildEventFixture } from "@innis/nostr-core/testing"
import { createRelayPool } from "../src/infrastructure/web-socket/web-socket-relay-pool.ts"
import { createInMemoryRelay } from "../testing.ts"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

interface AuthParkedReconnect {
  readonly storedId: string
  readonly authCallsBeforeDrop: number
  readonly receivedBeforeDrop: ReadonlyArray<string>
  readonly authCallsAfterReconnect: number
  readonly receivedAfterReconnect: ReadonlyArray<string>
}

const reconnectWhileFirstAuthIsHeldOpen = async (): Promise<AuthParkedReconnect> => {
  const relay = createInMemoryRelay({ requireAuth: true })
  await relay.start()

  let authCallCount = 0
  let releaseFirstAuth: () => void = () => {}
  const firstAuthHeldOpen = new Promise<void>((resolve) => {
    releaseFirstAuth = resolve
  })

  const pool = createRelayPool({
    onAuthChallenge: async (): Promise<NostrEvent> => {
      authCallCount++
      if (authCallCount === 1) await firstAuthHeldOpen
      return buildEventFixture({ kind: 22242 })
    },
  })

  try {
    const stored = buildEventFixture({ kind: 1 })
    relay.inject(stored)

    const received: string[] = []
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (event) => {
        received.push(event.id)
      },
    })

    await delay(200)
    const authCallsBeforeDrop = authCallCount
    const receivedBeforeDrop = [...received]

    relay.dropConnections()
    await delay(900)
    const authCallsAfterReconnect = authCallCount
    const receivedAfterReconnect = [...received]

    releaseFirstAuth()
    await delay(50)

    return {
      storedId: stored.id,
      authCallsBeforeDrop,
      receivedBeforeDrop,
      authCallsAfterReconnect,
      receivedAfterReconnect,
    }
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

Deno.test("auth-parked subscription - the first REQ to a relay requiring AUTH earns exactly one challenge", async () => {
  const { authCallsBeforeDrop } = await reconnectWhileFirstAuthIsHeldOpen()
  assertEquals(authCallsBeforeDrop, 1)
})

Deno.test("auth-parked subscription - receives nothing while the first AUTH is held open", async () => {
  const { receivedBeforeDrop } = await reconnectWhileFirstAuthIsHeldOpen()
  assertEquals(receivedBeforeDrop, [])
})

Deno.test("auth-parked subscription - a reconnect re-issues the parked subscription and earns a fresh challenge", async () => {
  const { authCallsAfterReconnect } = await reconnectWhileFirstAuthIsHeldOpen()
  assertEquals(authCallsAfterReconnect >= 2, true)
})

Deno.test("auth-parked subscription - survives a reconnect and receives the stored event after re-auth", async () => {
  const { storedId, receivedAfterReconnect } = await reconnectWhileFirstAuthIsHeldOpen()
  assertEquals(receivedAfterReconnect, [storedId])
})
