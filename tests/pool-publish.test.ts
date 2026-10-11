import { assertEquals } from "@std/assert"
import type { NostrEvent } from "@innis/nostr-core"
import { buildEventFixture } from "@innis/nostr-core/testing"
import type { RelayPool } from "../src/application/port/relay-pool.ts"
import type { PublishResponse } from "../src/domain/value-object/publish-history.ts"
import { createRelayPool } from "../src/infrastructure/web-socket/web-socket-relay-pool.ts"
import { createInMemoryRelay, type InMemoryRelay } from "../testing.ts"

const withPool = async <T>(act: (relay: InMemoryRelay, pool: RelayPool) => Promise<T>): Promise<T> => {
  const relay = createInMemoryRelay()
  await relay.start()
  const pool = createRelayPool()
  try {
    return await act(relay, pool)
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

interface AcceptedPublish {
  readonly response: PublishResponse
  readonly storedIds: ReadonlyArray<string>
}

const publishToAnAcceptingRelay = (event: NostrEvent): Promise<AcceptedPublish> =>
  withPool(async (relay, pool) => {
    const response = await pool.publish(relay.url, event)
    return { response, storedIds: relay.getStoredEvents().map((stored) => stored.id) }
  })

Deno.test("publish sends event and receives OK response", async () => {
  const { response } = await publishToAnAcceptingRelay(buildEventFixture({ kind: 1 }))
  assertEquals(response.ok, true)
})

Deno.test("publish stores the event on the relay", async () => {
  const event = buildEventFixture({ kind: 1 })
  const { storedIds } = await publishToAnAcceptingRelay(event)
  assertEquals(storedIds, [event.id])
})

const publishThenDisconnectBeforeTheAck = (): Promise<PublishResponse> =>
  withPool((relay, pool) => {
    const pending = pool.publish(relay.url, buildEventFixture({ kind: 1 }))
    pool.disconnect(relay.url)
    return pending
  })

Deno.test("publish is not ok when the socket is closed before it is acked", async () => {
  const { ok } = await publishThenDisconnectBeforeTheAck()
  assertEquals(ok, false)
})

Deno.test("publish settles as disconnected when the socket is closed before it is acked", async () => {
  const { message } = await publishThenDisconnectBeforeTheAck()
  assertEquals(message, "disconnected")
})
