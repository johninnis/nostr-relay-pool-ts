import { assertEquals } from "@std/assert"
import type { NostrEvent } from "@innis/nostr-core"
import { createRelayPool } from "../src/infrastructure/web-socket/web-socket-relay-pool.ts"
import type { RelayPoolConfig } from "../src/application/port/relay-pool-config.ts"
import { event } from "./_helpers/message.ts"
import { startRecordingRelay } from "./_helpers/recording-relay.ts"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))
const DEFAULT_MAX_MESSAGE_BYTES = 262_144

const eventFrameOfBytes = (subId: string, bytes: number): unknown[] => {
  const frameWith = (content: string): unknown[] => ["EVENT", subId, event({ content })]
  const room = bytes - new TextEncoder().encode(JSON.stringify(frameWith(""))).length
  return frameWith("x".repeat(room))
}

const subscriptionIdOf = (received: ReadonlyArray<string>): string => {
  const req = received.map((frame): unknown => JSON.parse(frame)).find((frame) =>
    Array.isArray(frame) && frame[0] === "REQ"
  )
  if (!Array.isArray(req) || typeof req[1] !== "string") throw new Error("expected the pool to have sent a REQ")
  return req[1]
}

const contentLengthsDelivered = async (frameBytes: number, config: RelayPoolConfig = {}): Promise<number[]> => {
  const relay = await startRecordingRelay()
  const pool = createRelayPool({ heartbeatIntervalMs: 0, ...config })
  const delivered: number[] = []
  try {
    pool.subscribe(relay.url, [{ kinds: [1] }], {
      onEvent: (received: NostrEvent) => {
        delivered.push(received.content.length)
      },
    })
    await delay(200)
    const subId = subscriptionIdOf(relay.received)
    relay.sendToAll(eventFrameOfBytes(subId, frameBytes))
    relay.sendToAll(["EVENT", subId, event({ content: "after" })])
    await delay(200)
    assertEquals(pool.getConnectedRelayUrls(), [relay.url])
    return delivered
  } finally {
    pool.dispose()
    await relay.stop()
  }
}

Deno.test("message size - a frame of exactly 256 KiB is delivered by default", async () => {
  assertEquals((await contentLengthsDelivered(DEFAULT_MAX_MESSAGE_BYTES)).length, 2)
})

Deno.test("message size - a frame one byte over 256 KiB is dropped by default and the connection stays open", async () => {
  assertEquals(await contentLengthsDelivered(DEFAULT_MAX_MESSAGE_BYTES + 1), [5])
})

Deno.test("message size - a host-raised maxMessageBytes delivers a frame over 256 KiB", async () => {
  const delivered = await contentLengthsDelivered(DEFAULT_MAX_MESSAGE_BYTES + 1, { maxMessageBytes: 512 * 1024 })
  assertEquals(delivered.length, 2)
})
