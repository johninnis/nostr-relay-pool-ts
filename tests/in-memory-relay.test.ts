import { assertEquals, assertGreater } from "@std/assert"
import type { NostrEvent } from "@innis/nostr-core"
import { createInMemoryRelay, type InMemoryRelay, type InMemoryRelayOptions } from "../testing.ts"
import { buildEventFixture } from "@innis/nostr-core/testing"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

type Frame = unknown[]

const connectWs = (url: string): Promise<WebSocket> =>
  new Promise((resolve) => {
    const ws = new WebSocket(url)
    ws.onopen = (): void => resolve(ws)
  })

const collectMessages = (ws: WebSocket): Array<Frame> => {
  const messages: Array<Frame> = []
  ws.onmessage = (msg: MessageEvent): void => {
    const data: unknown = msg.data
    if (typeof data !== "string") return
    const parsed: unknown = JSON.parse(data)
    if (Array.isArray(parsed)) messages.push(parsed)
  }
  return messages
}

const hasStringContent = (value: unknown): value is { readonly content: string } => {
  if (typeof value !== "object" || value === null || !("content" in value)) return false
  const { content } = value
  return typeof content === "string"
}

const isEventFrame = (frame: Frame): boolean => frame[0] === "EVENT"

const isEoseFrame = (frame: Frame): boolean => frame[0] === "EOSE"

const eventContentsOf = (frames: ReadonlyArray<Frame>): ReadonlyArray<string | null> =>
  frames.filter(isEventFrame).map(([, , event]) => hasStringContent(event) ? event.content : null)

const withRelay = async <T>(
  options: InMemoryRelayOptions,
  run: (relay: InMemoryRelay) => Promise<T> | T,
): Promise<T> => {
  const relay = createInMemoryRelay(options)
  await relay.start()
  try {
    return await run(relay)
  } finally {
    await relay.stop()
  }
}

const withClient = async <T>(
  relay: InMemoryRelay,
  run: (ws: WebSocket, frames: ReadonlyArray<Frame>) => Promise<T>,
): Promise<T> => {
  const ws = await connectWs(relay.url)
  const frames = collectMessages(ws)
  try {
    return await run(ws, frames)
  } finally {
    ws.close()
    await delay(50)
  }
}

const publishOverSocket = (event: NostrEvent) =>
  withRelay({}, (relay) =>
    withClient(relay, async (ws, frames) => {
      ws.send(JSON.stringify(["EVENT", event]))
      await delay(100)
      return { replies: [...frames], storedContents: relay.getStoredEvents().map((stored) => stored.content) }
    }))

const framesAnsweringReq = (
  stored: ReadonlyArray<NostrEvent>,
  filter: Readonly<Record<string, unknown>>,
): Promise<ReadonlyArray<Frame>> =>
  withRelay({}, (relay) => {
    for (const event of stored) relay.inject(event)
    return withClient(relay, async (ws, frames) => {
      ws.send(JSON.stringify(["REQ", "sub1", filter]))
      await delay(100)
      return [...frames]
    })
  })

const framesAfterLiveInject = (event: NostrEvent): Promise<ReadonlyArray<Frame>> =>
  withRelay({}, (relay) =>
    withClient(relay, async (ws, frames) => {
      ws.send(JSON.stringify(["REQ", "live", { kinds: [1] }]))
      await delay(50)
      relay.inject(event)
      await delay(100)
      return [...frames]
    }))

const authExchange = (auth: NostrEvent) =>
  withRelay({ requireAuth: true }, (relay) =>
    withClient(relay, async (ws, frames) => {
      ws.send(JSON.stringify(["REQ", "sub1", { kinds: [1] }]))
      await delay(50)
      const refusal = frames.slice(0)
      ws.send(JSON.stringify(["AUTH", auth]))
      await delay(50)
      const authReply = frames.slice(refusal.length)
      ws.send(JSON.stringify(["REQ", "sub1", { kinds: [1] }]))
      await delay(50)
      const served = frames.slice(refusal.length + authReply.length)
      return { refusal, authReply, served }
    }))

Deno.test("in-memory relay - starts and stops", async () => {
  const relay = createInMemoryRelay()
  await relay.start()
  assertGreater(relay.port, 0)
  await relay.stop()
})

Deno.test("in-memory relay - accepts WebSocket connections", async () => {
  const relay = createInMemoryRelay()
  await relay.start()

  const ws = await connectWs(relay.url)
  await delay(50)
  assertEquals(relay.getConnectionCount(), 1)

  ws.close()
  await delay(50)
  await relay.stop()
})

Deno.test("in-memory relay - answers a published event with OK", async () => {
  const event = buildEventFixture({ content: "hello relay" })
  const { replies } = await publishOverSocket(event)
  assertEquals(replies, [["OK", event.id, true, ""]])
})

Deno.test("in-memory relay - stores a published event", async () => {
  const { storedContents } = await publishOverSocket(buildEventFixture({ content: "hello relay" }))
  assertEquals(storedContents, ["hello relay"])
})

Deno.test("in-memory relay - REQ returns the stored events its filter matches", async () => {
  const frames = await framesAnsweringReq(
    [buildEventFixture({ kind: 1, content: "note 1" }), buildEventFixture({ kind: 0, content: "profile" })],
    { kinds: [1] },
  )
  assertEquals(eventContentsOf(frames), ["note 1"])
})

Deno.test("in-memory relay - REQ ends its stored events with an EOSE for its subscription", async () => {
  const frames = await framesAnsweringReq(
    [buildEventFixture({ kind: 1, content: "note 1" }), buildEventFixture({ kind: 0, content: "profile" })],
    { kinds: [1] },
  )
  assertEquals(frames.filter(isEoseFrame), [["EOSE", "sub1"]])
})

Deno.test("in-memory relay - REQ with no stored match is answered with EOSE alone", async () => {
  const frames = await framesAnsweringReq([], { kinds: [1] })
  assertEquals(frames, [["EOSE", "sub1"]])
})

Deno.test("in-memory relay - REQ filters by authors", async () => {
  const pubkeyA = "a".repeat(64)
  const frames = await framesAnsweringReq(
    [
      buildEventFixture({ pubkey: pubkeyA, content: "from A" }),
      buildEventFixture({ pubkey: "b".repeat(64), content: "from B" }),
    ],
    { authors: [pubkeyA] },
  )
  assertEquals(eventContentsOf(frames), ["from A"])
})

Deno.test("in-memory relay - live subscription receives new events", async () => {
  const frames = await framesAfterLiveInject(buildEventFixture({ kind: 1, content: "live!" }))
  assertEquals(eventContentsOf(frames), ["live!"])
})

Deno.test("in-memory relay - CLOSE unsubscribes", async () => {
  const frames = await withRelay({}, (relay) =>
    withClient(relay, async (ws, frames) => {
      ws.send(JSON.stringify(["REQ", "sub1", { kinds: [1] }]))
      await delay(50)
      ws.send(JSON.stringify(["CLOSE", "sub1"]))
      await delay(50)
      relay.inject(buildEventFixture({ kind: 1, content: "after close" }))
      await delay(100)
      return [...frames]
    }))
  assertEquals(frames.filter(isEventFrame), [])
})

Deno.test("in-memory relay - tag filter matching", async () => {
  const targetId = "d".repeat(64)
  const frames = await framesAnsweringReq(
    [buildEventFixture({ tags: [["e", targetId]], content: "reply" }), buildEventFixture({ content: "unrelated" })],
    { "#e": [targetId] },
  )
  assertEquals(eventContentsOf(frames), ["reply"])
})

Deno.test("in-memory relay - inject stores each event", async () => {
  const storedCount = await withRelay({}, (relay) => {
    relay.inject(buildEventFixture({ content: "one" }))
    relay.inject(buildEventFixture({ content: "two" }))
    return relay.getStoredEvents().length
  })
  assertEquals(storedCount, 2)
})

Deno.test("in-memory relay - clear removes all events", async () => {
  const storedCount = await withRelay({}, (relay) => {
    relay.inject(buildEventFixture({ content: "one" }))
    relay.inject(buildEventFixture({ content: "two" }))
    relay.clear()
    return relay.getStoredEvents().length
  })
  assertEquals(storedCount, 0)
})

Deno.test("in-memory relay - cross-client event broadcast", async () => {
  const frames = await withRelay(
    {},
    (relay) =>
      withClient(relay, (publisher) =>
        withClient(relay, async (subscriber, frames) => {
          subscriber.send(JSON.stringify(["REQ", "live", { kinds: [1] }]))
          await delay(50)
          publisher.send(JSON.stringify(["EVENT", buildEventFixture({ kind: 1, content: "from client 1" })]))
          await delay(100)
          return [...frames]
        })),
  )
  assertEquals(eventContentsOf(frames), ["from client 1"])
})

Deno.test("in-memory relay - refuses a REQ before AUTH with auth-required and a challenge", async () => {
  const { refusal } = await authExchange(buildEventFixture({ kind: 22242 }))
  assertEquals(refusal, [["CLOSED", "sub1", "auth-required: authentication required"], ["AUTH", "challenge"]])
})

Deno.test("in-memory relay - answers an AUTH event with OK", async () => {
  const auth = buildEventFixture({ kind: 22242 })
  const { authReply } = await authExchange(auth)
  assertEquals(authReply, [["OK", auth.id, true, ""]])
})

Deno.test("in-memory relay - serves the REQ it refused once the socket has sent AUTH", async () => {
  const { served } = await authExchange(buildEventFixture({ kind: 22242 }))
  assertEquals(served, [["EOSE", "sub1"]])
})
