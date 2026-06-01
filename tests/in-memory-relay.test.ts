import { assert, assertEquals, assertGreater } from "@std/assert"
import { createInMemoryRelay } from "../testing.ts"
import { buildEventFixture } from "@innis/nostr-core/testing"

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

const connectWs = (url: string): Promise<WebSocket> =>
  new Promise((resolve) => {
    const ws = new WebSocket(url)
    ws.onopen = (): void => resolve(ws)
  })

const collectMessages = (ws: WebSocket): Array<unknown[]> => {
  const messages: Array<unknown[]> = []
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

const eventContent = (value: unknown): string => {
  assert(hasStringContent(value), "expected an event with string content")
  return value.content
}

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

Deno.test("in-memory relay - stores published events and responds OK", async () => {
  const relay = createInMemoryRelay()
  await relay.start()

  const ws = await connectWs(relay.url)
  const messages = collectMessages(ws)

  const event = buildEventFixture({ content: "hello relay" })
  ws.send(JSON.stringify(["EVENT", event]))
  await delay(100)

  assertEquals(messages.length, 1)
  const [okMessage] = messages
  if (!okMessage) throw new Error("expected an OK message")
  assertEquals(okMessage[0], "OK")
  assertEquals(okMessage[1], event.id)
  assertEquals(okMessage[2], true)

  const storedEvents = relay.getStoredEvents()
  assertEquals(storedEvents.length, 1)
  const [storedEvent] = storedEvents
  if (!storedEvent) throw new Error("expected a stored event")
  assertEquals(storedEvent.content, "hello relay")

  ws.close()
  await delay(50)
  await relay.stop()
})

Deno.test("in-memory relay - REQ returns matching events then EOSE", async () => {
  const relay = createInMemoryRelay()
  await relay.start()

  const event1 = buildEventFixture({ kind: 1, content: "note 1" })
  const event2 = buildEventFixture({ kind: 0, content: "profile" })
  relay.inject(event1)
  relay.inject(event2)

  const ws = await connectWs(relay.url)
  const messages = collectMessages(ws)

  ws.send(JSON.stringify(["REQ", "sub1", { kinds: [1] }]))
  await delay(100)

  const eventMessages = messages.filter((m) => m[0] === "EVENT")
  const eoseMessages = messages.filter((m) => m[0] === "EOSE")

  assertEquals(eventMessages.length, 1)
  const [eventMessage] = eventMessages
  if (!eventMessage) throw new Error("expected an EVENT message")
  assertEquals(eventContent(eventMessage[2]), "note 1")
  assertEquals(eoseMessages.length, 1)
  const [eoseMessage] = eoseMessages
  if (!eoseMessage) throw new Error("expected an EOSE message")
  assertEquals(eoseMessage[1], "sub1")

  ws.close()
  await delay(50)
  await relay.stop()
})

Deno.test("in-memory relay - REQ filters by authors", async () => {
  const relay = createInMemoryRelay()
  await relay.start()

  const pubkeyA = "a".repeat(64)
  const pubkeyB = "b".repeat(64)
  relay.inject(buildEventFixture({ pubkey: pubkeyA, content: "from A" }))
  relay.inject(buildEventFixture({ pubkey: pubkeyB, content: "from B" }))

  const ws = await connectWs(relay.url)
  const messages = collectMessages(ws)

  ws.send(JSON.stringify(["REQ", "sub1", { authors: [pubkeyA] }]))
  await delay(100)

  const eventMessages = messages.filter((m) => m[0] === "EVENT")
  assertEquals(eventMessages.length, 1)
  const [eventMessage] = eventMessages
  if (!eventMessage) throw new Error("expected an EVENT message")
  assertEquals(eventContent(eventMessage[2]), "from A")

  ws.close()
  await delay(50)
  await relay.stop()
})

Deno.test("in-memory relay - live subscription receives new events", async () => {
  const relay = createInMemoryRelay()
  await relay.start()

  const ws = await connectWs(relay.url)
  const messages = collectMessages(ws)

  ws.send(JSON.stringify(["REQ", "live", { kinds: [1] }]))
  await delay(50)

  const eoseCount = messages.filter((m) => m[0] === "EOSE").length
  assertEquals(eoseCount, 1)

  const liveEvent = buildEventFixture({ kind: 1, content: "live!" })
  relay.inject(liveEvent)
  await delay(100)

  const eventMessages = messages.filter((m) => m[0] === "EVENT")
  assertEquals(eventMessages.length, 1)
  const [eventMessage] = eventMessages
  if (!eventMessage) throw new Error("expected an EVENT message")
  assertEquals(eventContent(eventMessage[2]), "live!")

  ws.close()
  await delay(50)
  await relay.stop()
})

Deno.test("in-memory relay - CLOSE unsubscribes", async () => {
  const relay = createInMemoryRelay()
  await relay.start()

  const ws = await connectWs(relay.url)
  const messages = collectMessages(ws)

  ws.send(JSON.stringify(["REQ", "sub1", { kinds: [1] }]))
  await delay(50)

  ws.send(JSON.stringify(["CLOSE", "sub1"]))
  await delay(50)

  relay.inject(buildEventFixture({ kind: 1, content: "after close" }))
  await delay(100)

  const eventMessages = messages.filter((m) => m[0] === "EVENT")
  assertEquals(eventMessages.length, 0)

  ws.close()
  await delay(50)
  await relay.stop()
})

Deno.test("in-memory relay - tag filter matching", async () => {
  const relay = createInMemoryRelay()
  await relay.start()

  const targetId = "d".repeat(64)
  relay.inject(buildEventFixture({ tags: [["e", targetId]], content: "reply" }))
  relay.inject(buildEventFixture({ content: "unrelated" }))

  const ws = await connectWs(relay.url)
  const messages = collectMessages(ws)

  ws.send(JSON.stringify(["REQ", "sub1", { "#e": [targetId] }]))
  await delay(100)

  const eventMessages = messages.filter((m) => m[0] === "EVENT")
  assertEquals(eventMessages.length, 1)
  const [eventMessage] = eventMessages
  if (!eventMessage) throw new Error("expected an EVENT message")
  assertEquals(eventContent(eventMessage[2]), "reply")

  ws.close()
  await delay(50)
  await relay.stop()
})

Deno.test("in-memory relay - clear removes all events", async () => {
  const relay = createInMemoryRelay()
  await relay.start()

  relay.inject(buildEventFixture())
  relay.inject(buildEventFixture())
  assertEquals(relay.getStoredEvents().length, 2)

  relay.clear()
  assertEquals(relay.getStoredEvents().length, 0)

  await relay.stop()
})

Deno.test("in-memory relay - cross-client event broadcast", async () => {
  const relay = createInMemoryRelay()
  await relay.start()

  const ws1 = await connectWs(relay.url)
  const ws2 = await connectWs(relay.url)
  const messages2 = collectMessages(ws2)

  ws2.send(JSON.stringify(["REQ", "live", { kinds: [1] }]))
  await delay(50)

  const event = buildEventFixture({ kind: 1, content: "from client 1" })
  ws1.send(JSON.stringify(["EVENT", event]))
  await delay(100)

  const eventMessages = messages2.filter((m) => m[0] === "EVENT")
  assertEquals(eventMessages.length, 1)
  const [eventMessage] = eventMessages
  if (!eventMessage) throw new Error("expected an EVENT message")
  assertEquals(eventContent(eventMessage[2]), "from client 1")

  ws1.close()
  ws2.close()
  await delay(50)
  await relay.stop()
})
