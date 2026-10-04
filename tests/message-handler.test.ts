import { assertEquals } from "@std/assert"
import { subscriptionIdFixture } from "@innis/nostr-core/testing"
import type { NostrEvent } from "@innis/nostr-core"
import { handleRelayMessage } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import type { SubHistoryRecord } from "../src/application/service/relay-history.ts"
import type { PublishAck } from "../src/infrastructure/web-socket/relay-state.ts"
import { stubInFlightPublish, stubRelayState, stubWireSub } from "./_helpers/relay-state.ts"
import { context, event, message, URL, wireSubWith } from "./_helpers/message.ts"

const SUB_1 = subscriptionIdFixture("sub-1")

Deno.test("handleRelayMessage - ignores a non-JSON payload", () => {
  const state = stubRelayState()
  handleRelayMessage(context(state), new MessageEvent("message", { data: "not json" }))
  assertEquals(state.subs.size, 0)
})

Deno.test("handleRelayMessage - ignores a non-array payload", () => {
  const state = stubRelayState()
  handleRelayMessage(context(state), message({ verb: "EVENT" }))
})

Deno.test("handleRelayMessage - delivers a matching EVENT to subscription listeners", () => {
  const received: NostrEvent[] = []
  const state = stubRelayState()
  state.subs.set(
    SUB_1,
    wireSubWith(
      [{
        onEvent: (e) => {
          received.push(e)
        },
      }],
    ),
  )
  handleRelayMessage(context(state), message(["EVENT", SUB_1, event()]))
  assertEquals(received.length, 1)
})

Deno.test("handleRelayMessage - drops an EVENT that does not match the filter", () => {
  const received: NostrEvent[] = []
  const state = stubRelayState()
  state.subs.set(
    SUB_1,
    wireSubWith(
      [{
        onEvent: (e) => {
          received.push(e)
        },
      }],
    ),
  )
  handleRelayMessage(context(state), message(["EVENT", SUB_1, event({ kind: 7 })]))
  assertEquals(received.length, 0)
})

Deno.test("handleRelayMessage - ignores an EVENT for an unknown subscription", () => {
  const state = stubRelayState()
  let counted = false
  handleRelayMessage(
    context(state, {
      onEventReceived: () => {
        counted = true
      },
    }),
    message(["EVENT", "unknown", event()]),
  )
  assertEquals(counted, false)
})

Deno.test("handleRelayMessage - fires onEose once for an EOSE message", () => {
  let eoseCalls = 0
  const state = stubRelayState()
  const sub = wireSubWith(
    [{
      onEvent: () => {},
      onEose: () => {
        eoseCalls++
      },
    }],
  )
  state.subs.set(SUB_1, sub)
  const ctx = context(state)
  handleRelayMessage(ctx, message(["EOSE", SUB_1]))
  handleRelayMessage(ctx, message(["EOSE", SUB_1]))
  assertEquals(eoseCalls, 1)
  assertEquals(sub.eoseFired, true)
})

Deno.test("handleRelayMessage - EOSE latency is measured from reqSentAt, not the original open", () => {
  const samples: number[] = []
  const state = stubRelayState()
  // Simulate a sub that was first issued long ago but re-issued (reconnect/auth) one tick before
  // this EOSE: latency must reflect the live request, not the whole gap since the first subscribe.
  state.subs.set(SUB_1, stubWireSub({ reqSentAt: 990 }))
  const ctx = context(state, { clock: () => 1000, onEoseLatency: (_url, ms) => samples.push(ms) })
  handleRelayMessage(ctx, message(["EOSE", SUB_1]))
  assertEquals(samples, [10])
})

Deno.test("handleRelayMessage - dispatches the OK ack to the publish settlement", () => {
  let result: PublishAck | null = null
  const state = stubRelayState()
  const pending = event()
  // The real settlement (createPublish's settle) removes its own in-flight entry; mirror that here
  // so the test verifies dispatch and the settlement's ownership of cleanup, not duplicated cleanup
  // in the message handler.
  state.inFlightPublishes.set(
    pending.id,
    stubInFlightPublish(pending, {
      settle: (r) => {
        result = r
        state.inFlightPublishes.delete(pending.id)
      },
    }),
  )
  handleRelayMessage(context(state), message(["OK", pending.id, true, "accepted"]))
  assertEquals(result, { ok: true, message: "accepted" })
  assertEquals(state.inFlightPublishes.size, 0)
})

Deno.test("handleRelayMessage - removes a subscription on a CLOSED message", () => {
  const state = stubRelayState()
  state.subs.set(SUB_1, stubWireSub())
  state.subIdByFilterHash.set("hash", SUB_1)
  handleRelayMessage(context(state), message(["CLOSED", SUB_1, "shutting down"]))
  assertEquals(state.subs.has(SUB_1), false)
  assertEquals(state.subIdByFilterHash.size, 0)
})

Deno.test("handleRelayMessage - fires onClosed with the reason on a non-auth CLOSED", () => {
  const reasons: string[] = []
  const state = stubRelayState()
  const sub = wireSubWith([{ onEvent: () => {}, onClosed: (reason) => reasons.push(reason) }])
  state.subs.set(SUB_1, sub)
  handleRelayMessage(context(state), message(["CLOSED", SUB_1, "rate-limited: slow down"]))
  assertEquals(reasons, ["rate-limited: slow down"])
  assertEquals(state.subs.has(SUB_1), false)
})

Deno.test("handleRelayMessage - a non-auth CLOSED does not fire onEose", () => {
  let eoseCalls = 0
  let closedCalls = 0
  const state = stubRelayState()
  const sub = wireSubWith([{ onEvent: () => {}, onEose: () => eoseCalls++, onClosed: () => closedCalls++ }])
  state.subs.set(SUB_1, sub)
  handleRelayMessage(context(state), message(["CLOSED", SUB_1, "shutting down"]))
  assertEquals(eoseCalls, 0)
  assertEquals(closedCalls, 1)
})

Deno.test("handleRelayMessage - increments the event count in subscription history", () => {
  const state = stubRelayState()
  state.subs.set(SUB_1, stubWireSub({ listeners: [{ onEvent: () => {} }] }))
  const entry: SubHistoryRecord = {
    subId: SUB_1,
    filters: [{ kinds: [1] }],
    openedAt: 0,
    closedAt: null,
    eventCount: 0,
  }
  const subHistory = new Map([[URL, new Map([[SUB_1, entry]])]])
  handleRelayMessage(context(state, { subHistory }), message(["EVENT", SUB_1, event()]))
  assertEquals(entry.eventCount, 1)
})

const utf8Bytes = (text: string): number => new TextEncoder().encode(text).length

const eventFrameOfBytes = (bytes: number, filler = "x"): MessageEvent => {
  const frameWith = (content: string): string => JSON.stringify(["EVENT", SUB_1, event({ content })])
  const room = bytes - utf8Bytes(frameWith(""))
  const repeated = filler.repeat(Math.floor(room / utf8Bytes(filler)))
  const data = frameWith(repeated + "x".repeat(room - utf8Bytes(repeated)))
  assertEquals(utf8Bytes(data), bytes)
  return new MessageEvent("message", { data })
}

const deliveredCount = (frame: MessageEvent, maxMessageBytes: number): number => {
  let delivered = 0
  const state = stubRelayState()
  state.subs.set(
    SUB_1,
    wireSubWith([{
      onEvent: () => {
        delivered++
      },
    }]),
  )
  handleRelayMessage(context(state, { maxMessageBytes }), frame)
  return delivered
}

Deno.test("handleRelayMessage - delivers a frame of exactly maxMessageBytes", () => {
  assertEquals(deliveredCount(eventFrameOfBytes(1_000), 1_000), 1)
})

Deno.test("handleRelayMessage - drops a frame one byte over maxMessageBytes", () => {
  assertEquals(deliveredCount(eventFrameOfBytes(1_001), 1_000), 0)
})

Deno.test("handleRelayMessage - drops a frame over maxMessageBytes in UTF-8 bytes though under it in UTF-16 code units", () => {
  const frame = eventFrameOfBytes(1_001, "\u20ac")
  assertEquals(frame.data.length < 1_000, true)
  assertEquals(deliveredCount(frame, 1_000), 0)
})

Deno.test("handleRelayMessage - delivers a multi-byte frame of exactly maxMessageBytes", () => {
  assertEquals(deliveredCount(eventFrameOfBytes(1_000, "\u20ac"), 1_000), 1)
})
