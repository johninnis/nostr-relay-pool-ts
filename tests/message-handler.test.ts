import { assertEquals } from "@std/assert"
import { subscriptionIdFixture } from "@innis/nostr-core/testing"
import type { NostrEvent } from "@innis/nostr-core"
import { handleRelayMessage } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import type { SubHistoryRecord } from "../src/application/service/relay-history.ts"
import { createRelayState, type PublishAck, type RelayState } from "../src/infrastructure/web-socket/relay-state.ts"
import type { WireSub } from "../src/infrastructure/web-socket/wire-sub.ts"
import { stubInFlightPublish, stubRelayState, stubWireSub } from "./_helpers/relay-state.ts"
import { context, event, message, URL, wireSubWith } from "./_helpers/message.ts"

const SUB_1 = subscriptionIdFixture("sub-1")

interface RepeatedEose {
  readonly eoseCalls: number
  readonly sub: WireSub
}

const afterRepeatedEose = (): RepeatedEose => {
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
  return { eoseCalls, sub }
}

const afterClosed = (): RelayState => {
  const state = stubRelayState()
  state.subs.set(SUB_1, stubWireSub())
  state.subIdByFilterHash.set("hash", SUB_1)
  handleRelayMessage(context(state), message(["CLOSED", SUB_1, "shutting down"]))
  return state
}

Deno.test("handleRelayMessage - ignores a non-JSON payload", () => {
  const state = stubRelayState()
  handleRelayMessage(context(state), new MessageEvent("message", { data: "not json" }))
  assertEquals(state.subs.size, 0)
})

Deno.test("handleRelayMessage - ignores a non-array payload", () => {
  const state = stubRelayState()
  handleRelayMessage(context(state), message({ verb: "EVENT" }))
  assertEquals(state, createRelayState())
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

Deno.test("handleRelayMessage - fires onEose once for a repeated EOSE message", () => {
  assertEquals(afterRepeatedEose().eoseCalls, 1)
})

Deno.test("handleRelayMessage - marks the subscription's EOSE as fired", () => {
  assertEquals(afterRepeatedEose().sub.eoseFired, true)
})

Deno.test("handleRelayMessage - EOSE latency is measured from the last REQ sent, not the original open", () => {
  const samples: number[] = []
  const state = stubRelayState()
  state.subs.set(SUB_1, stubWireSub({ lastReqSentAt: 990 }))
  const ctx = context(state, { clock: () => 1000, onEoseLatency: (_url, ms) => samples.push(ms) })
  handleRelayMessage(ctx, message(["EOSE", SUB_1]))
  assertEquals(samples, [10])
})

Deno.test("handleRelayMessage - dispatches the OK ack to the publish settlement", () => {
  let result: PublishAck | null = null
  const state = stubRelayState()
  const pending = event()
  state.inFlightPublishes.set(
    pending.id,
    stubInFlightPublish(pending, {
      settle: (r) => {
        result = r
      },
    }),
  )
  handleRelayMessage(context(state), message(["OK", pending.id, true, "accepted"]))
  assertEquals(result, { ok: true, message: "accepted" })
})

Deno.test("handleRelayMessage - leaves removing the in-flight publish to its settlement", () => {
  const state = stubRelayState()
  const pending = event()
  state.inFlightPublishes.set(pending.id, stubInFlightPublish(pending))
  handleRelayMessage(context(state), message(["OK", pending.id, true, "accepted"]))
  assertEquals(state.inFlightPublishes.has(pending.id), true)
})

Deno.test("handleRelayMessage - removes a subscription on a CLOSED message", () => {
  assertEquals(afterClosed().subs.has(SUB_1), false)
})

Deno.test("handleRelayMessage - drops the subscription's filter hash on a CLOSED message", () => {
  assertEquals(afterClosed().subIdByFilterHash.size, 0)
})

Deno.test("handleRelayMessage - fires onClosed with the reason on a non-auth CLOSED", () => {
  const reasons: string[] = []
  const state = stubRelayState()
  const sub = wireSubWith([{ onEvent: () => {}, onClosed: (reason) => reasons.push(reason) }])
  state.subs.set(SUB_1, sub)
  handleRelayMessage(context(state), message(["CLOSED", SUB_1, "rate-limited: slow down"]))
  assertEquals(reasons, ["rate-limited: slow down"])
})

Deno.test("handleRelayMessage - a non-auth CLOSED does not fire onEose", () => {
  let eoseCalls = 0
  const state = stubRelayState()
  const sub = wireSubWith([{ onEvent: () => {}, onEose: () => eoseCalls++, onClosed: () => {} }])
  state.subs.set(SUB_1, sub)
  handleRelayMessage(context(state), message(["CLOSED", SUB_1, "shutting down"]))
  assertEquals(eoseCalls, 0)
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
  if (utf8Bytes(data) !== bytes) throw new Error(`expected an EVENT frame of ${bytes} UTF-8 bytes`)
  return new MessageEvent("message", { data })
}

const frameOverInUtf8UnderInUtf16 = (limit: number): MessageEvent => {
  const frame = eventFrameOfBytes(limit + 1, "\u20ac")
  if (frame.data.length >= limit) throw new Error(`expected a frame under ${limit} UTF-16 code units`)
  return frame
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
  assertEquals(deliveredCount(frameOverInUtf8UnderInUtf16(1_000), 1_000), 0)
})

Deno.test("handleRelayMessage - delivers a multi-byte frame of exactly maxMessageBytes", () => {
  assertEquals(deliveredCount(eventFrameOfBytes(1_000, "\u20ac"), 1_000), 1)
})
