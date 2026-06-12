import { assertEquals } from "@std/assert"
import type { NostrEvent } from "@innis/nostr-core"
import { parseEventId, parsePublicKey, parseRelayUrl, parseSig } from "@innis/nostr-core"
import { systemWallClock } from "../src/infrastructure/adapter/system-wall-clock-adapter.ts"
import { systemScheduler } from "../src/infrastructure/adapter/system-scheduler-adapter.ts"
import type { MessageContext } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import { handleRelayMessage } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import type { RelayState } from "../src/infrastructure/web-socket/relay-state.ts"
import type { SubHistoryRecord } from "../src/application/service/relay-history.ts"
import type { PublishAck } from "../src/infrastructure/web-socket/relay-state.ts"
import type { WireSub } from "../src/infrastructure/web-socket/wire-sub.ts"
import { stubInFlightPublish, stubRelayState, stubWireSub } from "./_helpers/relay-state.ts"

const URL = parseRelayUrl("wss://relay.example.com")

const event = (overrides: Partial<NostrEvent> = {}): NostrEvent => ({
  id: parseEventId("a".repeat(64)),
  pubkey: parsePublicKey("b".repeat(64)),
  created_at: 1700000000,
  kind: 1,
  tags: [],
  content: "hello",
  sig: parseSig("c".repeat(128)),
  ...overrides,
})

const context = (state: RelayState, partial: Partial<MessageContext> = {}): MessageContext => ({
  state,
  url: URL,
  subHistory: new Map(),
  authHandler: () => null,
  clock: systemWallClock,
  scheduler: systemScheduler,
  onEventReceived: () => {},
  onEoseLatency: () => {},
  onStateChange: () => {},
  ...partial,
})

const wireSubWith = (listeners: WireSub["listeners"]): WireSub => stubWireSub({ listeners })

const message = (payload: unknown): MessageEvent => new MessageEvent("message", { data: JSON.stringify(payload) })

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
    "sub-1",
    wireSubWith(
      [{
        onEvent: (e) => {
          received.push(e)
        },
      }],
    ),
  )
  handleRelayMessage(context(state), message(["EVENT", "sub-1", event()]))
  assertEquals(received.length, 1)
})

Deno.test("handleRelayMessage - drops an EVENT that does not match the filter", () => {
  const received: NostrEvent[] = []
  const state = stubRelayState()
  state.subs.set(
    "sub-1",
    wireSubWith(
      [{
        onEvent: (e) => {
          received.push(e)
        },
      }],
    ),
  )
  handleRelayMessage(context(state), message(["EVENT", "sub-1", event({ kind: 7 })]))
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
  state.subs.set("sub-1", sub)
  const ctx = context(state)
  handleRelayMessage(ctx, message(["EOSE", "sub-1"]))
  handleRelayMessage(ctx, message(["EOSE", "sub-1"]))
  assertEquals(eoseCalls, 1)
  assertEquals(sub.eoseFired, true)
})

Deno.test("handleRelayMessage - EOSE latency is measured from reqSentAt, not the original open", () => {
  const samples: number[] = []
  const state = stubRelayState()
  // Simulate a sub that was first issued long ago but re-issued (reconnect/auth) one tick before
  // this EOSE: latency must reflect the live request, not the whole gap since the first subscribe.
  state.subs.set("sub-1", stubWireSub({ reqSentAt: 990 }))
  const ctx = context(state, { clock: () => 1000, onEoseLatency: (_url, ms) => samples.push(ms) })
  handleRelayMessage(ctx, message(["EOSE", "sub-1"]))
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

Deno.test("handleRelayMessage - queues an event for auth retry on an auth-required OK", () => {
  const state = stubRelayState()
  const pending = event()
  state.inFlightPublishes.set(pending.id, stubInFlightPublish(pending))
  handleRelayMessage(context(state), message(["OK", pending.id, false, "auth-required: please AUTH"]))
  assertEquals(state.pendingAuthPublish.length, 1)
  assertEquals(state.pendingAuthPublish[0], pending)
  // The in-flight record stays put — its timeout keeps running until AUTH completes or it expires.
  assertEquals(state.inFlightPublishes.size, 1)
})

Deno.test("handleRelayMessage - removes a subscription on a CLOSED message", () => {
  const state = stubRelayState()
  state.subs.set("sub-1", stubWireSub())
  state.subIdByFilterHash.set("hash", "sub-1")
  handleRelayMessage(context(state), message(["CLOSED", "sub-1", "shutting down"]))
  assertEquals(state.subs.has("sub-1"), false)
  assertEquals(state.subIdByFilterHash.size, 0)
})

Deno.test("handleRelayMessage - fires onClosed with the reason on a non-auth CLOSED", () => {
  const reasons: string[] = []
  const state = stubRelayState()
  const sub = wireSubWith([{ onEvent: () => {}, onClosed: (reason) => reasons.push(reason) }])
  state.subs.set("sub-1", sub)
  handleRelayMessage(context(state), message(["CLOSED", "sub-1", "rate-limited: slow down"]))
  assertEquals(reasons, ["rate-limited: slow down"])
  assertEquals(state.subs.has("sub-1"), false)
})

Deno.test("handleRelayMessage - a non-auth CLOSED does not fire onEose", () => {
  let eoseCalls = 0
  let closedCalls = 0
  const state = stubRelayState()
  const sub = wireSubWith([{ onEvent: () => {}, onEose: () => eoseCalls++, onClosed: () => closedCalls++ }])
  state.subs.set("sub-1", sub)
  handleRelayMessage(context(state), message(["CLOSED", "sub-1", "shutting down"]))
  assertEquals(eoseCalls, 0)
  assertEquals(closedCalls, 1)
})

Deno.test("handleRelayMessage - an auth-required CLOSED fires neither onEose nor onClosed", () => {
  let eoseCalls = 0
  let closedCalls = 0
  const state = stubRelayState()
  const sub = wireSubWith([{ onEvent: () => {}, onEose: () => eoseCalls++, onClosed: () => closedCalls++ }])
  state.subs.set("sub-1", sub)
  handleRelayMessage(context(state), message(["CLOSED", "sub-1", "auth-required: restricted"]))
  assertEquals(eoseCalls, 0)
  assertEquals(closedCalls, 0)
  assertEquals(state.pendingAuthSubs.has("sub-1"), true)
})

Deno.test("handleRelayMessage - re-queues a subscription closed with auth-required", () => {
  const state = stubRelayState()
  state.subs.set("sub-1", stubWireSub())
  handleRelayMessage(context(state), message(["CLOSED", "sub-1", "auth-required: restricted"]))
  assertEquals(state.subs.has("sub-1"), false)
  assertEquals(state.pendingAuthSubs.has("sub-1"), true)
})

Deno.test("handleRelayMessage - an auth-required CLOSED clears authed so a re-challenge can re-auth", () => {
  const state = stubRelayState()
  state.authed = true
  state.subs.set("sub-1", stubWireSub())
  handleRelayMessage(context(state), message(["CLOSED", "sub-1", "auth-required: restricted"]))
  assertEquals(state.authed, false)
  assertEquals(state.pendingAuthSubs.has("sub-1"), true)
})

Deno.test("handleRelayMessage - increments the event count in subscription history", () => {
  const state = stubRelayState()
  state.subs.set("sub-1", stubWireSub({ listeners: [{ onEvent: () => {} }] }))
  const entry: SubHistoryRecord = {
    subId: "sub-1",
    filters: [{ kinds: [1] }],
    openedAt: 0,
    closedAt: null,
    eventCount: 0,
  }
  const subHistory = new Map([[URL, new Map([["sub-1", entry]])]])
  handleRelayMessage(context(state, { subHistory }), message(["EVENT", "sub-1", event()]))
  assertEquals(entry.eventCount, 1)
})
