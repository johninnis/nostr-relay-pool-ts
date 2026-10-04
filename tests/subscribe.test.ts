import { assertEquals } from "@std/assert"
import { relayUrlFixture } from "@innis/nostr-core/testing"
import { createSubscribe } from "../src/infrastructure/web-socket/subscribe.ts"
import { createRelayState, type RelayState } from "../src/infrastructure/web-socket/relay-state.ts"
import type { SubHistoryMap } from "../src/application/service/relay-history.ts"
import { createManualTime } from "./_helpers/scheduler.ts"
import { handleRelayMessage } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import { context, event, message } from "./_helpers/message.ts"

const URL = relayUrlFixture("wss://relay.example.com")
const PENDING_SUB_TIMEOUT_MS = 30_000

// A sub opened against a not-yet-open socket parks in pendingSubs with a watchdog timer. The
// socket here (createRelayState gives ws = null) never opens, so the watchdog is the only path out.
const subscribeWith = (state: RelayState, time: ReturnType<typeof createManualTime>) => {
  const subHistory: SubHistoryMap = new Map()
  return createSubscribe({
    clock: time.clock,
    scheduler: time.scheduler,
    pendingSubTimeoutMs: PENDING_SUB_TIMEOUT_MS,
    subHistory,
    findOwningState: (): RelayState => state,
    invalidateCache: (): void => {},
    isDisposed: (): boolean => false,
    getOrCreateConnection: (): RelayState => state,
    releaseIfIdle: (): void => {},
  })
}

Deno.test("subscribe - a pending sub that times out fires the terminal onClosed", () => {
  const state = createRelayState()
  const time = createManualTime()
  const subscribe = subscribeWith(state, time)

  const closedReasons: Array<string> = []
  let eoseCalls = 0
  const handle = subscribe(URL, [{ kinds: [1] }], {
    onEvent: (): void => {},
    onEose: (): void => {
      eoseCalls++
    },
    onClosed: (reason): void => {
      closedReasons.push(reason)
    },
  })

  assertEquals(handle.active, true)
  assertEquals(state.pendingSubs.size, 1)

  time.tick(PENDING_SUB_TIMEOUT_MS)

  assertEquals(closedReasons, ["timeout"])
  assertEquals(eoseCalls, 0)
  assertEquals(state.pendingSubs.size, 0)
})

Deno.test("subscribe - filters that can match nothing open no connection and end their stored events at once", async () => {
  const state = createRelayState()
  const time = createManualTime()
  let connections = 0
  const subscribe = createSubscribe({
    clock: time.clock,
    scheduler: time.scheduler,
    pendingSubTimeoutMs: PENDING_SUB_TIMEOUT_MS,
    subHistory: new Map(),
    findOwningState: (): RelayState => state,
    invalidateCache: (): void => {},
    isDisposed: (): boolean => false,
    getOrCreateConnection: (): RelayState => {
      connections++
      return state
    },
    releaseIfIdle: (): void => {},
  })
  let eoseCalls = 0
  const handle = subscribe(URL, [{ authors: [] }, { kinds: [] }], {
    onEvent: (): void => {},
    onEose: (): void => {
      eoseCalls++
    },
  })
  await Promise.resolve()

  assertEquals([handle.active, connections, state.pendingSubs.size, eoseCalls], [true, 0, 0, 1])
})

Deno.test("subscribe - a subscription that can match nothing and is closed at once never ends its stored events", async () => {
  const state = createRelayState()
  const subscribe = subscribeWith(state, createManualTime())
  let eoseCalls = 0
  subscribe(URL, [{ ids: [] }], {
    onEvent: (): void => {},
    onEose: (): void => {
      eoseCalls++
    },
  }).unsubscribe()
  await Promise.resolve()

  assertEquals(eoseCalls, 0)
})

Deno.test("subscribe - an event a relay answers a search with is delivered, since the relay decides what matches it", () => {
  const state = createRelayState()
  const time = createManualTime()
  const subscribe = subscribeWith(state, time)
  const received: Array<string> = []
  subscribe(URL, [{ kinds: [1], search: "developers" }], {
    onEvent: (event): void => {
      received.push(event.content)
    },
  })
  const [[subId, wireSub] = []] = [...state.pendingSubs]
  if (subId === undefined || wireSub === undefined) throw new Error("expected a pending subscription")
  state.subs.set(subId, wireSub)
  handleRelayMessage(context(state), message(["EVENT", subId, event({ content: "nostr devs" })]))
  assertEquals(received, ["nostr devs"])
})

Deno.test("subscribe - an event outside a search subscription's other conditions is still dropped", () => {
  const state = createRelayState()
  const time = createManualTime()
  const subscribe = subscribeWith(state, time)
  const received: Array<string> = []
  subscribe(URL, [{ kinds: [1], search: "developers" }], {
    onEvent: (event): void => {
      received.push(event.content)
    },
  })
  const [[subId, wireSub] = []] = [...state.pendingSubs]
  if (subId === undefined || wireSub === undefined) throw new Error("expected a pending subscription")
  state.subs.set(subId, wireSub)
  handleRelayMessage(context(state), message(["EVENT", subId, event({ kind: 7, content: "developers" })]))
  assertEquals(received, [])
})
