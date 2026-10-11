import { assertEquals } from "@std/assert"
import type { SubscriptionId } from "@innis/nostr-core"
import { createSubscribe, type SubscribeDeps } from "../src/infrastructure/web-socket/subscribe.ts"
import type { Subscription } from "../src/domain/value-object/subscription.ts"
import { createRelayState, type RelayState } from "../src/infrastructure/web-socket/relay-state.ts"
import type { SubHistoryMap } from "../src/application/service/relay-history.ts"
import { createManualTime } from "./_helpers/scheduler.ts"
import { handleRelayMessage } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import { context, event, message, URL } from "./_helpers/message.ts"
import { SEARCH_RELAY_INFORMATION } from "./_helpers/relay-information.ts"

const PENDING_SUB_TIMEOUT_MS = 30_000

const subscribeBeforeSocketOpens = (
  state: RelayState,
  time: ReturnType<typeof createManualTime>,
  overrides: Partial<SubscribeDeps> = {},
) => {
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
    relayInformation: () => null,
    ...overrides,
  })
}

const openPendingSub = (state: RelayState): SubscriptionId => {
  const [[subId, wireSub] = []] = [...state.pendingSubs]
  if (subId === undefined || wireSub === undefined) throw new Error("expected a pending subscription")
  state.subs.set(subId, wireSub)
  return subId
}

interface SubscriptionSignals {
  readonly state: RelayState
  readonly handle: Subscription
  readonly closedReasons: ReadonlyArray<string>
  readonly eoseCalls: () => number
}

const subscribedBeforeSocketOpens = (time: ReturnType<typeof createManualTime>): SubscriptionSignals => {
  const state = createRelayState()
  const closedReasons: Array<string> = []
  let eoseCalls = 0
  const handle = subscribeBeforeSocketOpens(state, time)(URL, [{ kinds: [1] }], {
    onEvent: (): void => {},
    onEose: (): void => {
      eoseCalls++
    },
    onClosed: (reason): void => {
      closedReasons.push(reason)
    },
  })
  return { state, handle, closedReasons, eoseCalls: () => eoseCalls }
}

const pendingSubTimedOut = (): SubscriptionSignals => {
  const time = createManualTime()
  const subscribed = subscribedBeforeSocketOpens(time)
  time.tick(PENDING_SUB_TIMEOUT_MS)
  return subscribed
}

interface MatchingNothing {
  readonly handle: Subscription
  readonly connections: number
  readonly pendingSubs: number
  readonly eoseCalls: number
}

const subscribedMatchingNothing = async (): Promise<MatchingNothing> => {
  const state = createRelayState()
  let connections = 0
  const subscribe = subscribeBeforeSocketOpens(state, createManualTime(), {
    getOrCreateConnection: (): RelayState => {
      connections++
      return state
    },
  })
  let eoseCalls = 0
  const handle = subscribe(URL, [{ authors: [] }, { kinds: [] }], {
    onEvent: (): void => {},
    onEose: (): void => {
      eoseCalls++
    },
  })
  await Promise.resolve()
  return { handle, connections, pendingSubs: state.pendingSubs.size, eoseCalls }
}

Deno.test("subscribe - a sub made before the socket opens is active", () => {
  assertEquals(subscribedBeforeSocketOpens(createManualTime()).handle.active, true)
})

Deno.test("subscribe - a sub made before the socket opens waits in pendingSubs", () => {
  assertEquals(subscribedBeforeSocketOpens(createManualTime()).state.pendingSubs.size, 1)
})

Deno.test("subscribe - a pending sub that times out fires the terminal onClosed", () => {
  assertEquals(pendingSubTimedOut().closedReasons, ["timeout"])
})

Deno.test("subscribe - a pending sub that times out fires no onEose", () => {
  assertEquals(pendingSubTimedOut().eoseCalls(), 0)
})

Deno.test("subscribe - a pending sub that times out leaves pendingSubs", () => {
  assertEquals(pendingSubTimedOut().state.pendingSubs.size, 0)
})

Deno.test("subscribe - filters that can match nothing are answered with an active handle", async () => {
  assertEquals((await subscribedMatchingNothing()).handle.active, true)
})

Deno.test("subscribe - filters that can match nothing open no connection", async () => {
  assertEquals((await subscribedMatchingNothing()).connections, 0)
})

Deno.test("subscribe - filters that can match nothing park no pending subscription", async () => {
  assertEquals((await subscribedMatchingNothing()).pendingSubs, 0)
})

Deno.test("subscribe - filters that can match nothing end their stored events at once", async () => {
  assertEquals((await subscribedMatchingNothing()).eoseCalls, 1)
})

Deno.test("subscribe - a subscription that can match nothing and is closed at once never ends its stored events", async () => {
  const state = createRelayState()
  const subscribe = subscribeBeforeSocketOpens(state, createManualTime())
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
  const subscribe = subscribeBeforeSocketOpens(state, time, { relayInformation: () => SEARCH_RELAY_INFORMATION })
  const received: Array<string> = []
  subscribe(URL, [{ kinds: [1], search: "developers" }], {
    onEvent: (event): void => {
      received.push(event.content)
    },
  })
  const subId = openPendingSub(state)
  handleRelayMessage(context(state), message(["EVENT", subId, event({ content: "nostr devs" })]))
  assertEquals(received, ["nostr devs"])
})

Deno.test("subscribe - an event outside a search subscription's other conditions is still dropped", () => {
  const state = createRelayState()
  const time = createManualTime()
  const subscribe = subscribeBeforeSocketOpens(state, time, { relayInformation: () => SEARCH_RELAY_INFORMATION })
  const received: Array<string> = []
  subscribe(URL, [{ kinds: [1], search: "developers" }], {
    onEvent: (event): void => {
      received.push(event.content)
    },
  })
  const subId = openPendingSub(state)
  handleRelayMessage(context(state), message(["EVENT", subId, event({ kind: 7, content: "developers" })]))
  assertEquals(received, [])
})

Deno.test("subscribe - an event only a search withheld from the relay admits is dropped", () => {
  const state = createRelayState()
  const subscribe = subscribeBeforeSocketOpens(state, createManualTime())
  const received: Array<string> = []
  subscribe(URL, [{ kinds: [1], search: "developers" }, { kinds: [0] }], {
    onEvent: (event): void => {
      received.push(event.content)
    },
  })
  const subId = openPendingSub(state)
  handleRelayMessage(context(state), message(["EVENT", subId, event({ kind: 1, content: "developers" })]))
  assertEquals(received, [])
})

Deno.test("subscribe - a subscription of only searches opens no connection to a relay whose NIP-11 document is unknown", () => {
  const state = createRelayState()
  let connections = 0
  const subscribe = subscribeBeforeSocketOpens(state, createManualTime(), {
    getOrCreateConnection: (): RelayState => {
      connections++
      return state
    },
  })
  subscribe(URL, [{ kinds: [1], search: "developers" }], { onEvent: (): void => {} })
  assertEquals(connections, 0)
})

Deno.test("subscribe - a subscription of only searches ends its stored events at once at a relay whose NIP-11 document is unknown", async () => {
  const subscribe = subscribeBeforeSocketOpens(createRelayState(), createManualTime())
  let eoseCalls = 0
  subscribe(URL, [{ kinds: [1], search: "developers" }], {
    onEvent: (): void => {},
    onEose: (): void => {
      eoseCalls++
    },
  })
  await Promise.resolve()
  assertEquals(eoseCalls, 1)
})

Deno.test("subscribe - a subscription of only searches is active at a relay whose NIP-11 document is unknown", () => {
  const subscribe = subscribeBeforeSocketOpens(createRelayState(), createManualTime())
  const handle = subscribe(URL, [{ kinds: [1], search: "developers" }], { onEvent: (): void => {} })
  assertEquals(handle.active, true)
})
