import { assertEquals } from "@std/assert"
import { parseRelayUrl } from "@innis/nostr-core"
import { createSubscribe } from "../src/infrastructure/web-socket/subscribe.ts"
import { createRelayState, type RelayState } from "../src/infrastructure/web-socket/relay-state.ts"
import type { SubHistoryMap } from "../src/application/service/relay-history.ts"
import { createManualTime } from "./_helpers/scheduler.ts"

const URL = parseRelayUrl("wss://relay.example.com")
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
    connections: new Map([[URL, state]]),
    invalidateCache: (): void => {},
    isDisposed: (): boolean => false,
    getOrCreateConnection: (): RelayState => state,
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
