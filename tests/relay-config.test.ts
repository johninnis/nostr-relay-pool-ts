import { assertEquals } from "@std/assert"
import type { RelayUrl } from "@innis/nostr-core"
import { parseRelayUrl } from "@innis/nostr-core"
import type { RelayPool } from "../src/application/port/relay-pool.ts"
import { createRelayConfig } from "../src/application/service/relay-config.ts"

const stubPool = (): RelayPool & { gates: Array<(url: RelayUrl) => boolean> } => {
  const gates: Array<(url: RelayUrl) => boolean> = []
  const unimplemented = (): never => {
    throw new Error("stub pool method not implemented")
  }
  return {
    setConnectionGate: (gate: (url: RelayUrl) => boolean): void => {
      gates.push(gate)
    },
    gates,
    subscribe: unimplemented,
    subscribeMany: unimplemented,
    publish: unimplemented,
    getConnectedRelayUrls: () => [],
    getAttemptedRelayUrls: () => [],
    getRelayPoolState: () => [],
    getRelaySubscriptions: () => [],
    getRelayPublishHistory: () => [],
    setAuthHandler: () => {},
    clearDisabled: () => {},
    disconnect: () => {},
    clearRelayHistory: () => {},
    onConnectionChange: () => () => {},
    suggestedTimeout: () => 4000,
    dispose: () => {},
  }
}

const URL_A = parseRelayUrl("wss://a.example.com")
const URL_B = parseRelayUrl("wss://b.example.com")

Deno.test("createRelayConfig - defaults are unrestricted and empty allowlist", () => {
  const pool = stubPool()
  const config = createRelayConfig(pool)
  assertEquals(config.isRestrictedMode(), false)
  assertEquals(config.getAllowedRelays(), [])
})

Deno.test("createRelayConfig - restricted + allowlist gates connections to the allowlist", () => {
  const pool = stubPool()
  const config = createRelayConfig(pool)
  config.setRestrictedMode(true)
  config.setAllowedRelays([URL_A])
  const gate = pool.gates.at(-1)
  if (!gate) throw new Error("expected a gate to have been applied")
  assertEquals(gate(URL_A), true)
  assertEquals(gate(URL_B), false)
})

Deno.test("createRelayConfig - restricted but empty allowlist accepts everything", () => {
  const pool = stubPool()
  const config = createRelayConfig(pool)
  config.setRestrictedMode(true)
  const gate = pool.gates.at(-1)
  if (!gate) throw new Error("expected a gate to have been applied")
  assertEquals(gate(URL_A), true)
})

Deno.test("createRelayConfig - clearing restricted mode releases the gate", () => {
  const pool = stubPool()
  const config = createRelayConfig(pool)
  config.setRestrictedMode(true)
  config.setAllowedRelays([URL_A])
  config.setRestrictedMode(false)
  const gate = pool.gates.at(-1)
  if (!gate) throw new Error("expected a gate to have been applied")
  assertEquals(gate(URL_B), true)
})

Deno.test("createRelayConfig - setAllowedRelays is a no-op when the set is unchanged", () => {
  const pool = stubPool()
  const config = createRelayConfig(pool)
  config.setAllowedRelays([URL_A])
  const callsAfterFirst = pool.gates.length
  config.setAllowedRelays([URL_A])
  assertEquals(pool.gates.length, callsAfterFirst)
})
