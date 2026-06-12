import { compileFilters, type NostrEvent, type NostrFilter } from "@innis/nostr-core"
import {
  createRelayState,
  type InFlightPublish,
  type RelayState,
} from "../../src/infrastructure/web-socket/relay-state.ts"
import type { WireSub } from "../../src/infrastructure/web-socket/wire-sub.ts"

export const stubRelayState = (overrides: Partial<RelayState> = {}): RelayState => ({
  ...createRelayState(),
  ...overrides,
})

// Build an in-flight publish record with an already-cleared timer handle, so a test that never
// settles it leaks no pending op. Callers override `settle` to observe dispatch.
export const stubInFlightPublish = (event: NostrEvent, overrides: Partial<InFlightPublish> = {}): InFlightPublish => {
  const timeoutId = setTimeout(() => {}, 0)
  clearTimeout(timeoutId)
  return {
    event,
    resolvers: new Set(),
    settle: () => {},
    timeoutId,
    ...overrides,
  }
}

export const stubWireSub = (overrides: Partial<WireSub> = {}): WireSub => {
  const filters: ReadonlyArray<NostrFilter> = overrides.filters ?? [{ kinds: [1] }]
  return {
    filters,
    filterHash: "hash",
    compiled: compileFilters(filters),
    listeners: [],
    eoseFired: false,
    reqSentAt: 0,
    ...overrides,
  }
}
