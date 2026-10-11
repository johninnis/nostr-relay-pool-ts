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

export const stubInFlightPublish = (event: NostrEvent, overrides: Partial<InFlightPublish> = {}): InFlightPublish => ({
  event,
  resolvers: new Set(),
  settle: () => {},
  suspendTimeout: () => {},
  restartTimeout: () => {},
  ...overrides,
})

export const stubWireSub = (overrides: Partial<WireSub> = {}): WireSub => {
  const filters: ReadonlyArray<NostrFilter> = overrides.filters ?? [{ kinds: [1] }]
  return {
    filters,
    filterHash: "hash",
    compiledWithoutSearch: compileFilters(filters),
    listeners: [],
    eoseFired: false,
    lastReqSentAt: 0,
    widening: "none",
    ...overrides,
  }
}
