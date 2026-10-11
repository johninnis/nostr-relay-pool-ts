import type { CompiledFilter, NostrFilter } from "@innis/nostr-core"
import type { RelaySubscribeCallbacks } from "../../domain/value-object/subscription.ts"

export type Widening = "none" | "awaiting-auth" | "sent"

export interface WireSub {
  readonly filters: ReadonlyArray<NostrFilter>
  readonly filterHash: string
  // Deliberate: an event is matched without the search, which the relay answers by its own reading — see shared ADR-0082 and ADR-0009
  readonly compiledWithoutSearch: CompiledFilter
  listeners: ReadonlyArray<RelaySubscribeCallbacks>
  eoseFired: boolean
  lastReqSentAt: number
  widening: Widening
}
