import type { RelayUrl } from "@innis/nostr-core"

/**
 * Allow-list control over which relays the pool may connect to, layered on `setConnectionGate`.
 * See {@link createRelayConfig}.
 */
export interface RelayConfig {
  /** Replace the allow-list. Takes effect immediately when restricted mode is on. */
  readonly setAllowedRelays: (urls: ReadonlyArray<RelayUrl>) => void
  /** The current allow-list. */
  readonly getAllowedRelays: () => ReadonlyArray<RelayUrl>
  /** Enable or disable restriction. When on with a non-empty allow-list, only listed relays connect. */
  readonly setRestrictedMode: (restricted: boolean) => void
  /** Whether restricted mode is currently on. */
  readonly isRestrictedMode: () => boolean
}
