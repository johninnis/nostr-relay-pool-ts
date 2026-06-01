import type { RelayUrl } from "@innis/nostr-core"
import type { RelayConfig } from "../port/relay-config.ts"
import type { RelayPool } from "../port/relay-pool.ts"

const setEquals = (a: ReadonlySet<RelayUrl>, b: ReadonlySet<RelayUrl>): boolean => {
  if (a.size !== b.size) return false
  for (const url of a) if (!b.has(url)) return false
  return true
}

/**
 * Build a {@link RelayConfig} that drives `pool.setConnectionGate` from an allow-list and a
 * restricted-mode flag. When restricted is on and the allow-list is non-empty, only listed relays
 * may connect; otherwise the gate admits everything. The library has no opinion on what the relays
 * represent — the host supplies them.
 */
export const createRelayConfig = (pool: RelayPool): RelayConfig => {
  let allowedRelays: ReadonlySet<RelayUrl> = new Set()
  let restricted = false

  const applyGate = (): void => {
    if (restricted && allowedRelays.size > 0) {
      const allowed = allowedRelays
      pool.setConnectionGate((url) => allowed.has(url))
    } else {
      pool.setConnectionGate(() => true)
    }
  }

  return Object.freeze({
    setAllowedRelays: (urls: ReadonlyArray<RelayUrl>): void => {
      const next = new Set<RelayUrl>(urls)
      if (setEquals(allowedRelays, next)) return
      allowedRelays = next
      applyGate()
    },
    getAllowedRelays: (): ReadonlyArray<RelayUrl> => [...allowedRelays],
    setRestrictedMode: (next: boolean): void => {
      if (restricted === next) return
      restricted = next
      applyGate()
    },
    isRestrictedMode: (): boolean => restricted,
  })
}
