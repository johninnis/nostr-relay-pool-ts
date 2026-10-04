import type { NostrEvent, RelayUrl } from "@innis/nostr-core"

/**
 * Handle returned by {@link RelayPool.subscribe}.
 *
 * `active` is `false` if the underlying connection could not be established — invalid URL,
 * disposed pool, a relay in backoff cooldown, or a connection gate that rejected the URL. In that
 * case `unsubscribe()` is a no-op. Callers needing to distinguish those failure modes should
 * consult the pool's diagnostic methods.
 */
export interface Subscription {
  /** `false` when no connection could be established (see the interface doc for the causes). */
  readonly active: boolean
  /** Close the subscription. A no-op when `active` is `false` or already unsubscribed. */
  readonly unsubscribe: () => void
}

/**
 * Handle returned by {@link RelayPool.subscribeMany} and {@link RelayPool.subscribeManyLive}. `unsubscribe()` closes every per-relay
 * connection the fan-out opened. `syncUrls(...)` swaps the connected URL set in place — URLs
 * dropped from the new list are closed, URLs added are opened.
 */
export interface PoolSubscription {
  /** Close every per-relay leg the fan-out opened. */
  readonly unsubscribe: () => void
  /** Swap the connected relay set in place: drop legs not in `urls`, open legs newly listed. */
  readonly syncUrls: (urls: ReadonlyArray<string>) => void
}

/**
 * Callbacks for a single-relay {@link RelayPool.subscribe}.
 *
 * `onEose` fires once when the relay signals end-of-stored-events; the subscription stays open for
 * live events. `onClosed(reason)` is the terminal signal — it fires once when the subscription is
 * dead and will not be revived: either the relay *terminated* it (a NIP-01 `CLOSED` for any reason
 * other than auth — rate-limiting, shutdown, an unsupported filter) or the pool tore it down
 * (`disconnect`, `dispose`, or a connection gate now rejecting the relay, with `reason` set to
 * `"disconnected"` / `"disposed"` / `"connection gate rejected"` accordingly). Distinct from `onEose`,
 * which means the stored backlog merely ended. A clean socket drop the pool will reconnect is neither
 * — it surfaces via the pool's connection-change and state APIs instead.
 *
 * Identical filters on the same relay are de-duplicated onto one wire subscription. A caller that
 * subscribes after that wire sub has already EOSE'd receives `onEose` on a microtask but does *not*
 * replay the stored events already delivered to the earlier subscriber — it sees only events from
 * the moment it joined onward. Subscribe before you need the backlog, or use a distinct filter.
 */
export interface RelaySubscribeCallbacks {
  /** Fires per event delivered, with the relay that sent it. */
  readonly onEvent: (event: NostrEvent, url: RelayUrl) => void
  /** Fires once at end-of-stored-events; the subscription stays open for live events. */
  readonly onEose?: (() => void) | undefined
  /** Terminal signal — fires once when the subscription is dead and will not be revived. */
  readonly onClosed?: ((reason: string) => void) | undefined
}

/**
 * Callbacks for a multi-relay {@link RelayPool.subscribeMany} or {@link RelayPool.subscribeManyLive} fan-out: the
 * NIP-01 `EVENT`, `EOSE` and `CLOSED` messages of one subscription, as {@link RelaySubscribeCallbacks} receives them
 * from one relay, each naming the relay it came from.
 */
export interface SubscribeManyCallbacks {
  /** Fires per event delivered by any relay in the set, with the relay that sent it. */
  readonly onEvent: (event: NostrEvent, url: RelayUrl) => void
  /** Fires once per relay when that relay reaches end-of-stored-events. */
  readonly onRelayEose?: (url: RelayUrl) => void
  /** Fires once per relay when that relay's leg is terminally closed. */
  readonly onRelayClosed?: (url: RelayUrl, reason: string) => void
}
