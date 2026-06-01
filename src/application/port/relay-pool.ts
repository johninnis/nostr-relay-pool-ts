import type { NostrEvent, NostrFilter, RelayUrl } from "@innis/nostr-core"
import type { AuthHandler } from "./auth-handler.ts"
import type { PublishHistoryEntry, PublishResponse } from "../../domain/value-object/publish-history.ts"
import type { RelayPoolStateEntry } from "../../domain/value-object/relay-pool-state-entry.ts"
import type { RelaySubscriptionEntry } from "../../domain/value-object/relay-subscription.ts"
import type {
  PoolSubscription,
  RelaySubscribeCallbacks,
  SubscribeCallbacks,
  SubscribeManyOptions,
  Subscription,
} from "../../domain/value-object/subscription.ts"

/**
 * The Nostr relay pool: WebSocket subscriptions, publishes, NIP-42 AUTH, backoff, and latency
 * tracking across many relays. Every URL-shaped argument takes a raw `string`, normalised internally
 * via `@innis/nostr-core`'s `normaliseRelayUrl`. Construct one with {@link createRelayPool}.
 */
export interface RelayPool {
  /** Open a single-relay subscription; returns a {@link Subscription} handle. */
  readonly subscribe: (
    rawUrl: string,
    filters: ReadonlyArray<NostrFilter>,
    callbacks: RelaySubscribeCallbacks,
  ) => Subscription
  /** Fan a subscription out across many relays; returns a {@link PoolSubscription} handle. */
  readonly subscribeMany: (
    rawUrls: ReadonlyArray<string>,
    filters: ReadonlyArray<NostrFilter>,
    callbacks: SubscribeCallbacks,
    options?: SubscribeManyOptions,
  ) => PoolSubscription
  /** Publish one event to one relay; resolves once the relay replies or the publish times out. */
  readonly publish: (rawUrl: string, event: NostrEvent) => Promise<PublishResponse>
  /** Relays with a currently open socket. */
  readonly getConnectedRelayUrls: () => ReadonlyArray<RelayUrl>
  /** Every relay a connection has been attempted to this session, connected or not. */
  readonly getAttemptedRelayUrls: () => ReadonlyArray<RelayUrl>
  /** Per-relay state snapshot, in a stable, intent-free order. */
  readonly getRelayPoolState: () => ReadonlyArray<RelayPoolStateEntry>
  /**
   * Subscriptions for the relay, grouped by status: active first, then pending, then closed.
   * Live subscriptions are always retained; closed entries are capped at the most recent 100.
   */
  readonly getRelaySubscriptions: (rawUrl: string) => ReadonlyArray<RelaySubscriptionEntry>
  /** The relay's publish log, newest first, capped at the most recent 100 publishes. */
  readonly getRelayPublishHistory: (rawUrl: string) => ReadonlyArray<PublishHistoryEntry>
  /** Set (or replace) the NIP-42 challenge handler after construction. */
  readonly setAuthHandler: (handler: AuthHandler) => void
  /** Clear a relay's backoff cooldown and fire any queued reconnect immediately. */
  readonly clearDisabled: (rawUrl: string) => void
  /** Tear down a relay's subscriptions, settle its in-flight publishes, and close its socket. */
  readonly disconnect: (rawUrl: string) => void
  /** Install a predicate gating which relays may connect; relays it now rejects are torn down. */
  readonly setConnectionGate: (gate: (url: RelayUrl) => boolean) => void
  /** Drop a relay's closed-subscription and settled-publish diagnostic history. */
  readonly clearRelayHistory: (rawUrl: string) => void
  /** Subscribe to per-relay connect/disconnect events; returns an unsubscribe function. */
  readonly onConnectionChange: (listener: (url: RelayUrl, connected: boolean) => void) => () => void
  /** Adaptive EOSE timeout (ms) for the relay, derived from its observed latency. */
  readonly suggestedTimeout: (rawUrl: string) => number
  /** Close every socket, cancel every timer, drop every listener, and reject further calls. Idempotent. */
  readonly dispose: () => void
}
