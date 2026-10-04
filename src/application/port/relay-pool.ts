import type { NostrEvent, NostrFilter, RelayUrl } from "@innis/nostr-core"
import type { AuthHandler } from "./auth-handler.ts"
import type { PublishHistoryEntry, PublishResponse } from "../../domain/value-object/publish-history.ts"
import type { RelayPoolStateEntry } from "../../domain/value-object/relay-pool-state-entry.ts"
import type { RelaySubscriptionEntry } from "../../domain/value-object/relay-subscription.ts"
import type {
  PoolSubscription,
  RelaySubscribeCallbacks,
  SubscribeManyCallbacks,
  Subscription,
} from "../../domain/value-object/subscription.ts"

/**
 * The Nostr relay pool: WebSocket subscriptions, publishes, NIP-42 AUTH, backoff, and latency
 * tracking across many relays. Every URL-shaped argument takes a raw `string`, normalised internally
 * via `@innis/nostr-core`'s `parseRelayUrl`. Construct one with {@link createRelayPool}.
 */
export interface RelayPool {
  /**
   * Open a single-relay subscription; returns a {@link Subscription} handle. A filter that can match nothing is not
   * sent, and a subscription none of whose filters can match anything opens no connection: it ends its stored events
   * on a microtask and never delivers an event.
   */
  readonly subscribe: (
    rawUrl: string,
    filters: ReadonlyArray<NostrFilter>,
    callbacks: RelaySubscribeCallbacks,
  ) => Subscription
  /**
   * Fetch stored events from many relays; returns a {@link PoolSubscription} handle. Each relay's leg closes once that
   * relay reaches end-of-stored-events, or at its adaptive timeout. For a feed that stays open, use
   * {@link RelayPool.subscribeManyLive}.
   */
  readonly subscribeMany: (
    rawUrls: ReadonlyArray<string>,
    filters: ReadonlyArray<NostrFilter>,
    callbacks: SubscribeManyCallbacks,
  ) => PoolSubscription
  /**
   * Open a live subscription across many relays; returns a {@link PoolSubscription} handle. Each relay's leg stays open
   * for live events after its end-of-stored-events, until `unsubscribe()` or the relay closes it.
   */
  readonly subscribeManyLive: (
    rawUrls: ReadonlyArray<string>,
    filters: ReadonlyArray<NostrFilter>,
    callbacks: SubscribeManyCallbacks,
  ) => PoolSubscription
  /**
   * Publish one event to one relay; resolves with that relay's outcome once it replies, the publish times out, or the
   * socket is torn down. It never rejects for a relay or transport reason: a refusal, a timeout, a drop, an unparseable
   * URL, a relay it cannot connect to and a disposed pool all resolve with `ok: false` and a `message` saying which.
   */
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
  /** Close every socket, cancel every timer and drop every listener; later subscribes are inactive and later publishes resolve `"disposed"`. Idempotent. */
  readonly dispose: () => void
}
