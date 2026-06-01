# @innis/nostr-relay-pool

[![CI](https://github.com/johninnis/nostr-relay-pool-ts/actions/workflows/ci.yml/badge.svg)](https://github.com/johninnis/nostr-relay-pool-ts/actions/workflows/ci.yml)

WebSocket connections to Nostr relays. Subscriptions, publishes, NIP-42 AUTH, exponential backoff, and latency tracking. Pure transport — no caching, no event-store knowledge, no relay-selection policy (that lives in `@innis/nostr-relay-selection`). Events are delivered to your callbacks exactly as each relay sent them; dedup and persistence are your event store's job.

The package is organised under Clean Architecture (`src/domain`, `src/application/{port,service}`, `src/infrastructure/{adapter,web-socket}`). The public surface is re-exported from `mod.ts`; consumers never need to reach into the subdirectories.

## Install

```ts
import { createRelayPool } from "@innis/nostr-relay-pool"
```

## Public surface

### `createRelayPool(config?)` — `infrastructure/adapter/web-socket-relay-pool-adapter.ts`

```ts
const pool = createRelayPool({
  onAuthChallenge?: AuthHandler,            // (url, challenge) => Promise<NostrEvent | null>
  latency?: LatencyTrackerOverrides,
  initialBackoff?: ReadonlyArray<BackoffRecord>,
  backoffPersistence?: BackoffPersistence,
  clock?: WallClock,                        // default: () => Date.now() (ms since epoch)
  scheduler?: Scheduler,                    // default: systemScheduler (setTimeout/clearTimeout)
  publishTimeoutMs?: number,                // default: 8 000
  pendingSubTimeoutMs?: number,             // default: 30 000
  stableConnectionMs?: number,              // default: 30 000
  relayConnectionHardTimeoutMs?: number,    // default: 12 000
})
```

Every URL-shaped parameter on the public surface accepts a raw `string` and is normalised internally via `@innis/nostr-core`'s `normaliseRelayUrl`.

Returns a `RelayPool` with:

- `subscribe(rawUrl, filters, { onEvent, onEose?, onClosed? })` — single-relay subscription. Returns `{ active, unsubscribe }`; `active` is `false` if the URL was invalid, the pool is disposed, the relay is in backoff cooldown, or the connection gate rejected the URL. `onEose` fires once at end-of-stored-events (the sub stays open for live events); `onClosed(reason)` is the terminal signal — it fires once when the sub is dead and won't be revived, whether the relay *terminated* it via a non-auth NIP-01 `CLOSED` (`reason` is the relay's message: rate-limit, shutdown, unsupported filter) or the pool tore it down (`reason` is `"disconnected"`, `"disposed"`, or `"connection gate rejected"`). Distinct from `onEose`, which only means the stored backlog ended. Identical filters on the same relay de-duplicate onto one wire subscription; a caller that joins after that wire sub has already EOSE'd gets a synthetic `onEose` but **not** a replay of events already delivered — subscribe before you need the backlog, or use a distinct filter.
- `subscribeMany(rawUrls, filters, callbacks, options?)` — fan-out across many relays; `callbacks` is `{ onEvent, onRelayEose?, onRelayClosed? }` and `options` is `{ persistent? }`. `persistent` (default `false`) keeps each leg open for live events after EOSE. Returns `{ unsubscribe, syncUrls }`; `syncUrls(newRawUrls)` swaps the connected set in place.
- `publish(rawUrl, event)` — publishes one event to one relay; resolves with `{ from, ok, message }`.
- `getConnectedRelayUrls()`, `getAttemptedRelayUrls()`, `getRelayPoolState()`, `getRelaySubscriptions(rawUrl)`, `getRelayPublishHistory(rawUrl)` — diagnostics. Each `getRelayPoolState()` entry carries an `eventCount` (events the relay has delivered); entries come back in a stable, intent-free order (connected relays first, then attempt-only, then backoff-disabled). The pool ranks nothing — rank or threshold relays from `eventCount`, latency, or whatever you like in your selection layer; relay-selection policy is not the pool's job.
- `setAuthHandler(handler)` — set the NIP-42 challenge handler after pool construction.
- `clearDisabled(rawUrl)`, `disconnect(rawUrl)`, `setConnectionGate(gate)`, `clearRelayHistory(rawUrl)` — connection control.
- `onConnectionChange(listener)` — subscribe to per-relay connect/disconnect events. Returns an unsubscribe function.
- `suggestedTimeout(rawUrl)` — adaptive timeout from observed EOSE latency (default 4 s, learned upward, capped at 10 s).
- `dispose()` — close every socket, cancel every timer, drop every listener, and reject all further `subscribe` / `publish` calls. Idempotent.

### `createRelayConfig(pool)` — `application/service/relay-config.ts`

```ts
const config = createRelayConfig(pool)
config.setAllowedRelays([...])
config.setRestrictedMode(true)
config.getAllowedRelays()
config.isRestrictedMode()
```

A small wrapper that drives `pool.setConnectionGate`. When `restricted` is true and `allowedRelays` is non-empty, the gate admits only URLs in the set; otherwise everything is allowed. The library has no opinion on what those relays *represent* — the application supplies them.

## Ports

The package exposes these interfaces from `application/port/` so consumers and adapter implementers can satisfy them:

- `AuthHandler` — `(url, challenge) => Promise<NostrEvent | null>`
- `BackoffPersistence` — `{ write, remove }` for persisting backoff state across reloads
- `WallClock` — `() => number` (milliseconds-since-epoch); default `systemWallClock` is `() => Date.now()`. Distinct from `@innis/nostr-core`'s `Clock` (which returns seconds for protocol timestamps), so the two cannot be silently interchanged. This is the time the pool *reads*.
- `Scheduler` — `{ setTimer, clearTimer }`, the one-shot timers the pool *acts on* (reconnect, backoff, stability, publish/sub timeouts); default `systemScheduler` wraps `setTimeout`/`clearTimeout`. Inject a virtual scheduler alongside a fake `WallClock` to drive every timer deterministically in tests.
- `ConnectionPool` — the small slice of the pool (`subscribe`, `suggestedTimeout`) the internal subscription fan-out drives. It takes already-normalised `RelayUrl`s rather than raw strings: a relay URL is normalised exactly once, at the public boundary, then passed through internally as a branded value
- `RelayConfig`, `RelayPool`, `RelayPoolConfig`

## Backoff schedule

500 ms → 1 s → 2 s → 5 s → 15 s → 1 m → 5 m → 30 m → 2 h → 24 h. `BackoffPersistence` is an injectable port (write/remove records); the application persists via localStorage (or whatever) so backoff survives reload.

## URL normalisation

`RelayUrl` and `normaliseRelayUrl` come from `@innis/nostr-core`. The pool normalises raw strings at every public boundary (`subscribe`, `publish`, `disconnect`, etc.) and stores branded `RelayUrl` internally.

## Connection lifecycle

1. `pool.subscribe(url, ...)` opens a WebSocket if one isn't already open.
2. The pool tracks each relay's connection and auth state, plus a stability timer (default 30 s) that fires `backoff.recordSuccess(url)` once the connection has held — so a flapping relay never gets its backoff schedule reset. A relay that dropped while it still had subscriptions is surfaced as `reconnecting` in `getRelayPoolState()` until its scheduled reconnect fires.
3. On disconnect, the pool reconnects with exponential backoff.
4. NIP-42 AUTH challenges are queued: subscriptions and publishes wait for AUTH to complete. The pool calls `onAuthChallenge(url, challenge)`; the application returns a signed kind-22242 event or `null`.
5. Publish results: relays that reply with an `OK` ack within `publishTimeoutMs` resolve the publish; otherwise it times out. A publish still awaiting its ack when the socket drops settles immediately as `{ ok: false, message: "disconnected" }` rather than waiting out the timeout — the event is not re-sent on reconnect.
6. EOSE timeouts are *adaptive* per relay — `suggestedTimeout(url)` reflects what the latency tracker has learned.

## Disposal

```ts
const pool = createRelayPool()
// ...later
pool.dispose()
```

`dispose()` closes every socket, cancels every pending timer (stability, publish, sub, reconnect), drops every connection-change listener, and switches the pool into a state where `subscribe` returns an inactive subscription and `publish` resolves with `{ ok: false, message: "disposed" }`. Idempotent. Tests and short-lived hosts should always call it.

## Anti-patterns

- **Awaiting a publish then gating UI on the result.** Publishes can take seconds. Optimistically update; reconcile on the publish promise.
- **Persisting raw `BackoffRecord`s without going through `BackoffPersistence`.** The pool owns the schedule; if you want backoff to survive reload, supply the persistence port.
- **Holding subscription handles past their useful life.** Every `subscribe` / `subscribeMany` handle holds a wire subscription open until you call its `unsubscribe()`. Long-running views must release them on teardown.
- **Forgetting `dispose()`.** Pools register timers and sockets. The disposer cleans all of it.

## Tests

Integration tests use the in-memory relay from `testing.ts` — a real WebSocket Nostr relay implementation that the pool can connect to. Don't mock the pool; spin up an in-memory relay and let the pool talk to it.

```ts
import { createInMemoryRelay } from "@innis/nostr-relay-pool/testing"

const relay = createInMemoryRelay()
await relay.start()
const pool = createRelayPool()
try {
  // ... exercise the pool against `relay.url` ...
} finally {
  pool.dispose()
  await relay.stop()
}
```

Deno's `sanitizeOps` / `sanitizeResources` are left **on** in every test — the disposal API is correct, so resource leaks would surface as test failures.

## Layout

```
src/
  domain/
    value-object/   — branded types, public/internal type aliases
  application/
    port/           — interfaces the application needs from outside
                      (AuthHandler, BackoffPersistence, Clock, Scheduler, ConnectionPool,
                       RelayPool, RelayConfig, RelayPoolConfig)
    service/        — orchestration services
                      (backoff-tracker, latency-tracker, relay-history,
                       relay-config, relay-connection, subscribe-many)
  infrastructure/
    adapter/        — port implementations (the composition root + the system defaults)
                      (web-socket-relay-pool-adapter,
                       system-scheduler-adapter, system-wall-clock-adapter)
    web-socket/     — all WebSocket plumbing the adapter composes
                      (socket-manager, subscribe, publish, relay-state,
                       relay-message-handler, pool-state-projection,
                       wire-sub, web-socket-helpers)
```

`adapter/` holds only true adapters — `createRelayPool` (the composition root that wires everything
together) and the two system port defaults. Everything that touches a raw `WebSocket` or mutates
relay transport state lives under `web-socket/`, regardless of whether it's a factory the adapter
calls (`socket-manager`, `subscribe`, `publish`) or a lower-level helper.
