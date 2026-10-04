# @innis/nostr-relay-pool

[![CI](https://github.com/johninnis/nostr-relay-pool-ts/actions/workflows/ci.yml/badge.svg)](https://github.com/johninnis/nostr-relay-pool-ts/actions/workflows/ci.yml)

WebSocket connections to Nostr relays. Subscriptions, publishes, NIP-42 AUTH, exponential backoff, and latency tracking. Pure transport — no caching, no event-store knowledge, no relay-selection policy (that lives in `@innis/nostr-relay-selection`). Events are delivered to your callbacks exactly as each relay sent them; dedup and persistence are your event store's job.

The package is organised under Clean Architecture (`src/domain`, `src/application/{port,service}`, `src/infrastructure/{web-socket,time}`). The public surface is re-exported from `mod.ts`; consumers never need to reach into the subdirectories.

## Install

```ts
import { createRelayPool } from "@innis/nostr-relay-pool"
```

## Public surface

### `createRelayPool(config?)` — `infrastructure/web-socket/web-socket-relay-pool.ts`

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
  idleSocketTimeoutMs?: number,             // default: 30 000
  relayConnectionHardTimeoutMs?: number,    // default: 12 000
  authTimeoutMs?: number,                   // default: 60 000
  heartbeatIntervalMs?: number,             // default: 30 000; 0 disables
  maxMessageBytes?: number,                 // default: 262 144 (256 KiB)
})
```

Every URL-shaped parameter on the public surface accepts a raw `string` and is normalised internally via `@innis/nostr-core`'s `parseRelayUrl`.

Returns a `RelayPool` with:

- `subscribe(rawUrl, filters, { onEvent, onEose?, onClosed? })` — single-relay subscription. Returns `{ active, unsubscribe }`; `active` is `false` if the URL was invalid, the pool is disposed, the relay is in backoff cooldown, or the connection gate rejected the URL. `onEose` fires once at end-of-stored-events (the sub stays open for live events); `onClosed(reason)` is the terminal signal — it fires once when the sub is dead and won't be revived, whether the relay *terminated* it via a NIP-01 `CLOSED` (`reason` is the relay's message: rate-limit, shutdown, unsupported filter, or `auth-required:` when the pool cannot authenticate; an AUTH the relay refuses closes it with `auth-required: auth rejected: …`) or the pool tore it down (`reason` is `"disconnected"`, `"disposed"`, or `"connection gate rejected"`). Distinct from `onEose`, which only means the stored backlog ended. Identical filters on the same relay de-duplicate onto one wire subscription; a caller that joins after that wire sub has already EOSE'd gets a synthetic `onEose` but **not** a replay of events already delivered — subscribe before you need the backlog, or use a distinct filter. A filter that can match nothing (an empty list, a `since` after its `until`) is never sent; a subscription none of whose filters can match anything opens no connection and gets `onEose` on a microtask (ADR-0006).
- `subscribeMany(rawUrls, filters, callbacks)` — fetch stored events from many relays: each relay's leg closes at its EOSE (or its adaptive timeout). `callbacks` is `{ onEvent, onRelayEose?, onRelayClosed? }` — the subscription's `EVENT`, `EOSE` and `CLOSED` per relay, the multi-relay counterpart of `subscribe`'s callbacks. Returns `{ unsubscribe, syncUrls }`; `syncUrls(newRawUrls)` swaps the connected set in place.
- `subscribeManyLive(rawUrls, filters, callbacks)` — the same fan-out for a live feed: each leg stays open for live events after its EOSE until `unsubscribe()` or the relay closes it.
- `publish(rawUrl, event)` — publishes one event to one relay; resolves with `{ from, ok, message }`. It never rejects for a relay or transport reason: a refusal (`OK` false, with the relay's message), a timeout, a drop, an unparseable URL, a relay it cannot connect to and a disposed pool are all `ok: false` outcomes.
- `getConnectedRelayUrls()`, `getAttemptedRelayUrls()`, `getRelayPoolState()`, `getRelaySubscriptions(rawUrl)`, `getRelayPublishHistory(rawUrl)` — diagnostics. Each `getRelayPoolState()` entry carries an `eventCount` (events the relay has delivered); entries come back in a stable, intent-free order (connected relays first, then attempt-only, then backoff-disabled). The pool ranks nothing — rank or threshold relays from `eventCount`, latency, or whatever you like in your selection layer; relay-selection policy is not the pool's job.
- `setAuthHandler(handler)` — set the NIP-42 challenge handler after pool construction.
- `clearDisabled(rawUrl)`, `disconnect(rawUrl)`, `setConnectionGate(gate)`, `clearRelayHistory(rawUrl)` — connection control.
- `onConnectionChange(listener)` — subscribe to per-relay connect/disconnect events. Returns an unsubscribe function.
- `suggestedTimeout(rawUrl)` — adaptive timeout from observed EOSE latency (default 4 s, learned upward, capped at 10 s).
- `dispose()` — close every socket, cancel every timer, drop every listener; later `subscribe` calls return an inactive subscription and later `publish` calls resolve with `{ ok: false, message: "disposed" }`. Idempotent.

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

The package exposes these interfaces from `application/port/` so consumers and port implementers can satisfy them:

- `AuthHandler` — `(url, challenge) => Promise<NostrEvent | null>`
- `BackoffPersistence` — `{ write, remove }` for persisting backoff state across reloads
- `WallClock` — `() => number` (milliseconds-since-epoch); default `systemWallClock` is `() => Date.now()`. Distinct from `@innis/nostr-core`'s `Clock` (which returns seconds for protocol timestamps), so the two cannot be silently interchanged. This is the time the pool *reads*.
- `Scheduler` — `{ setTimer, clearTimer }`, the one-shot timers the pool *acts on* (reconnect, backoff, stability, publish/sub timeouts); default `systemScheduler` wraps `setTimeout`/`clearTimeout`. Inject a virtual scheduler alongside a fake `WallClock` to drive every timer deterministically in tests.
- `ConnectionPool` — the small slice of the pool (`subscribe`, `suggestedTimeout`) the internal subscription fan-out drives. It takes already-normalised `RelayUrl`s rather than raw strings: a relay URL is normalised exactly once, at the public boundary, then passed through internally as a branded value
- `RelayConfig`, `RelayPool`, `RelayPoolConfig`

## Backoff schedule

500 ms → 1 s → 2 s → 5 s → 15 s → 1 m → 5 m → 30 m → 2 h → 24 h. `BackoffPersistence` is an injectable port (write/remove records); the application persists via localStorage (or whatever) so backoff survives reload.

## URL normalisation

`RelayUrl` and `parseRelayUrl` come from `@innis/nostr-core`. The pool normalises raw strings at every public boundary (`subscribe`, `publish`, `disconnect`, etc.) and stores branded `RelayUrl` internally.

## Connection lifecycle

1. `pool.subscribe(url, ...)` opens a WebSocket if one isn't already open.
2. The pool tracks each relay's connection and auth state, plus a stability timer (default 30 s) that fires `backoff.recordSuccess(url)` once the connection has held — so a flapping relay never gets its backoff schedule reset. A relay that dropped while it still had subscriptions is surfaced as `reconnecting` in `getRelayPoolState()` until its scheduled reconnect fires.
3. On disconnect, the pool reconnects with exponential backoff. The reconnect lives only as long as its subscriptions do: unsubscribing the last one during the backoff window cancels it.
4. A socket left with no subscriptions and no in-flight publishes closes after `idleSocketTimeoutMs` (default 30 s); any new subscribe or publish within the window disarms the close. A close the pool initiates is not a relay failure, so it records no backoff penalty.
5. NIP-42 AUTH: the pool keeps the relay's latest challenge and, when an auth handler is set, answers it by calling `onAuthChallenge(url, challenge)`; the application returns a signed kind-22242 event or `null`. A publish refused with `auth-required:` and a subscription `CLOSED` with `auth-required:` are parked only while a handler can still authenticate the connection; the relay's `OK true` for the AUTH event resends the parked events and re-issues the parked subscriptions. An `auth-required:` reply to the AUTH event itself means the relay issued a fresh challenge, which the pool answers; any other refusal resolves parked publishes and closes parked subscriptions with `auth-required: auth rejected: <relay's reason>`. A handler that returns `null` declines: parked work resolves and closes with `auth-required: auth declined`. With no handler, after a decline until the relay's next challenge, or once the relay has already accepted an AUTH, an `auth-required:` refusal is the outcome. One auth timer per connection, `authTimeoutMs` (default 60 s), runs while an AUTH answer (handler plus the relay's `OK`) is in flight or work is parked, and restarts with each answer; when it fires the answer is abandoned, the relay's next challenge retries, and parked publishes resolve and parked subscriptions close with `auth-required: auth timed out`. Design records: `docs/adr/`.
6. Publish results: relays that reply with an `OK` ack within `publishTimeoutMs` resolve the publish; otherwise it times out. While a publish is parked for AUTH its own timeout is suspended and the auth timeout bounds it; the resend starts `publishTimeoutMs` again. A publish still awaiting its ack when the socket drops settles immediately as `{ ok: false, message: "disconnected" }` rather than waiting out the timeout — the event is not re-sent on reconnect.
7. Keep-alive: every open socket sends `["CLOSE", "keepalive"]` each `heartbeatIntervalMs` (default 30 s, `0` disables) so an idle-timeout relay does not drop a quiet subscriber, and a `NOTICE` whose text is `ping` is answered with the same frame.
8. Message size: a frame from a relay longer than `maxMessageBytes` UTF-8 bytes (default 256 KiB) is dropped before it is parsed, as a frame that is not a relay message is; the connection stays open and later frames are read. A relay that sends larger events than the default needs a host that raises it.
9. EOSE timeouts are *adaptive* per relay — `suggestedTimeout(url)` reflects what the latency tracker has learned.

## Disposal

```ts
const pool = createRelayPool()
// ...later
pool.dispose()
```

`dispose()` closes every socket, cancels every pending timer (stability, idle, publish, sub, reconnect), drops every connection-change listener, and switches the pool into a state where `subscribe` returns an inactive subscription and `publish` resolves with `{ ok: false, message: "disposed" }`. Idempotent. Tests and short-lived hosts should always call it.

## Anti-patterns

- **Awaiting a publish then gating UI on the result.** Publishes can take seconds. Optimistically update; reconcile on the publish promise.
- **Persisting raw `BackoffRecord`s without going through `BackoffPersistence`.** The pool owns the schedule; if you want backoff to survive reload, supply the persistence port.
- **Holding subscription handles past their useful life.** Every `subscribe` / `subscribeMany` / `subscribeManyLive` handle holds a wire subscription open until you call its `unsubscribe()`. Long-running views must release them on teardown.
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
    web-socket/     — the WebSocket-backed pool and all the plumbing it composes
                      (web-socket-relay-pool, socket-manager, subscribe, publish,
                       relay-state, relay-message-handler, pool-state-projection,
                       wire-sub, web-socket-helpers)
    time/           — the system defaults for the time ports
                      (system-scheduler, system-wall-clock)
```

Infrastructure is grouped by concern, not by pattern. `web-socket/` holds everything that touches a raw `WebSocket` or mutates relay transport state — `createRelayPool` (the composition root that wires it all together) and every factory and helper it calls. `time/` holds the two host-backed defaults for the `Scheduler` and `WallClock` ports.
