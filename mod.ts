/**
 * `@innis/nostr-relay-pool` — WebSocket connections to Nostr relays.
 *
 * Pure transport: subscriptions, publishes, NIP-42 AUTH, exponential backoff, and latency
 * tracking. No caching, no event-store knowledge, no relay-selection policy. Events reach your
 * callbacks exactly as each relay sent them; dedup and persistence are your event store's job.
 *
 * ## Public surface
 *
 * Every public symbol is curated through its layer barrel; this file aggregates them with
 * `export *` and does no curation of its own. Adding a new public symbol is a one-line edit to
 * the appropriate layer barrel:
 *
 *   - `src/domain/value-object/mod.ts`    — branded types, public value shapes
 *   - `src/application/port/mod.ts`       — interfaces the package needs from outside
 *   - `src/application/service/mod.ts`    — public orchestration helpers
 *   - `src/infrastructure/adapter/mod.ts` — concrete entry-point factories
 *
 * @module
 */

export * from "./src/domain/value-object/mod.ts"
export * from "./src/application/port/mod.ts"
export * from "./src/application/service/mod.ts"
export * from "./src/infrastructure/adapter/mod.ts"
