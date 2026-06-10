import type { AuthHandler } from "./auth-handler.ts"
import type { BackoffPersistence } from "./backoff-persistence.ts"
import type { WallClock } from "./clock.ts"
import type { Scheduler } from "./scheduler.ts"
import type { BackoffRecord } from "../../domain/value-object/backoff-record.ts"
import type { LatencyTrackerOverrides } from "../../domain/value-object/latency-config.ts"

/** Construction options for {@link createRelayPool}; every field is optional and has a default. */
export interface RelayPoolConfig {
  /** NIP-42 challenge responder; also settable later via `setAuthHandler`. Default: none. */
  readonly onAuthChallenge?: AuthHandler
  /** Overrides for the adaptive latency timeout. Default: built-in {@link LatencyTrackerConfig} values. */
  readonly latency?: LatencyTrackerOverrides
  /** Cooldown records to restore on construction (e.g. persisted across a restart). Default: none. */
  readonly initialBackoff?: ReadonlyArray<BackoffRecord>
  /** Sink that persists cooldown changes for later replay via `initialBackoff`. Default: none. */
  readonly backoffPersistence?: BackoffPersistence
  /** Wall-clock source (ms since epoch). Default: {@link systemWallClock}. */
  readonly clock?: WallClock
  /** Timer source. Default: {@link systemScheduler}. */
  readonly scheduler?: Scheduler
  /** How long a publish waits for its relay `OK` before timing out (ms). Default: 8000. */
  readonly publishTimeoutMs?: number
  /** How long a sub may wait for the socket to open before it is dropped (ms). Default: 30000. */
  readonly pendingSubTimeoutMs?: number
  /** How long a connection must stay open before its backoff step resets (ms). Default: 30000. */
  readonly stableConnectionMs?: number
  /** How long a socket with no subscriptions or in-flight publishes stays open before the pool closes it (ms). Default: 30000. */
  readonly idleSocketTimeoutMs?: number
  /** Upper bound on a `subscribeMany` leg's wait for EOSE before forced teardown (ms). Default: 12000. */
  readonly relayConnectionHardTimeoutMs?: number
}
