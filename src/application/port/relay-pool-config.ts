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
  /** How long a publish waits for its relay `OK` before timing out (ms); suspended while it is parked for AUTH. Default: 8000. */
  readonly publishTimeoutMs?: number
  /** How long a sub may wait for the socket to open before it is dropped (ms). Default: 30000. */
  readonly pendingSubTimeoutMs?: number
  /** How long a connection must stay open before its backoff step resets (ms). Default: 30000. */
  readonly stableConnectionMs?: number
  /** How long a socket with no subscriptions or in-flight publishes stays open before the pool closes it (ms). Default: 30000. */
  readonly idleSocketTimeoutMs?: number
  /** Upper bound on a `subscribeMany` leg's wait for EOSE before forced teardown (ms). Default: 12000. */
  readonly relayConnectionHardTimeoutMs?: number
  /**
   * How long the connection's NIP-42 auth timer runs (ms). It runs while an AUTH answer is in flight —
   * the {@link AuthHandler} producing a signed AUTH event and the relay answering it with `OK` — or
   * while `auth-required` work is parked, and restarts with each answer. When it fires the answer is
   * abandoned and parked publishes and subscriptions are settled with `auth-required: auth timed out`.
   * Generous by default because auth handlers often have a human in the loop (a NIP-07 prompt, a
   * remote NIP-46 approval). Default: 60000.
   */
  readonly authTimeoutMs?: number
  /**
   * How often an open connection sends `["CLOSE", "keepalive"]` so a relay's idle timeout does not
   * drop a quiet subscriber (ms). `0` disables the heartbeat. Default: 30000.
   */
  readonly heartbeatIntervalMs?: number
  /**
   * Largest relay message the pool reads, in UTF-8 bytes. A longer frame is dropped unread, as a frame that is not a
   * relay message is, and the connection stays open. Default: 262144 (256 KiB).
   */
  readonly maxMessageBytes?: number
}
