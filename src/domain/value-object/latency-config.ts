/** Tuning knobs for the adaptive EOSE-latency timeout. See {@link LatencyTrackerOverrides}. */
export interface LatencyTrackerConfig {
  /** Timeout returned before enough samples exist, and the floor every suggestion is clamped up to (ms). */
  readonly defaultTimeoutMs: number
  /** Hard ceiling every suggested timeout is clamped down to (ms). */
  readonly maxTimeoutMs: number
  /** Safety factor applied to the observed p95 latency to derive the suggested timeout. */
  readonly multiplier: number
  /** Size of the per-relay ring buffer of recent latency samples. */
  readonly sampleSize: number
  /** Minimum samples a relay must accrue before its measured latency overrides `defaultTimeoutMs`. */
  readonly minSamples: number
}

/** Partial {@link LatencyTrackerConfig} passed as `latency` to override individual defaults. */
export type LatencyTrackerOverrides = Partial<LatencyTrackerConfig>
