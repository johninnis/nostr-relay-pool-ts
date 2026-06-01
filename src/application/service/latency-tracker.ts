import type { RelayUrl } from "@innis/nostr-core"
import type { LatencyTrackerConfig, LatencyTrackerOverrides } from "../../domain/value-object/latency-config.ts"

export interface LatencyTracker {
  readonly record: (url: RelayUrl, ms: number) => void
  readonly suggestedTimeout: (url: RelayUrl) => number
  readonly defaultTimeoutMs: number
}

const DEFAULTS: LatencyTrackerConfig = Object.freeze({
  defaultTimeoutMs: 4_000,
  maxTimeoutMs: 10_000,
  multiplier: 1.5,
  sampleSize: 20,
  minSamples: 3,
})

// Linear-interpolated percentile (the R-7 / Excel PERCENTILE.INC method). Interpolating between
// the two nearest ranks keeps small sample sets honest — a plain nearest-rank index collapses to
// the maximum for any set of 20 or fewer, which is what `sampleSize` caps at.
const percentile = (sortedAscending: ReadonlyArray<number>, fraction: number): number => {
  const rank = (sortedAscending.length - 1) * fraction
  const lowerIndex = Math.floor(rank)
  const upperIndex = Math.ceil(rank)
  const lower = sortedAscending[lowerIndex] ?? 0
  const upper = sortedAscending[upperIndex] ?? lower
  return lower + (upper - lower) * (rank - lowerIndex)
}

// A bounded ring of the most recent samples. `writeIndex` marks the next slot to overwrite once the
// ring is full; until then samples accumulate by push. suggestedTimeout sorts a copy before reading,
// so the ring's physical order never matters — overwriting oldest in place keeps record() O(1).
interface SampleRing {
  readonly values: Array<number>
  writeIndex: number
}

export const createLatencyTracker = (overrides: LatencyTrackerOverrides = {}): LatencyTracker => {
  const { defaultTimeoutMs, maxTimeoutMs, multiplier, sampleSize, minSamples } = { ...DEFAULTS, ...overrides }
  const samples = new Map<RelayUrl, SampleRing>()

  const record = (url: RelayUrl, ms: number): void => {
    if (!Number.isFinite(ms) || ms < 0) return
    let ring = samples.get(url)
    if (!ring) {
      ring = { values: [], writeIndex: 0 }
      samples.set(url, ring)
    }
    if (ring.values.length < sampleSize) {
      ring.values.push(ms)
    } else {
      ring.values[ring.writeIndex] = ms
      ring.writeIndex = (ring.writeIndex + 1) % sampleSize
    }
  }

  const suggestedTimeout = (url: RelayUrl): number => {
    const ring = samples.get(url)
    if (!ring || ring.values.length < minSamples) return defaultTimeoutMs
    const sorted = [...ring.values].sort((a, b) => a - b)
    const suggested = percentile(sorted, 0.95) * multiplier
    return Math.min(maxTimeoutMs, Math.max(defaultTimeoutMs, suggested))
  }

  return Object.freeze({ record, suggestedTimeout, defaultTimeoutMs })
}
