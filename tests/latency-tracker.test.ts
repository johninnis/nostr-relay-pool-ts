import { assertEquals } from "@std/assert"
import { relayUrlFixture } from "@innis/nostr-core/testing"
import { createLatencyTracker, type LatencyTracker } from "../src/application/service/latency-tracker.ts"

const url = relayUrlFixture("wss://relay.example.com")

Deno.test("cold relay returns defaultTimeoutMs", () => {
  const t = createLatencyTracker()
  assertEquals(t.suggestedTimeout(url), 4000)
})

Deno.test("below minSamples returns defaultTimeoutMs", () => {
  const t = createLatencyTracker()
  t.record(url, 200)
  t.record(url, 300)
  assertEquals(t.suggestedTimeout(url), 4000)
})

Deno.test("fast relay clamps to defaultTimeoutMs floor", () => {
  const t = createLatencyTracker()
  for (let i = 0; i < 10; i++) t.record(url, 200)
  assertEquals(t.suggestedTimeout(url), 4000)
})

Deno.test("slow-but-valid relay suggests p95 * multiplier", () => {
  const t = createLatencyTracker({
    defaultTimeoutMs: 1000,
    maxTimeoutMs: 20000,
    multiplier: 1.5,
    sampleSize: 20,
    minSamples: 3,
  })
  for (let i = 0; i < 10; i++) t.record(url, 6000)
  assertEquals(t.suggestedTimeout(url), 9000)
})

Deno.test("extremely slow relay clamps to maxTimeoutMs ceiling", () => {
  const t = createLatencyTracker()
  for (let i = 0; i < 10; i++) t.record(url, 30000)
  assertEquals(t.suggestedTimeout(url), 10000)
})

Deno.test("ring buffer evicts oldest samples past sampleSize", () => {
  const t = createLatencyTracker({
    defaultTimeoutMs: 1000,
    maxTimeoutMs: 20000,
    multiplier: 1,
    sampleSize: 5,
    minSamples: 3,
  })
  for (let i = 0; i < 5; i++) t.record(url, 10000)
  for (let i = 0; i < 5; i++) t.record(url, 2000)
  assertEquals(t.suggestedTimeout(url), 2000)
})

Deno.test("p95 interpolates between ranks rather than collapsing to the maximum", () => {
  const t = createLatencyTracker({
    defaultTimeoutMs: 100,
    maxTimeoutMs: 100000,
    multiplier: 1,
    sampleSize: 20,
    minSamples: 3,
  })
  for (let i = 1; i <= 20; i++) t.record(url, i * 100)
  const p95InterpolatedBetween1900And2000 = 1900 + (2000 - 1900) * 0.05
  assertEquals(t.suggestedTimeout(url), p95InterpolatedBetween1900And2000)
})

Deno.test("ignores non-finite and negative samples", () => {
  const t = createLatencyTracker()
  t.record(url, NaN)
  t.record(url, Infinity)
  t.record(url, -100)
  assertEquals(t.suggestedTimeout(url), t.defaultTimeoutMs)
})

const fast = relayUrlFixture("wss://fast.example.com")
const slow = relayUrlFixture("wss://slow.example.com")

const trackerOfFastAndSlowRelays = (): LatencyTracker => {
  const t = createLatencyTracker({
    defaultTimeoutMs: 1000,
    maxTimeoutMs: 20000,
    multiplier: 1.5,
    sampleSize: 20,
    minSamples: 3,
  })
  for (let i = 0; i < 5; i++) {
    t.record(fast, 100)
    t.record(slow, 6000)
  }
  return t
}

Deno.test("tracks samples per relay independently: a fast relay keeps the floor beside a slow one", () => {
  assertEquals(trackerOfFastAndSlowRelays().suggestedTimeout(fast), 1000)
})

Deno.test("tracks samples per relay independently: a slow relay keeps its own p95 beside a fast one", () => {
  assertEquals(trackerOfFastAndSlowRelays().suggestedTimeout(slow), 9000)
})
