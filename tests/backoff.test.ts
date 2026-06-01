import { assertEquals } from "@std/assert"
import type { RelayUrl } from "@innis/nostr-core"
import { parseRelayUrl } from "@innis/nostr-core"
import type { BackoffRecord } from "../src/domain/value-object/backoff-record.ts"
import type { BackoffPersistence } from "../src/application/port/backoff-persistence.ts"
import { systemWallClock } from "../src/infrastructure/adapter/system-wall-clock-adapter.ts"
import { createBackoffTracker } from "../src/application/service/backoff-tracker.ts"

const url = parseRelayUrl("wss://example.com")
const other = parseRelayUrl("wss://other.example.com")
const noop = (): void => {}

const createRecordingPersistence = (): BackoffPersistence & {
  readonly writes: ReadonlyArray<BackoffRecord>
  readonly removes: ReadonlyArray<RelayUrl>
} => {
  const writes: Array<BackoffRecord> = []
  const removes: Array<RelayUrl> = []
  return {
    write: (record: BackoffRecord): void => {
      writes.push(record)
    },
    remove: (u: RelayUrl): void => {
      removes.push(u)
    },
    get writes(): ReadonlyArray<BackoffRecord> {
      return writes
    },
    get removes(): ReadonlyArray<RelayUrl> {
      return removes
    },
  }
}

Deno.test("first failure schedules ~500ms disable window", () => {
  const tracker = createBackoffTracker({ onChange: noop, clock: systemWallClock })
  const before = Date.now()
  tracker.recordFailure(url)
  const until = tracker.disabledUntil(url)
  assertEquals(until !== null, true)
  const window = (until ?? 0) - before
  assertEquals(window >= 500 && window <= 600, true)
})

Deno.test("repeated failures escalate the schedule", () => {
  const tracker = createBackoffTracker({ onChange: noop, clock: systemWallClock })
  const windows: Array<number> = []
  for (let i = 0; i < 5; i++) {
    const before = Date.now()
    tracker.recordFailure(url)
    windows.push((tracker.disabledUntil(url) ?? 0) - before)
  }
  for (let i = 1; i < windows.length; i++) {
    const prev = windows[i - 1] ?? 0
    const curr = windows[i] ?? 0
    assertEquals(curr > prev, true)
  }
})

Deno.test("recordSuccess resets the step", () => {
  const tracker = createBackoffTracker({ onChange: noop, clock: systemWallClock })
  tracker.recordFailure(url)
  tracker.recordFailure(url)
  tracker.recordSuccess(url)
  const before = Date.now()
  tracker.recordFailure(url)
  const window = (tracker.disabledUntil(url) ?? 0) - before
  assertEquals(window >= 500 && window <= 600, true)
})

Deno.test("isDisabled returns true while window is active and false after expiry", () => {
  const tracker = createBackoffTracker({ onChange: noop, clock: systemWallClock })
  tracker.recordFailure(url)
  assertEquals(tracker.isDisabled(url), true)
  const until = tracker.disabledUntil(url) ?? 0
  const remaining = until - Date.now()
  assertEquals(remaining > 0 && remaining <= 600, true)
})

Deno.test("disabledUntil is pure: does not mutate after expiry", () => {
  let now = 1_000_000
  const clock = (): number => now
  const tracker = createBackoffTracker({
    onChange: noop,
    clock,
    initial: [{ url, disabledUntil: now + 100, step: 1 }],
  })
  assertEquals(tracker.disabledUntil(url), now + 100)
  now += 500
  assertEquals(tracker.disabledUntil(url), null)
  assertEquals(tracker.isDisabled(url), false)
  // The hydrated step should still be intact: a new failure jumps to step 2's window.
  tracker.recordFailure(url)
  const window = (tracker.disabledUntil(url) ?? 0) - now
  assertEquals(window >= 1_000 && window <= 1_100, true)
})

Deno.test("clear removes disable window and step", () => {
  const tracker = createBackoffTracker({ onChange: noop, clock: systemWallClock })
  tracker.recordFailure(url)
  tracker.recordFailure(url)
  tracker.clear(url)
  assertEquals(tracker.disabledUntil(url), null)
  assertEquals(tracker.isDisabled(url), false)
  const before = Date.now()
  tracker.recordFailure(url)
  const window = (tracker.disabledUntil(url) ?? 0) - before
  assertEquals(window >= 500 && window <= 600, true)
})

Deno.test("hydrates from initial snapshot preserving disabledUntil and step", () => {
  const futureUntil = Date.now() + 60_000
  const initial: ReadonlyArray<BackoffRecord> = [
    { url, disabledUntil: futureUntil, step: 3 },
  ]
  const tracker = createBackoffTracker({ onChange: noop, clock: systemWallClock, initial })
  assertEquals(tracker.isDisabled(url), true)
  assertEquals(tracker.disabledUntil(url), futureUntil)
})

Deno.test("hydrated step continues the backoff sequence", () => {
  const initial: ReadonlyArray<BackoffRecord> = [
    { url, disabledUntil: Date.now() + 1, step: 3 },
  ]
  const tracker = createBackoffTracker({ onChange: noop, clock: systemWallClock, initial })
  const before = Date.now()
  tracker.recordFailure(url)
  const window = (tracker.disabledUntil(url) ?? 0) - before
  assertEquals(window >= 5_000 && window <= 5_100, true)
})

Deno.test("recordFailure writes to persistence", () => {
  const persistence = createRecordingPersistence()
  const tracker = createBackoffTracker({ onChange: noop, clock: systemWallClock, persistence })
  tracker.recordFailure(url)
  assertEquals(persistence.writes.length, 1)
  assertEquals(persistence.writes[0]?.url, url)
  assertEquals(persistence.writes[0]?.step, 1)
})

Deno.test("clear removes from persistence", () => {
  const persistence = createRecordingPersistence()
  const tracker = createBackoffTracker({ onChange: noop, clock: systemWallClock, persistence })
  tracker.recordFailure(url)
  tracker.clear(url)
  assertEquals(persistence.removes, [url])
})

Deno.test("recordSuccess removes from persistence when nothing left", () => {
  const persistence = createRecordingPersistence()
  const past = Date.now() - 1
  const tracker = createBackoffTracker({
    onChange: noop,
    clock: systemWallClock,
    initial: [{ url, disabledUntil: past, step: 2 }],
    persistence,
  })
  tracker.recordSuccess(url)
  assertEquals(persistence.removes.includes(url), true)
})

Deno.test("isDisabled after expiry preserves step in persistence", async () => {
  const persistence = createRecordingPersistence()
  const tracker = createBackoffTracker({
    onChange: noop,
    clock: systemWallClock,
    initial: [{ url, disabledUntil: Date.now() + 20, step: 1 }],
    persistence,
  })
  assertEquals(tracker.isDisabled(url), true)
  await new Promise((r) => setTimeout(r, 40))
  assertEquals(tracker.isDisabled(url), false)
  assertEquals(persistence.removes, [])
  tracker.recordFailure(url)
  assertEquals(persistence.writes.at(-1)?.step, 2)
})

Deno.test("hydration ignores urls whose window already expired", () => {
  const tracker = createBackoffTracker({
    onChange: noop,
    clock: systemWallClock,
    initial: [
      { url, disabledUntil: Date.now() - 1_000, step: 2 },
      { url: other, disabledUntil: Date.now() + 60_000, step: 1 },
    ],
  })
  assertEquals(tracker.isDisabled(url), false)
  assertEquals(tracker.isDisabled(other), true)
})
