import { assertEquals } from "@std/assert"
import type { RelayUrl } from "@innis/nostr-core"
import { relayUrlFixture } from "@innis/nostr-core/testing"
import type { BackoffRecord } from "../src/domain/value-object/backoff-record.ts"
import type { BackoffPersistence } from "../src/application/port/backoff-persistence.ts"
import { systemWallClock } from "../src/infrastructure/time/system-wall-clock.ts"
import { type BackoffTracker, createBackoffTracker } from "../src/application/service/backoff-tracker.ts"
import { createManualTime, type ManualTime } from "./_helpers/scheduler.ts"

const url = relayUrlFixture("wss://example.com")
const other = relayUrlFixture("wss://other.example.com")
const noop = (): void => {}
const START = 1_000_000

const trackerOn = (time: ManualTime, initial: ReadonlyArray<BackoffRecord> = []): BackoffTracker =>
  createBackoffTracker({ onChange: noop, clock: time.clock, initial })

const windowOfNextFailure = (tracker: BackoffTracker, time: ManualTime): number => {
  tracker.recordFailure(url)
  return (tracker.disabledUntil(url) ?? 0) - time.clock()
}

const failedOnce = (time: ManualTime): BackoffTracker => {
  const tracker = trackerOn(time)
  tracker.recordFailure(url)
  return tracker
}

const clearedAfterTwoFailures = (time: ManualTime): BackoffTracker => {
  const tracker = trackerOn(time)
  tracker.recordFailure(url)
  tracker.recordFailure(url)
  tracker.clear(url)
  return tracker
}

const hydratedStepOneReadAfterExpiry = (time: ManualTime): BackoffTracker => {
  const tracker = trackerOn(time, [{ url, disabledUntil: START + 100, step: 1 }])
  time.tick(500)
  tracker.disabledUntil(url)
  tracker.isDisabled(url)
  return tracker
}

const persistedAfterExpiry = (): {
  readonly persistence: ReturnType<typeof createRecordingPersistence>
  readonly tracker: BackoffTracker
} => {
  const time = createManualTime(START)
  const persistence = createRecordingPersistence()
  const tracker = createBackoffTracker({
    onChange: noop,
    clock: time.clock,
    initial: [{ url, disabledUntil: START + 20, step: 1 }],
    persistence,
  })
  time.tick(40)
  tracker.isDisabled(url)
  return { persistence, tracker }
}

const hydratedWithOneExpired = (): BackoffTracker =>
  trackerOn(createManualTime(START), [
    { url, disabledUntil: START - 1_000, step: 2 },
    { url: other, disabledUntil: START + 60_000, step: 1 },
  ])

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

Deno.test("first failure disables the relay until 500ms later", () => {
  const time = createManualTime(START)
  assertEquals(failedOnce(time).disabledUntil(url), START + 500)
})

Deno.test("repeated failures escalate the schedule", () => {
  const time = createManualTime(START)
  const tracker = trackerOn(time)
  const windows = Array.from({ length: 5 }, () => windowOfNextFailure(tracker, time))
  assertEquals(windows, [500, 1_000, 2_000, 5_000, 15_000])
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

Deno.test("isDisabled is true while the window is active", () => {
  assertEquals(failedOnce(createManualTime(START)).isDisabled(url), true)
})

Deno.test("isDisabled is false once the window expires", () => {
  const time = createManualTime(START)
  const tracker = failedOnce(time)
  time.tick(500)
  assertEquals(tracker.isDisabled(url), false)
})

Deno.test("disabledUntil is null once a hydrated window expires", () => {
  assertEquals(hydratedStepOneReadAfterExpiry(createManualTime(START)).disabledUntil(url), null)
})

Deno.test("isDisabled is false once a hydrated window expires", () => {
  assertEquals(hydratedStepOneReadAfterExpiry(createManualTime(START)).isDisabled(url), false)
})

Deno.test("disabledUntil is pure: reading an expired window keeps the hydrated step for the next failure", () => {
  const time = createManualTime(START)
  const tracker = hydratedStepOneReadAfterExpiry(time)
  assertEquals(windowOfNextFailure(tracker, time), 1_000)
})

Deno.test("clear removes the disable window", () => {
  assertEquals(clearedAfterTwoFailures(createManualTime(START)).disabledUntil(url), null)
})

Deno.test("clear re-enables the relay", () => {
  assertEquals(clearedAfterTwoFailures(createManualTime(START)).isDisabled(url), false)
})

Deno.test("clear resets the step so the next failure starts the schedule again", () => {
  const time = createManualTime(START)
  assertEquals(windowOfNextFailure(clearedAfterTwoFailures(time), time), 500)
})

Deno.test("hydration disables a relay whose snapshot window is still open", () => {
  const tracker = trackerOn(createManualTime(START), [{ url, disabledUntil: START + 60_000, step: 3 }])
  assertEquals(tracker.isDisabled(url), true)
})

Deno.test("hydration preserves the snapshot's disabledUntil", () => {
  const tracker = trackerOn(createManualTime(START), [{ url, disabledUntil: START + 60_000, step: 3 }])
  assertEquals(tracker.disabledUntil(url), START + 60_000)
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

Deno.test("recordFailure writes the relay's first-step record to persistence", () => {
  const persistence = createRecordingPersistence()
  const tracker = createBackoffTracker({ onChange: noop, clock: () => START, persistence })
  tracker.recordFailure(url)
  assertEquals(persistence.writes, [
    { url, disabledUntil: START + 500, step: 1, lastFailure: { code: null, reason: "", at: START } },
  ])
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

Deno.test("isDisabled after expiry removes nothing from persistence", () => {
  assertEquals(persistedAfterExpiry().persistence.removes, [])
})

Deno.test("isDisabled after expiry preserves the step the next failure persists", () => {
  const { persistence, tracker } = persistedAfterExpiry()
  tracker.recordFailure(url)
  assertEquals(persistence.writes.at(-1)?.step, 2)
})

Deno.test("hydration ignores a url whose window already expired", () => {
  assertEquals(hydratedWithOneExpired().isDisabled(url), false)
})

Deno.test("hydration keeps a url whose window is still open beside one that expired", () => {
  assertEquals(hydratedWithOneExpired().isDisabled(other), true)
})
