import type { RelayUrl } from "@innis/nostr-core"
import type { BackoffFailureInfo, BackoffRecord } from "../../domain/value-object/backoff-record.ts"
import type { BackoffPersistence } from "../port/backoff-persistence.ts"
import type { WallClock } from "../port/clock.ts"

const BACKOFF_SCHEDULE_MS: ReadonlyArray<number> = [
  500,
  1_000,
  2_000,
  5_000,
  15_000,
  60_000,
  5 * 60_000,
  30 * 60_000,
  2 * 3_600_000,
  24 * 3_600_000,
]

const FALLBACK_BACKOFF_MS = 60_000

export interface BackoffTracker {
  readonly isDisabled: (url: RelayUrl) => boolean
  readonly disabledUntil: (url: RelayUrl) => number | null
  readonly getDisabledUrls: () => ReadonlyArray<RelayUrl>
  readonly getLastFailure: (url: RelayUrl) => BackoffFailureInfo | null
  readonly recordFailure: (url: RelayUrl, info?: { code?: number | null; reason?: string }) => void
  readonly recordSuccess: (url: RelayUrl) => void
  readonly clear: (url: RelayUrl) => void
}

export interface BackoffTrackerOptions {
  readonly onChange: () => void
  readonly clock: WallClock
  readonly initial?: ReadonlyArray<BackoffRecord>
  readonly persistence?: BackoffPersistence
}

export const createBackoffTracker = (options: BackoffTrackerOptions): BackoffTracker => {
  const { onChange, clock, initial, persistence } = options
  const disabledUntilByUrl = new Map<RelayUrl, number>()
  const stepByUrl = new Map<RelayUrl, number>()
  const lastFailureByUrl = new Map<RelayUrl, BackoffFailureInfo>()

  if (initial) {
    const now = clock()
    for (const record of initial) {
      if (record.disabledUntil > now) disabledUntilByUrl.set(record.url, record.disabledUntil)
      if (record.step > 0) stepByUrl.set(record.url, record.step)
      if (record.lastFailure) lastFailureByUrl.set(record.url, record.lastFailure)
    }
  }

  const disabledUntil = (url: RelayUrl): number | null => {
    const until = disabledUntilByUrl.get(url)
    if (until === undefined) return null
    if (clock() >= until) return null
    return until
  }

  const isDisabled = (url: RelayUrl): boolean => disabledUntil(url) !== null

  const recordFailure = (
    url: RelayUrl,
    info?: { code?: number | null; reason?: string },
  ): void => {
    const step = stepByUrl.get(url) ?? 0
    const index = Math.min(step, BACKOFF_SCHEDULE_MS.length - 1)
    const duration = BACKOFF_SCHEDULE_MS[index] ?? FALLBACK_BACKOFF_MS
    const at = clock()
    const until = at + duration
    const nextStep = step + 1
    const lastFailure: BackoffFailureInfo = {
      code: info?.code ?? null,
      reason: info?.reason ?? "",
      at,
    }
    disabledUntilByUrl.set(url, until)
    stepByUrl.set(url, nextStep)
    lastFailureByUrl.set(url, lastFailure)
    persistence?.write({ url, disabledUntil: until, step: nextStep, lastFailure })
    onChange()
  }

  const recordSuccess = (url: RelayUrl): void => {
    stepByUrl.delete(url)
    const until = disabledUntilByUrl.get(url)
    if (until === undefined || clock() >= until) {
      disabledUntilByUrl.delete(url)
      lastFailureByUrl.delete(url)
      persistence?.remove(url)
    } else {
      // Still inside the disabled window (e.g. a manual early reconnect): reset the step but keep
      // the last failure so the persisted record stays consistent with the live disabled state.
      persistence?.write({ url, disabledUntil: until, step: 0, lastFailure: lastFailureByUrl.get(url) })
    }
    onChange()
  }

  const clear = (url: RelayUrl): void => {
    disabledUntilByUrl.delete(url)
    stepByUrl.delete(url)
    lastFailureByUrl.delete(url)
    persistence?.remove(url)
    onChange()
  }

  const getDisabledUrls = (): ReadonlyArray<RelayUrl> => {
    const now = clock()
    const urls: Array<RelayUrl> = []
    for (const [url, until] of disabledUntilByUrl) {
      if (until > now) urls.push(url)
    }
    return urls
  }

  return Object.freeze({
    isDisabled,
    disabledUntil,
    getDisabledUrls,
    getLastFailure: (url: RelayUrl): BackoffFailureInfo | null => lastFailureByUrl.get(url) ?? null,
    recordFailure,
    recordSuccess,
    clear,
  })
}
