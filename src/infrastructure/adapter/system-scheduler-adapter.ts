import type { Scheduler, TimerHandle } from "../../application/port/scheduler.ts"

/** Default {@link Scheduler} backed by the host's `setTimeout`/`clearTimeout`. */
export const systemScheduler: Scheduler = Object.freeze({
  setTimer: (callback: () => void, delayMs: number): TimerHandle => setTimeout(callback, delayMs),
  clearTimer: (handle: TimerHandle): void => clearTimeout(handle),
})
