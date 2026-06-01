/** Opaque token identifying a scheduled timer, as returned by {@link Scheduler.setTimer}. */
export type TimerHandle = ReturnType<typeof setTimeout>

/**
 * Schedules one-shot timers in wall-clock milliseconds — the side of time the pool *acts on*,
 * as opposed to {@link WallClock}, which is the time it *reads*. Injecting both lets a host drive
 * the pool's reconnect, backoff, stability, and timeout behaviour deterministically in tests.
 */
export interface Scheduler {
  /** Run `callback` once after `delayMs` milliseconds; returns a handle for cancellation. */
  readonly setTimer: (callback: () => void, delayMs: number) => TimerHandle
  /** Cancel a timer scheduled by {@link Scheduler.setTimer}. */
  readonly clearTimer: (handle: TimerHandle) => void
}
