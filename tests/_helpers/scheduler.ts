import type { Scheduler, TimerHandle, WallClock } from "../../src/application/port/mod.ts"

interface PendingTimer {
  readonly seq: number
  readonly fireAt: number
  readonly callback: () => void
}

/**
 * Deterministic time control for tests: a paired {@link WallClock} and {@link Scheduler} sharing
 * one virtual clock. Nothing fires until `tick(ms)` advances time, so timer-driven behaviour
 * (reconnect, backoff, soft/hard timeouts) runs instantly and without flake.
 */
export interface ManualTime {
  readonly clock: WallClock
  readonly scheduler: Scheduler
  readonly tick: (ms: number) => void
  readonly pendingCount: () => number
}

export const createManualTime = (startMs = 0): ManualTime => {
  let current = startMs
  let nextSeq = 0
  const timers = new Map<TimerHandle, PendingTimer>()

  const scheduler: Scheduler = {
    setTimer: (callback: () => void, delayMs: number): TimerHandle => {
      // Mint a real, immediately-cleared handle so it is the platform's opaque timer type rather
      // than a fabricated number. The virtual clock — not the runtime — decides when it fires.
      const handle = setTimeout(() => {}, 0)
      clearTimeout(handle)
      timers.set(handle, { seq: ++nextSeq, fireAt: current + Math.max(0, delayMs), callback })
      return handle
    },
    clearTimer: (handle: TimerHandle): void => {
      timers.delete(handle)
    },
  }

  const tick = (ms: number): void => {
    const target = current + ms
    // Re-scan after every fire: a callback may schedule a new timer that is itself due before
    // `target`, or clear one that was — both must be honoured mid-drain.
    for (;;) {
      let nextHandle: TimerHandle | undefined
      let next: PendingTimer | undefined
      for (const [handle, timer] of timers) {
        if (timer.fireAt > target) continue
        if (!next || timer.fireAt < next.fireAt || (timer.fireAt === next.fireAt && timer.seq < next.seq)) {
          next = timer
          nextHandle = handle
        }
      }
      if (!next || nextHandle === undefined) break
      timers.delete(nextHandle)
      current = next.fireAt
      next.callback()
    }
    current = target
  }

  return {
    clock: (): number => current,
    scheduler,
    tick,
    pendingCount: (): number => timers.size,
  }
}
