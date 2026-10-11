import type { Scheduler, TimerHandle, WallClock } from "../../src/application/port/mod.ts"

interface PendingTimer {
  readonly seq: number
  readonly fireAt: number
  readonly callback: () => void
}

export interface ManualTime {
  readonly clock: WallClock
  readonly scheduler: Scheduler
  readonly tick: (ms: number) => void
  readonly pendingCount: () => number
}

const opaqueTimerHandle = (): TimerHandle => {
  const handle = setTimeout(() => {}, 0)
  clearTimeout(handle)
  return handle
}

const firesBefore = (timer: PendingTimer, other: PendingTimer): boolean =>
  timer.fireAt < other.fireAt || (timer.fireAt === other.fireAt && timer.seq < other.seq)

export const createManualTime = (startMs = 0): ManualTime => {
  let current = startMs
  let nextSeq = 0
  const timers = new Map<TimerHandle, PendingTimer>()

  const scheduler: Scheduler = {
    setTimer: (callback: () => void, delayMs: number): TimerHandle => {
      const handle = opaqueTimerHandle()
      timers.set(handle, { seq: ++nextSeq, fireAt: current + Math.max(0, delayMs), callback })
      return handle
    },
    clearTimer: (handle: TimerHandle): void => {
      timers.delete(handle)
    },
  }

  const nextTimerDueBy = (target: number): [TimerHandle, PendingTimer] | undefined => {
    let next: [TimerHandle, PendingTimer] | undefined
    for (const [handle, timer] of timers) {
      if (timer.fireAt > target) continue
      if (next === undefined || firesBefore(timer, next[1])) next = [handle, timer]
    }
    return next
  }

  const tick = (ms: number): void => {
    const target = current + ms
    for (let due = nextTimerDueBy(target); due !== undefined; due = nextTimerDueBy(target)) {
      const [handle, timer] = due
      timers.delete(handle)
      current = timer.fireAt
      timer.callback()
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
