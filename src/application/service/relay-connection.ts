import type { NostrEvent, NostrFilter, RelayUrl } from "@innis/nostr-core"
import type { ConnectionPool } from "../port/connection-pool.ts"
import type { Scheduler, TimerHandle } from "../port/scheduler.ts"

export interface RelayConnectionCallbacks {
  readonly onEvent: (event: NostrEvent, relayUrl: RelayUrl) => void
  readonly onRelayEose: (url: RelayUrl) => void
  readonly onRelayClosed?: (url: RelayUrl, reason: string) => void
}

export interface RelayConnectionHandle {
  readonly unsubscribe: () => void
}

export interface CreateRelayConnectionInput {
  readonly pool: ConnectionPool
  readonly scheduler: Scheduler
  readonly url: RelayUrl
  readonly filters: ReadonlyArray<NostrFilter>
  readonly callbacks: RelayConnectionCallbacks
  readonly hardTimeoutMs: number
  readonly persistent?: boolean
}

interface RequestState {
  closed: boolean
  softTimeoutId?: TimerHandle
  hardTimeoutId?: TimerHandle
}

const NOOP_HANDLE: RelayConnectionHandle = Object.freeze({ unsubscribe: (): void => {} })

export const createRelayConnection = (input: CreateRelayConnectionInput): RelayConnectionHandle => {
  const { pool, scheduler, url, filters, callbacks, hardTimeoutMs, persistent = false } = input
  const requestState: RequestState = { closed: false }
  let completed = false

  const fireComplete = (): void => {
    if (completed) return
    completed = true
    callbacks.onRelayEose(url)
  }

  const clearTimers = (): void => {
    if (requestState.softTimeoutId !== undefined) scheduler.clearTimer(requestState.softTimeoutId)
    if (requestState.hardTimeoutId !== undefined) scheduler.clearTimer(requestState.hardTimeoutId)
    requestState.softTimeoutId = undefined
    requestState.hardTimeoutId = undefined
  }

  const finish = (alsoUnsubscribe: boolean): void => {
    if (requestState.closed) return
    requestState.closed = true
    clearTimers()
    if (alsoUnsubscribe) sub.unsubscribe()
  }

  const onEose = (): void => {
    fireComplete()
    if (!persistent) finish(true)
  }

  const onClosed = (reason: string): void => {
    callbacks.onRelayClosed?.(url, reason)
    // A relay-terminated subscription is dead and will not be revived — settle and tear down even
    // in persistent mode, where an EOSE alone would keep the stream notionally open.
    fireComplete()
    finish(true)
  }

  const sub = pool.subscribe(url, filters, {
    onEvent: (event: NostrEvent, relayUrl: RelayUrl) => {
      if (!requestState.closed) callbacks.onEvent(event, relayUrl)
    },
    onEose,
    onClosed,
  })

  if (!sub.active) {
    requestState.closed = true
    callbacks.onRelayEose(url)
    return NOOP_HANDLE
  }

  if (!persistent) {
    requestState.softTimeoutId = scheduler.setTimer(fireComplete, pool.suggestedTimeout(url))
    requestState.hardTimeoutId = scheduler.setTimer(() => finish(true), hardTimeoutMs)
  }

  return Object.freeze({ unsubscribe: (): void => finish(true) })
}
