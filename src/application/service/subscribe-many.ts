import type { NostrFilter, RelayUrl } from "@innis/nostr-core"
import { toRelayUrls } from "@innis/nostr-core"
import type { ConnectionPool } from "../port/connection-pool.ts"
import type { Scheduler } from "../port/scheduler.ts"
import type { PoolSubscription, SubscribeManyCallbacks } from "../../domain/value-object/subscription.ts"
import { createRelayConnection, type RelayConnectionHandle } from "./relay-connection.ts"

export interface SubscribeManyDeps {
  readonly connectionPool: ConnectionPool
  readonly scheduler: Scheduler
  readonly hardTimeoutMs: number
}

type FanOut = (
  rawUrls: ReadonlyArray<string>,
  filters: ReadonlyArray<NostrFilter>,
  callbacks: SubscribeManyCallbacks,
) => PoolSubscription

const createFanOut = (deps: SubscribeManyDeps, persistent: boolean): FanOut =>
(
  rawUrls,
  filters,
  callbacks,
) => {
  const { connectionPool, scheduler, hardTimeoutMs } = deps
  const { onEvent, onRelayEose, onRelayClosed } = callbacks
  const openConnections = new Map<RelayUrl, RelayConnectionHandle>()
  let closed = false
  let currentUrls: ReadonlyArray<RelayUrl> = toRelayUrls(rawUrls)

  const openFor = (url: RelayUrl): void => {
    if (openConnections.has(url)) return
    const handle = createRelayConnection({
      pool: connectionPool,
      scheduler,
      url,
      filters,
      hardTimeoutMs,
      persistent,
      callbacks: {
        onEvent,
        onRelayEose: (relay) => {
          // In non-persistent mode the leg self-tears-down at EOSE; drop its handle so a later
          // syncUrls re-listing this relay reopens it rather than skipping it as still-live.
          // A persistent leg stays open past EOSE, so its handle must remain.
          if (!persistent) openConnections.delete(relay)
          onRelayEose?.(relay)
        },
        onRelayClosed: (relay, reason) => {
          // A relay-terminated subscription self-tears-down; drop its handle so a later syncUrls
          // listing this relay reopens it rather than treating the dead connection as still live.
          openConnections.delete(relay)
          onRelayClosed?.(relay, reason)
        },
      },
    })
    openConnections.set(url, handle)
  }

  const openAll = (): void => {
    for (const url of currentUrls) openFor(url)
  }

  const closeAll = (): void => {
    for (const [, conn] of openConnections) conn.unsubscribe()
    openConnections.clear()
  }

  openAll()

  const syncUrls = (newRawUrls: ReadonlyArray<string>): void => {
    if (closed) return
    currentUrls = toRelayUrls(newRawUrls)
    const desired = new Set(currentUrls)
    for (const [url, conn] of [...openConnections]) {
      if (!desired.has(url)) {
        conn.unsubscribe()
        openConnections.delete(url)
      }
    }
    openAll()
  }

  const unsubscribe = (): void => {
    if (closed) return
    closed = true
    closeAll()
  }

  return Object.freeze({ unsubscribe, syncUrls })
}

/** The one-shot fan-out: each relay's leg closes at its EOSE. */
export const createSubscribeMany = (deps: SubscribeManyDeps): FanOut => createFanOut(deps, false)

/** The live fan-out: each relay's leg stays open past its EOSE. */
export const createSubscribeManyLive = (deps: SubscribeManyDeps): FanOut => createFanOut(deps, true)
