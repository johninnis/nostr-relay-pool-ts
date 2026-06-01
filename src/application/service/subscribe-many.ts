import type { NostrFilter, RelayUrl } from "@innis/nostr-core"
import { toRelayUrls } from "@innis/nostr-core"
import type { ConnectionPool } from "../port/connection-pool.ts"
import type { Scheduler } from "../port/scheduler.ts"
import type {
  PoolSubscription,
  SubscribeCallbacks,
  SubscribeManyOptions,
} from "../../domain/value-object/subscription.ts"
import { createRelayConnection, type RelayConnectionHandle } from "./relay-connection.ts"

export interface SubscribeManyDeps {
  readonly connectionPool: ConnectionPool
  readonly scheduler: Scheduler
  readonly hardTimeoutMs: number
}

export const createSubscribeMany = (deps: SubscribeManyDeps) =>
// deno-lint-ignore innis/max-params -- closure mirrors RelayPool.subscribeMany's public signature.
(
  rawUrls: ReadonlyArray<string>,
  filters: ReadonlyArray<NostrFilter>,
  callbacks: SubscribeCallbacks,
  options: SubscribeManyOptions = {},
): PoolSubscription => {
  const { connectionPool, scheduler, hardTimeoutMs } = deps
  const { persistent = false } = options
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
        onEvent: callbacks.onEvent,
        onRelayEose: (relay) => {
          // In non-persistent mode the leg self-tears-down at EOSE; drop its handle so a later
          // syncUrls re-listing this relay reopens it rather than skipping it as still-live.
          // A persistent leg stays open past EOSE, so its handle must remain.
          if (!persistent) openConnections.delete(relay)
          callbacks.onRelayEose?.(relay)
        },
        onRelayClosed: (relay, reason) => {
          // A relay-terminated subscription self-tears-down; drop its handle so a later syncUrls
          // listing this relay reopens it rather than treating the dead connection as still live.
          openConnections.delete(relay)
          callbacks.onRelayClosed?.(relay, reason)
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
