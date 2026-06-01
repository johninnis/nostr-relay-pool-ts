import type { NostrEvent, RelayUrl } from "@innis/nostr-core"
import { serialiseEventMessage } from "@innis/nostr-core"
import type { WallClock } from "../../application/port/clock.ts"
import type { Scheduler } from "../../application/port/scheduler.ts"
import type { PublishOutcome, PublishResponse } from "../../domain/value-object/publish-history.ts"
import { type PublishHistoryRecord, recordPublishEntry } from "../../application/service/relay-history.ts"
import type { PublishAck, RelayState } from "./relay-state.ts"
import { isOpen, sendOnWebSocket } from "./web-socket-helpers.ts"

export interface PublishDeps {
  readonly clock: WallClock
  readonly scheduler: Scheduler
  readonly publishTimeoutMs: number
  readonly publishHistory: Map<RelayUrl, Array<PublishHistoryRecord>>
  readonly invalidateCache: () => void
  readonly onPublishInitiated: (url: RelayUrl) => void
  readonly getOrCreateConnection: (url: RelayUrl) => RelayState | null
}

export const createPublish = (deps: PublishDeps) => {
  const {
    clock,
    scheduler,
    publishTimeoutMs,
    publishHistory,
    invalidateCache,
    onPublishInitiated,
    getOrCreateConnection,
  } = deps
  return (url: RelayUrl, event: NostrEvent): Promise<PublishResponse> =>
    new Promise((resolve) => {
      const state = getOrCreateConnection(url)
      if (!state) {
        resolve({ from: url, ok: false, message: "failed to connect" })
        return
      }

      // Identical event already in flight to this relay: join its single outcome rather than open a
      // second timer/resolver under the same event-id key, which would clobber the first's timer
      // handle and could strand either promise. Same dedup the wire-sub layer applies to filters.
      const existing = state.inFlightPublishes.get(event.id)
      if (existing) {
        existing.resolvers.add(resolve)
        return
      }

      const record: PublishHistoryRecord = {
        eventId: event.id,
        kind: event.kind,
        publishedAt: clock(),
        result: "pending",
        message: "",
      }
      recordPublishEntry(publishHistory, url, record)
      // A new publish (not a dedup join): bump the lifetime tally, which also invalidates the cache.
      onPublishInitiated(url)

      // Single settlement path for every outcome (ack, timeout, disconnect, dispose): clear the timer,
      // drop the in-flight record, leave the AUTH resend queue, stamp the history record, and resolve
      // every awaiting caller exactly once.
      const settle = (outcome: PublishOutcome, ack: PublishAck): void => {
        const inFlight = state.inFlightPublishes.get(event.id)
        if (!inFlight) return
        scheduler.clearTimer(inFlight.timeoutId)
        state.inFlightPublishes.delete(event.id)
        // An event parked for AUTH that settles (e.g. times out before AUTH completes) must leave the
        // resend queue too, or a later successful AUTH re-sends an already-resolved publish.
        const parkedIndex = state.pendingAuthPublish.indexOf(event)
        if (parkedIndex !== -1) state.pendingAuthPublish.splice(parkedIndex, 1)
        record.result = outcome
        record.message = ack.message
        invalidateCache()
        const response: PublishResponse = { from: url, ...ack }
        for (const resolver of inFlight.resolvers) resolver(response)
      }

      const timeoutId = scheduler.setTimer(() => settle("timeout", { ok: false, message: "timeout" }), publishTimeoutMs)
      state.inFlightPublishes.set(event.id, {
        event,
        resolvers: new Set([resolve]),
        timeoutId,
        settle: (ack) => settle(ack.ok ? "ok" : "failed", ack),
      })

      const ws = state.ws
      if (ws === null) return
      if (isOpen(ws)) {
        sendOnWebSocket(ws, serialiseEventMessage(event))
      } else {
        // Socket is still connecting: defer the send until it opens. This is deliberately a private
        // one-shot listener rather than riding the socket-manager's onopen reissue path (which
        // re-sends every pending sub on every reconnect): a publish must fire on *this* open only and
        // is never re-sent on a later reconnect, so it cannot live in the reissue set. Re-check the
        // in-flight record on fire — if the publish already settled (timeout) or the socket dropped
        // (disconnect deletes the record), there is nothing left to send. `{ once: true }` clears it.
        ws.addEventListener("open", () => {
          if (state.inFlightPublishes.has(event.id) && state.ws) {
            sendOnWebSocket(state.ws, serialiseEventMessage(event))
          }
        }, { once: true })
      }
    })
}
