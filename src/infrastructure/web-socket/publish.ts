import type { NostrEvent, RelayUrl } from "@innis/nostr-core"
import { serialiseEventMessage } from "@innis/nostr-core"
import type { WallClock } from "../../application/port/clock.ts"
import type { Scheduler, TimerHandle } from "../../application/port/scheduler.ts"
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
  readonly releaseIfIdle: (url: RelayUrl, state: RelayState) => void
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
    releaseIfIdle,
  } = deps
  return (url: RelayUrl, event: NostrEvent): Promise<PublishResponse> =>
    new Promise((resolve) => {
      const state = getOrCreateConnection(url)
      if (!state) {
        resolve({ from: url, ok: false, message: "failed to connect" })
        return
      }

      const sameEventInFlight = state.inFlightPublishes.get(event.id)
      if (sameEventInFlight) {
        sameEventInFlight.resolvers.add(resolve)
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
      onPublishInitiated(url)

      let timeoutId: TimerHandle | null = null
      const suspendTimeout = (): void => {
        if (timeoutId !== null) scheduler.clearTimer(timeoutId)
        timeoutId = null
      }

      const settle = (outcome: PublishOutcome, ack: PublishAck): void => {
        const inFlight = state.inFlightPublishes.get(event.id)
        if (!inFlight) return
        suspendTimeout()
        state.inFlightPublishes.delete(event.id)
        state.pendingAuthPublish.delete(event.id)
        record.result = outcome
        record.message = ack.message
        invalidateCache()
        const response: PublishResponse = { from: url, ...ack }
        for (const resolver of inFlight.resolvers) resolver(response)
        releaseIfIdle(url, state)
      }

      // Deliberate: a publish parked for AUTH is bounded by the auth timeout, not its own — see ADR-0003
      const restartTimeout = (): void => {
        suspendTimeout()
        timeoutId = scheduler.setTimer(() => settle("timeout", { ok: false, message: "timeout" }), publishTimeoutMs)
      }
      restartTimeout()
      state.inFlightPublishes.set(event.id, {
        event,
        resolvers: new Set([resolve]),
        settle: (ack) => settle(ack.ok ? "ok" : "failed", ack),
        suspendTimeout,
        restartTimeout,
      })

      const sendIfStillInFlight = (): void => {
        if (state.inFlightPublishes.has(event.id) && state.ws) {
          sendOnWebSocket(state.ws, serialiseEventMessage(event))
        }
      }

      const ws = state.ws
      if (ws === null) return
      if (isOpen(ws)) sendOnWebSocket(ws, serialiseEventMessage(event))
      else ws.addEventListener("open", sendIfStillInFlight, { once: true })
    })
}
