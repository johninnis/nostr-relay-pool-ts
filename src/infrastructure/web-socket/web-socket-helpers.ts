import type { SubscriptionId } from "@innis/nostr-core"
import { parseSubscriptionId, serialiseCloseMessage } from "@innis/nostr-core"

export const isOpen = (ws: WebSocket | null): boolean => ws !== null && ws.readyState === WebSocket.OPEN

export const isConnecting = (ws: WebSocket | null): boolean => ws !== null && ws.readyState === WebSocket.CONNECTING

export const isOpenOrConnecting = (ws: WebSocket | null): boolean => isOpen(ws) || isConnecting(ws)

export const sendOnWebSocket = (ws: WebSocket, data: string): void => {
  if (ws.readyState === WebSocket.OPEN) ws.send(data)
}

export const subscriptionIdOf = (raw: string): SubscriptionId => {
  const subId = parseSubscriptionId(raw)
  if (subId === null) throw new Error(`The pool's own subscription id ${raw} is not 1 to 64 characters`)
  return subId
}

const KEEPALIVE_SUBSCRIPTION_ID = subscriptionIdOf("keepalive")

// Deliberate: a CLOSE for a subscription that was never opened is the keep-alive frame — see ADR-0004 and ADR-0005
export const sendKeepalive = (ws: WebSocket): void =>
  sendOnWebSocket(ws, serialiseCloseMessage(KEEPALIVE_SUBSCRIPTION_ID))
