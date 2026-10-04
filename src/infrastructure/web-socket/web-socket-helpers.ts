import type { SubscriptionId } from "@innis/nostr-core"
import { parseSubscriptionId, serialiseCloseMessage } from "@innis/nostr-core"

// Plain predicates over the socket's readyState. They report a *state*, not a *type*: a
// `ws is WebSocket` guard would be a lie (a CONNECTING socket is equally a WebSocket) and would
// wrongly narrow the false branch to `null`, forcing callers that need the still-open-later socket
// to bypass the helper. Callers narrow null-ness explicitly, then ask these about the state.
export const isOpen = (ws: WebSocket | null): boolean => ws !== null && ws.readyState === WebSocket.OPEN

export const isConnecting = (ws: WebSocket | null): boolean => ws !== null && ws.readyState === WebSocket.CONNECTING

export const isOpenOrConnecting = (ws: WebSocket | null): boolean => isOpen(ws) || isConnecting(ws)

export const closeWebSocket = (ws: WebSocket): void => {
  if (ws.readyState === WebSocket.CLOSED) return
  try {
    ws.close()
  } catch {
    // WebSocket.close throws on some platforms when the underlying socket is
    // already torn down — there is no recoverable action and onclose has either
    // already fired or will not fire, so we have nothing to clean up.
  }
}

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
