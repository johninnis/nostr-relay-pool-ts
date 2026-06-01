import type { Scheduler, TimerHandle } from "../../application/port/scheduler.ts"

// Plain predicates over the socket's readyState. They report a *state*, not a *type*: a
// `ws is WebSocket` guard would be a lie (a CONNECTING socket is equally a WebSocket) and would
// wrongly narrow the false branch to `null`, forcing callers that need the still-open-later socket
// to bypass the helper. Callers narrow null-ness explicitly, then ask these about the state.
export const isOpen = (ws: WebSocket | null): boolean => ws !== null && ws.readyState === WebSocket.OPEN

export const isConnecting = (ws: WebSocket | null): boolean => ws !== null && ws.readyState === WebSocket.CONNECTING

export const isOpenOrConnecting = (ws: WebSocket | null): boolean => isOpen(ws) || isConnecting(ws)

export const clearTimerEntry = (scheduler: Scheduler, timers: Map<string, TimerHandle>, key: string): void => {
  const timer = timers.get(key)
  if (timer !== undefined) scheduler.clearTimer(timer)
  timers.delete(key)
}

export const clearAllTimers = (scheduler: Scheduler, timers: Map<string, TimerHandle>): void => {
  for (const timer of timers.values()) scheduler.clearTimer(timer)
  timers.clear()
}

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
