import type { RelayUrl } from "@innis/nostr-core"
import { relayUrlFixture } from "@innis/nostr-core/testing"

export interface RecordingRelay {
  readonly url: RelayUrl
  readonly received: ReadonlyArray<string>
  readonly sendToAll: (frame: unknown) => void
  readonly stop: () => Promise<void>
}

export const startRecordingRelay = async (): Promise<RecordingRelay> => {
  const received: string[] = []
  const sockets = new Set<WebSocket>()
  let resolvePort: (port: number) => void = () => {}
  const bound = new Promise<number>((resolve) => {
    resolvePort = resolve
  })
  const server = Deno.serve({ port: 0, onListen: ({ port }) => resolvePort(port) }, (req) => {
    const { socket, response } = Deno.upgradeWebSocket(req)
    sockets.add(socket)
    socket.onmessage = (msg: MessageEvent): void => {
      if (typeof msg.data === "string") received.push(msg.data)
    }
    socket.onclose = (): void => {
      sockets.delete(socket)
    }
    return response
  })
  const port = await bound
  return {
    url: relayUrlFixture(`ws://127.0.0.1:${port}`),
    received,
    sendToAll: (frame: unknown): void => {
      for (const socket of sockets) {
        if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(frame))
      }
    },
    stop: async (): Promise<void> => {
      for (const socket of sockets) socket.close()
      await server.shutdown()
    },
  }
}
