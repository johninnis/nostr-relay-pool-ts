/**
 * A relay's connection state in {@link RelayPoolStateEntry}.
 *
 * - `connected` — socket open.
 * - `connecting` — socket opening for the first time (no reconnect queued).
 * - `reconnecting` — dropped while it still had subscriptions; a revive is scheduled.
 * - `disconnected` — no socket and nothing scheduled.
 * - `disabled` — in a backoff cooldown window.
 */
export type RelayStatus = "connected" | "connecting" | "reconnecting" | "disconnected" | "disabled"
