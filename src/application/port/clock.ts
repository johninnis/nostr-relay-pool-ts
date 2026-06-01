/**
 * A function returning milliseconds since the Unix epoch — the unit `setTimeout`, `Date.now()`,
 * and every timer in this package speaks. Deliberately distinct from `@innis/nostr-core`'s
 * `Clock` (which returns **seconds** for protocol timestamps) so the two cannot be silently
 * interchanged.
 */
export type WallClock = () => number
