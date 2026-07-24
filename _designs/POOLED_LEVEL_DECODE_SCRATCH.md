# Design: pooled repetition/definition level decode scratch

**Status: Implemented.** Tracking issue: #814.

## Goal

Stop allocating a fresh repetition/definition level array on every page
decode. Level decoding runs on the hot read path for every optional or
repeated column, and each page previously allocated `new int[numValues]`
per level stream. For a full nested column scan this is the single
largest `int[]` allocation site, and the per-page churn is a meaningful
source of GC pressure — which surfaces as run-to-run throughput variance
on the general reconstruction path (the fixed-size-list fast path, which
skips level materialization, does not exhibit it).

The batch holders and the value/level *accumulators* are already reused
across batches; the per-page *decode scratch* is a separate array
population that was not pooled. This design pools it.

## Behaviour

Level scratch is reused per in-flight reorder-buffer slot.

`ColumnWorker` submits page decodes to a pooled executor, keeping up to
`MAX_INFLIGHT_PAGES` decoded pages parked in an `AtomicReferenceArray`
reorder buffer indexed by `slot = seq % MAX_INFLIGHT_PAGES`. The drain
thread assembles each page synchronously and advances `consumePosition`;
the retriever throttles submission while `nextSeq - consumePosition >=
MAX_INFLIGHT_PAGES`, so a slot is never reused for a new decode until its
previous occupant has been fully drained.

That throttle is the exclusion guarantee the pooling relies on. Each slot
owns one `PageDecoder.LevelScratch` holder (`{ int[] rep; int[] def; }`),
allocated once and passed into `PageDecoder.decodePage`. The level
decoders write into the holder's buffers, growing and rebinding them when
a page needs more than the current capacity, and the decoded `Page`
references whichever buffer was used. Because a slot's previous page is
drained before the slot is reused, no live page's level arrays are
overwritten. Single-page callers (tooling, tests) pass a null holder and
fall back to fresh allocation.

Over a scan the buffers grow to the largest page's value count once and
then stop allocating. The fix lives in the shared `PageDecoder` and the
shared `ColumnWorker`, so both the nested (rep + def) and flat
(optional-column def) paths benefit.

### Buffers may be longer than the page

A reused buffer is sized to the largest page seen, so it can be longer
than the current page's `numValues`. Level arrays are therefore no longer
guaranteed to have length equal to the value count, and consumers must
scan `page.size()` / an explicit length rather than `array.length`.

All consumers already used an explicit count except `countNonNulls`
(`SimdOperations`), which derived the count from `defLevels.length` when
sizing the dictionary-index read. It now takes an explicit `len`
parameter — the caller already had the correct value count — and scans
only the leading `len` entries. This also removes a latent fragility: a
decode primitive should count over the logical value count, not the
physical buffer size.

## Safety argument

1. **No concurrent access to a slot's holder.** A slot holds one page at a
   time; the throttle keeps a slot's previous page drained before reuse,
   so at most one decode task touches a given holder at any moment. No
   synchronization is required, and `PageDecoder` remains stateless
   (the holder is passed per call, not held as a field).
2. **Page level arrays are not retained past assembly.** `assembleRegularPage`
   consumes the page's repetition/definition levels element-by-element
   into the reused accumulators during the synchronous drain.
   `REAL_VIEW_KEEP_LEVELS` keeps the *accumulator* levels, not the page
   arrays. Nothing references a page's level arrays after its assemble
   call returns, so overwriting the slot buffer on the next decode is
   safe.
3. **No length dependence.** Consumers scan the logical value count, not
   the buffer length (see above).
