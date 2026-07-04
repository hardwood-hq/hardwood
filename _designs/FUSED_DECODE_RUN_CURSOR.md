# Fused Decode — Run-Cursor Path

## Status

In development (guarded off by default; enable with `-Dhardwood.internal.fusedDecode=true`).

## Problem

The materialising decode path allocates two `int[]` arrays per data page:

- A **def-level array** (`int[numValues]`) decoded from the RLE/bit-packing hybrid stream.
- A **dictionary-index array** (`int[numValues]`) decoded from a second hybrid stream.

For dictionary-encoded columns these two arrays dwarf the actual primitive-value storage.
In practice, both streams consist largely of constant **RLE runs** — a highly-nullable column
emits long runs of `0` / `maxDefLevel`, and a low-cardinality column emits long runs of the same
index. Materialising an array just to iterate over it once is wasteful.

## Design

### `HybridStreamCursor`

A pull-based cursor over a single RLE/bit-packing hybrid stream. Constructed from a private copy
of the encoded bytes (so it remains valid after the pooled decompression buffer is reused on the
next page decode). Exposes:

- `advance()` — load the next run; returns `false` at end-of-stream.
- `isRle()` / `value()` — constant-run branch: the value is known in O(1).
- `remaining()` — values left in the current run.
- `unpack(dst, off, max)` — bit-packed branch: fill up to `max` values into `dst`.
- `skip(count)` — advance past `count` values without reading them (O(1) for RLE,
  byte-advance arithmetic for bit-packed).

The cursor owns its byte array. This is the key design invariant: a cursor is constructed on the
I/O thread during page decode, then consumed later on the drain thread while the decompression
buffer that provided the source bytes is already reused for the next page. Owning the bytes
eliminates a time-of-use race without any locking.

The bit-unpacking kernel in `HybridStreamCursor.decodeBitPacked` intentionally duplicates the
equivalent logic in `RleBitPackingHybridDecoder.decodeBitPacked`. Coupling the two would require
sharing state and would re-introduce the lifetime dependency the copy was designed to break. Any
optimisation to one must be mirrored in the other (a cross-reference comment in each class
documents this).

### Fused path gate

`PageDecoder` enables the fused path when **all** of:

1. The column worker advertises support via `supportsFusedPath()` (only `FlatColumnWorker`).
2. The page encoding is `RLE_DICTIONARY` or `PLAIN_DICTIONARY`.
3. `maxRepetitionLevel == 0` (the column is flat, not nested).
4. `maxDefinitionLevel > 0` (the column is optional; required columns have no def-level stream).

When the gate fires, `PageDecoder` constructs two `HybridStreamCursor` objects instead of two
`int[]` arrays and stores them in the `Page` record alongside the dictionary. The `Page` records
carry `defLevelCursor()` and `indexCursor()` default-null accessors; non-null means the fused
path is active for that page.

### `FlatColumnWorker` fused drain

`assemblePage` checks `page.defLevelCursor()` and, if non-null, stores both cursors as instance
fields. `copyPageDataFused` then interleaves def-level consumption with index scatter:

- **RLE def-level run, value == maxDefLevel** (all present): bulk-scatter index values
  via `copyIndexValues`, set the validity range in O(1).
- **RLE def-level run, value < maxDefLevel** (all absent): zero the value slots via
  `fillNulls`, mark the validity bits in O(1).
- **Bit-packed def-level run**: unpack a chunk into `tempDefs`, then coalesce consecutive
  present/absent positions into sub-runs before dispatching to `copyIndexValues` / `fillNulls`.
  The coalescing avoids per-value index decodes on null-heavy pages.

Within `copyIndexValues`, the same run-dispatch pattern applies to the index cursor:

- **RLE index run**: `Arrays.fill` the typed value array, O(repeat-count).
- **Bit-packed index run**: unpack into `tempIndices`, then scatter into the value array.
- **Bit-width 0** (all values reference entry 0): the cursor stream is empty; `copyIndexValues`
  detects `bitWidth() == 0` and fills with `d.values()[0]` directly.

### Validity bitmap interaction

`FlatColumnWorker` maintains a packed `long[]` validity bitmap where a set bit means present.
The fused path uses `BitmapWords.setRange` to bulk-set ranges instead of the per-value
`markNulls` walk of the materialising path. Absent value slots are zeroed by `fillNulls` to
keep slot contents deterministic across batch recycles.

## Scope

The fused path is dictionary-only. PLAIN-encoded, delta-encoded, and all nested (`maxRepetitionLevel > 0`) columns fall through to the materialising path unchanged. `NestedColumnWorker` always returns `false` from `supportsFusedPath()`.

## Verification

`FlatColumnWorkerParityTest` runs both paths over four test Parquet files and asserts
element-by-element value and validity parity. The `fused_tiny_pages_dict.parquet` fixture
exercises two edge cases that only surface with many small pages:

1. A cursor outliving its source decompression buffer (exercises the byte-copy safety).
2. A page with all values referencing a single dictionary entry (exercises the bit-width-0 path).

## Known limitations / future work

- Plain-encoded dictionary pages (column has a dictionary but the data page is PLAIN) are not
  fused. Support requires extending the gate and adding a plain-index cursor or partial
  materialisation.
- `BooleanPage` is excluded because boolean values use `RLE` (not `RLE_DICTIONARY`).
- No JMH benchmark yet isolating the fused path. End-to-end numbers to follow before the
  feature flag is enabled by default.
