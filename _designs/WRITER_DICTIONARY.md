# Dictionary write support (#9, stage 9)

**Status: Settled; implemented in stage 9.** Tracking issue: #9. Delivery stage 9 (Dimension)
of [WRITER_SUPPORT.md](WRITER_SUPPORT.md). This document is the reference the dictionary
increment implements against.

## Context

Through stage 8 every value section a page carries is `PLAIN`: after the optional
repetition- and definition-level streams, the present values are written as raw
little-endian `INT32`. Dictionary encoding replaces **only that value section** with two
new artefacts:

- a **dictionary page** — one per column chunk, holding the chunk's distinct values once,
  `PLAIN`-encoded;
- **`RLE_DICTIONARY` data pages** — the value section becomes a stream of *indices* into
  the dictionary rather than the values themselves.

The level streams are untouched, so dictionary encoding is orthogonal to nulls and nesting:
a page body is still `[rep levels?][def levels?][value section]`, and dictionary encoding
swaps the value section from `PLAIN INT32` to `[1-byte index bit width][RLE/bit-packed
indices]`. This is exactly the read-side split the decoder already parses — `PageDecoder`
routes `RLE_DICTIONARY`/`PLAIN_DICTIONARY` value sections through a dictionary while the
level parsing is shared — so the write path is its inverse.

Dictionary encoding is a size win precisely for **low-cardinality** columns: an `INT32`
column with `D` distinct values over `N` rows costs `D·32` dictionary bits plus roughly
`N·ceil(log2 D)` index bits, versus `N·32` for `PLAIN`. For small `D` this is several times
smaller and, being low-entropy, compresses far better under stage 10. For high-cardinality
columns (`D → N`) the dictionary approaches the size of the data while the indices stay
wide, so dictionary encoding loses — which is why **plain fallback is a first-class part of
this increment**, not an afterthought: a column chunk that would build an oversized
dictionary abandons it and finishes as `PLAIN`.

PyArrow dictionary-encodes integer columns by default, so files hardwood already reads (the
taxi fixtures' `passenger_count`, `RatecodeID`, `payment_type`, …) are dictionary-encoded
`INT32`. Producing that encoding closes a read/write asymmetry rather than inventing a new
shape.

## Scope

Stage 9 settles the **column-chunk layout** for dictionary encoding on `INT32`:

1. **The dictionary page** — its `DICTIONARY_PAGE` header, `PLAIN` body, and placement ahead
   of the chunk's data pages, addressed by `dictionary_page_offset`.
2. **`RLE_DICTIONARY` data pages** — the `[bit width][indices]` value section sitting behind
   the existing level streams.
3. **Plain fallback** — the rule by which a chunk that overflows the dictionary size limit
   stops dictionary-encoding and finishes as `PLAIN`, and how both encodings are then
   reported.
4. **`ColumnMetaData` wiring** — `dictionary_page_offset` and the `encodings` list.

Dictionary encoding is exercised on `REQUIRED`, `OPTIONAL`, and nested (`struct` / `LIST` /
`MAP`) `INT32` columns, so the level-plus-index page layout is proven together. Leaf physical
type stays `INT32`; type breadth (stage 12) repeats this proven mechanism for the remaining
types — most importantly `BYTE_ARRAY`, where dictionary encoding is the dominant real-world
encoding — inheriting the page and chunk layout unchanged. Compression (stage 10) and
statistics (stage 11) layer on afterwards. `DELTA_*` and `BYTE_STREAM_SPLIT` remain out of
scope (stage 14). Only the modern `RLE_DICTIONARY` form is produced; the deprecated
`PLAIN_DICTIONARY` (which the reader still tolerates) is not written.

## Column-chunk layout

A dictionary-encoded column chunk is laid out as:

```
<dictionary page>            (DICTIONARY_PAGE header, PLAIN body)   ← dictionary_page_offset
<data page 0>                (DATA_PAGE header, RLE_DICTIONARY)     ← data_page_offset
<data page 1>                (DATA_PAGE header, RLE_DICTIONARY)
...
```

The dictionary page comes **first**, so `dictionary_page_offset` is the chunk's start offset
and `data_page_offset` is the offset of the first data page (immediately after the dictionary
page). Both are recorded explicitly; the reader's `RowGroupIterator` handles the explicit
`dictionary_page_offset` directly, and the format also permits omitting it (implicit
dictionary as the first page), but this writer always sets it, the unambiguous form.

### Dictionary page

- **Header** — a `PageHeader` of type `DICTIONARY_PAGE` (thrift page type `2`), carrying a
  `DictionaryPageHeader` (field `7`) with `num_values` = the number of distinct values and
  `encoding` = `PLAIN`. `uncompressed_page_size` / `compressed_page_size` are equal
  (uncompressed here) and a CRC-32 over the body is written, exactly as for data pages.
- **Body** — the distinct values in index order, `PLAIN`-encoded (`PlainEncoder.encodeInts`).
  Index `i` addresses the `i`-th value in this body.

### `RLE_DICTIONARY` data page

Identical framing to a stage 1–8 data page — the same `DATA_PAGE` header, CRC, and level
streams — with two differences:

- the header's values `encoding` is `RLE_DICTIONARY` (not `PLAIN`);
- the value section behind the level streams is `[1-byte bit width][RLE/bit-packed hybrid
  indices]`, **not** length-prefixed. The single leading byte is the index bit width; the
  remaining bytes are the indices for the page's present values, RLE/bit-packed through the
  shared `RleBitPackingHybridEncoder`, running to the end of the page. (This differs from the
  level streams, which *are* 4-byte-length-prefixed — the dictionary index section's length
  is implied by the page's compressed size.)

Only **present** values carry an index, exactly as `PLAIN` only encodes present values; a
null contributes a level entry but no index. `num_values` in the data-page header still counts
all level entries including nulls.

## Dictionary construction and index assignment

The dictionary is **column-chunk scoped** (one per column per row group) and built by
streaming, mirroring the existing page pipeline:

- A value→index map assigns indices in **first-seen order**: the first distinct value seen in
  the chunk is index `0`, the next new value index `1`, and so on. A repeat reuses its
  assigned index.
- Indices stream into the page buffer as values arrive through `accept`, and index pages are
  sealed as they fill — including part-way through a record — so nothing scales with batch
  size and the stage 7 single-large-record streaming property is preserved. The dictionary
  itself (distinct values only, bounded by the fallback limit) is the sole per-chunk
  accumulation.
- Each data page's **index bit width** is `ceil(log2(D_seal))`, where `D_seal` is the number
  of distinct values assigned through that page's seal. Because indices are assigned globally
  in growing order, every index in a page is `< D_seal`, so the per-page width is both correct
  and minimal; a later, wider page does not force earlier pages wider.

At row-group flush the dictionary page is emitted first (its body is the accumulated distinct
values in index order), then the buffered data pages.

## Plain fallback

A column chunk **falls back to `PLAIN`** when its dictionary would exceed the configured
dictionary page-size limit (`WriterConfig.dictionaryPageLimitBytes`, default 1 MiB — for
`INT32`, `limit / 4` distinct values). Fallback is evaluated when a value about to be added is
a **new** distinct value that would push the dictionary past the limit:

- the current pending page is sealed as an `RLE_DICTIONARY` page (its buffered indices are
  valid against the dictionary as it stands);
- the chunk is marked *fallen back*, and from that point every value — including the one that
  triggered fallback and any later repeat of an already-indexed value — is buffered as a raw
  value and its pages sealed as `PLAIN`;
- the dictionary page is still written (it holds the values assigned before fallback), because
  the already-sealed `RLE_DICTIONARY` pages reference it.

Each data page declares its own encoding in its header, so the reader decodes each
independently and any mix of `PLAIN` and `RLE_DICTIONARY` data pages within one chunk is valid
Parquet. In the common cases a chunk is either all `RLE_DICTIONARY` (no overflow) or an
`RLE_DICTIONARY` prefix followed by `PLAIN` pages (overflow). The one exception is a page
sealed while the dictionary is still empty: a leading run of nulls long enough to fill a page
seals as `PLAIN` before any value is interned, which can place a `PLAIN` page *ahead* of the
`RLE_DICTIONARY` pages. Because encoding is per-page, this reads back correctly — but nothing
downstream may assume a set `dictionary_page_offset` implies the first data page is
`RLE_DICTIONARY`. A chunk whose very first value would overflow (limit smaller than one value)
is never possible — one distinct value always fits — so at least the dictionary path is always
attempted; the degenerate tiny-limit case simply falls back after the first page.

Dictionary encoding is **on by default** for every `INT32` column and can be disabled per
writer through `WriterConfig.enableDictionary(false)`, in which case every chunk is `PLAIN`
from the start with no dictionary page — the stage 1–8 behaviour.

## `ColumnMetaData` wiring

`ColumnMetaDataWriter` gains two things:

- **`dictionary_page_offset`** (field `11`) — written when the chunk has a dictionary page,
  omitted otherwise. `data_page_offset` (field `9`) continues to point at the first data page.
- **`encodings`** (field `2`) — the set of encodings actually used in the chunk:
  - dictionary, no fallback: `PLAIN` (dictionary page) + `RLE` (levels, if any) +
    `RLE_DICTIONARY` (data pages);
  - dictionary with fallback: the above **plus** `PLAIN` already covers the fallback data
    pages, so the set is `{PLAIN, RLE, RLE_DICTIONARY}`;
  - dictionary disabled / immediate all-plain: `PLAIN` + `RLE` (levels, if any), the stage 1–8
    set.

The list is the deduplicated set of encodings present, matching how the reader and other
engines interpret field `2`. `RLE` appears only when the column has a level stream (max
definition or repetition level > 0), unchanged from stage 4. The optional `encoding_stats`
(field `13`, per-page-type encoding counts) is not written; it is advisory and no reader in
scope requires it.

## `WriterConfig`

Two knobs are added, both library-level (the CLI surface stays minimal):

- `dictionaryPageLimitBytes` (default `1 << 20`) — the dictionary size ceiling that triggers
  fallback. Must be at least one value wide.
- `enableDictionary` (default `true`) — turns dictionary encoding off for the whole writer.

The page and row-group size targets are unchanged. The existing page/row-group cadence keeps
using its `PLAIN`-based `4 bytes/value` proxy to choose seal points; a dictionary-encoded page
simply comes out smaller than the byte target, which is harmless (a page is cut by level-entry
count, and a smaller page never violates the target). Right-sizing pages to the actual encoded
width is deferred with the variable-width work in stage 12.

## Component architecture

Dictionary support extends the writer components from
[WRITER_SUPPORT.md](WRITER_SUPPORT.md); it adds one encoder and one page-header variant and
reworks the column-chunk buffer's value section.

| Layer | Package | Change |
|---|---|---|
| Public API | `dev.hardwood.writer` | `WriterConfig` gains `dictionaryPageLimitBytes` and `enableDictionary` |
| Value encoding | `dev.hardwood.internal.encoding` | `DictionaryEncoder` — first-seen value→index map, distinct-value list, byte-size tracking for the fallback trigger; reuses `RleBitPackingHybridEncoder` for the index stream and `PlainEncoder` for the dictionary body |
| Orchestration | `dev.hardwood.internal.writer` | `ColumnChunkBuffer` gains a dictionary mode: it routes present values through `DictionaryEncoder`, seals `RLE_DICTIONARY` pages with the `[bit width][indices]` value section, performs the plain fallback, and prepends the dictionary page at flush |
| Metadata serialization | `dev.hardwood.internal.thrift` | `PageHeaderWriter` gains `writeDictionaryPageV1` (type `DICTIONARY_PAGE`, `DictionaryPageHeader` field `7`); `ColumnMetaDataWriter` writes `dictionary_page_offset` (field `11`) and the multi-encoding list |

`RleBitPackingHybridEncoder` already encodes an index stream unchanged — the bit width is
supplied per page — so no new index encoder is needed; the value section prepends the
1-byte width the encoder does not itself emit. `ColumnChunkBuffer.flushTo` changes from
"write the data pages at `dataPageOffset`" to "write the dictionary page at the chunk start
(when present), then the data pages", deriving both offsets from the chunk start position
`RowGroupBuffer` passes in.

## Validation strategy

The two-tier check from [WRITER_SUPPORT.md](WRITER_SUPPORT.md) applies unchanged, with cases
chosen to stress the dictionary layout:

1. **DuckDB differential (primary)** — hardwood writes dictionary-encoded files that DuckDB
   reads through `read_parquet`. Cases: a low-cardinality column (small dictionary, narrow
   indices); a nullable dictionary column (indices only for present rows); a dictionary column
   inside a `LIST`/`MAP` (index section behind rep/def levels); and a high-cardinality column
   that **forces fallback** (tiny `dictionaryPageLimitBytes`), so the mixed
   `RLE_DICTIONARY`-then-`PLAIN` chunk is proven readable by an independent engine.
2. **Round-trip** — write with hardwood, read back with the `ColumnReader`/`ParquetFileReader`
   layer, asserting values and null positions survive. Round-trip pins details DuckDB does not
   surface: that `dictionary_page_offset` and the `encodings` list are correct, that the
   dictionary page decodes, and that a fallback chunk's dictionary and plain pages both read
   back. Boundary cases: a single distinct value (1-value dictionary, bit width 0/1); all-null
   dictionary column (dictionary empty, no indices); dictionary values straddling page and
   row-group boundaries; and `enableDictionary(false)` producing byte-identical output to the
   stage 8 all-`PLAIN` path.

## Delivery

This is stage 9 of [WRITER_SUPPORT.md](WRITER_SUPPORT.md). It lands with the DuckDB
differential and round-trip checks above, so `main` never holds dictionary write code that
cannot produce a readable file. It completes roadmap box 2.2 (dictionary encoding write).
Stage 10 (compression) then compresses these — dictionary and plain — page bodies, and stage
12 repeats the proven dictionary mechanism across the remaining physical types.
