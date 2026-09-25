# Row-group and page sizing (#9, stage 27)

**Status: Complete.** Tracking issue: #981 (pages). Delivery stages 27 and 28 (Layer) of
[WRITER_SUPPORT.md](../_plans/WRITER_SUPPORT.md).

#980 asks for row groups of about N MB *on disk*, which this does not provide and argues against
providing the way it was asked for — see *Why a byte target cannot mean bytes on disk* below, and
*Successor: on-disk targeting* for what taking it would cost. What it takes from #980 is the
complaint underneath: that the one byte knob answered neither question a caller has. It now
answers one of them exactly, and a row target answers the other.

## What a row-group control has to do

A caller setting a row-group option wants one of two things, and usually does not say which:

- **Layout.** How the file is banded. A row group is self-contained, so it is the boundary a
  file partitions on across separate readers, and it is the granularity at which a reader skips
  on column-chunk statistics. It is not what bounds a single reader's parallelism — this one
  decodes pages concurrently within a row group, so pages and columns bound that.
- **Memory.** How much the writer holds. A row group's column chunks must be encoded before any
  of their metadata is known, so the whole group stays resident until it flushes, and that is
  the writer's largest allocation.

One number serves both only where what is held and what is produced are the same size. They are
not, and how far apart they sit is a property of the data rather than of the setting.

## Why a byte target cannot mean bytes on disk

At the moment a row group is cut, nothing has been compressed, and a codec's ratio on data it has
not seen cannot be derived from the data's other properties. Nor has the encoding been chosen: a
chunk's dictionary is weighed against `PLAIN` over the whole chunk, which is not known until the
chunk is complete.

The distance involved is large. On a flat three-column fixture — an all-distinct `INT64`, a
`STRING` of six distinct values, and a `DOUBLE` of 997 distinct values — 8.39 MB of values at
their `PLAIN` width produce 3.21 MB once encodings are applied and 0.41 MB once ZSTD is, a
twentieth of what was held. Both gaps grow with how repetitive the data is, so the ratio between
a memory setting and a file is not a constant a caller could apply themselves.

A produced-size target therefore requires either compressing during accumulation, which settles a
chunk's encoding before the row group has been seen, or predicting a ratio. Of the writers
surveyed, those that target produced size compress as they go — parquet-java's block-size check
counts the compressed bytes of completed pages — and those that buffer first target rows or
in-memory bytes and say so.

## The design

Two controls. A row group is cut when either is reached, and a third bound sits below both:

- **`rowGroupTargetRows`** — the layout control. A row count is exactly what it says: it needs
  no estimate, it does not vary with the data, and it is the number a reader's scheduler cares
  about, since row groups per file follows from it directly.
- **`rowGroupBufferTargetBytes`** — the memory control. The bytes the writer retains, measured
  directly. It keeps a row group of unexpectedly wide records from exhausting a heap that a row
  count alone would not bound.
- **The structural cap** — `RowGroupBuffer.MAX_ROWS`. A chunk accumulates into `int`-indexed
  buffers, so a row group cannot hold more records than those can index whatever the two
  controls say. Not configurable, and not reachable except by a target set above it.

Neither control is the size of what reaches the file, and the documentation says so where each
is set. A caller who needs a particular on-disk size measures one file and scales the setting;
a caller who needs a particular banding sets the row count.

## The memory measure: retained bytes

Every buffer knows what it holds. A chunk's retained bytes are a sum of field reads:

| term | value |
| --- | --- |
| repetition levels | `repLevels.length()` — one byte per entry |
| definition levels | `defLevels.length()` — one byte per entry |
| dictionary indices | `indexCount × 4` |
| value store and dictionary | `values.retainedBytes()` |

`ValueEncoder.retainedBytes()` is the per-type term:

| encoder | retained |
| --- | --- |
| `IntValueEncoder` / `FloatValueEncoder` | `plainCount × 4` + dictionary entries and table |
| `LongValueEncoder` / `DoubleValueEncoder` | `plainCount × 8` + dictionary entries and table |
| `BooleanValueEncoder` | `plainCount / 8` |
| `BinaryValueEncoder` | `plainData.length() + plainCount × 4` + dictionary content and table |

A row group's retained bytes are the sum across its chunks — O(columns), evaluated once per
appended slice rather than accumulated per value.

**Content, not capacity.** The stores grow geometrically and keep the capacity they reach across
row groups, so charging capacity would report a freshly reset row group as already full. Charging
content means the resident heap is bounded by the growth factor above the target: at most 1.5×
the target in store capacity, plus the high-water capacity the file's widest row group reached.

## The cut: size a slice by what it could cost, cut on what it did

A dictionary-alive column retains 4 bytes for a value that repeats and 4 bytes plus a whole
dictionary entry for one that is new, and which of the two it is cannot be known without hashing
it. Retained bytes are therefore knowable only after appending a range.

Appending a fixed number of records and then checking does not hold the target: nothing about the
records already appended anticipates a batch whose records widen part way through, and one slice
of newly wide records carries a row group far past its target. So a slice is sized by what it
*could* cost, and the row group is cut on what it turned out to hold:

```java
int slice = Math.min(Math.min(rows - pos, SLICE_RECORDS),
                     rowGroupTargetRows - current.rowCount());
slice = current.sliceThatFits(shredder, sources, pos, slice,
                              config.rowGroupBufferTargetBytes() - current.retainedBytes());
current.appendRecords(shredder, sources, pos, slice);
if (current.retainedBytes() >= config.rowGroupBufferTargetBytes()
        || current.rowCount() >= rowGroupTargetRows) {
    flushRowGroup();
}
```

`SLICE_RECORDS` is 4096: the ceiling on how much goes in between two readings of what the row
group holds, so that the reading is amortized over a bulk append rather than taken per record.

**The bound.** `sliceThatFits` halves down from a whole slice until what the range can cost fits
the room left. Per column, every leaf slot is charged a present value *and* a new dictionary
entry, and every layer that can stand in for absent content is charged an entry it may not emit:

| column | most one value can retain |
| --- | --- |
| `INT32` / `FLOAT` | `max(4, 4 + 20)` — its store slot, or an index and a new table entry |
| `INT64` / `DOUBLE` | `max(8, 4 + 32)` |
| `BOOLEAN` | 1, and never a dictionary |
| `FIXED_LEN_BYTE_ARRAY` | `len + 32` |
| `BYTE_ARRAY` | read from the batch, plus 36 |

Leaf counts come from `RecordShredder.leafRange`, which composes the cumulative offsets in one
array lookup per repeated layer rather than walking the records, so the bound costs O(columns ×
layers) for every column whose width the schema fixes, and a read of the incoming lengths only for
`BYTE_ARRAY`.

Halving rather than searching for the largest fit keeps this to a dozen evaluations at worst and
one wherever a whole slice fits — which is every slice of a row group but its last few. The bound
being conservative shortens those last slices; it never shortens the row group, because the cut is
made on the measurement.

**The guarantee.** A row group passes its byte target by at most what one record retains: the
slice halves to a single record as the room runs out, and a record goes in whole because a record
cannot be split across row groups. The row target is exact — the slice is clamped to what remains
of it — and the structural cap sits beneath both.

## Appending a slice

Every column takes the same path. The column's encoder is bound to the batch's source, and
`RecordShredder` walks the record range, emitting one `(repetition, definition, valueIndex)`
entry per leaf slot into the chunk buffer. Per entry the buffer retains a byte for each level
stream the column has, and a present value is either interned against the chunk's dictionary or
copied into the value store, with the statistics extended either way.

The shredder resolves entry structure, which is what a repeated or struct-nested column needs and
what a flat leaf does not — for a flat leaf the walk reduces to one entry per record. It is used
for both, so there is one append path rather than one per column shape.

Each encoder reads its source through a fixed typed window, filled in bulk. That is what keeps
reading a value from being a virtual call into `ColumnSource` per value, and it is sized to
`SLICE_RECORDS`, so a column fills it exactly once per slice.

A bulk path for flat leaves — one `ColumnSource.copyInto` straight into the value store where the
column is required and holds no dictionary, definition levels filled from the validity bitmap
where it is optional — is available and not taken. It would remove the walk and the per-value
window read, but not the interning or the statistics, which are per value by nature and are what
a dictionary-encoded column spends its time on.

## What decides the encoding

A chunk is interned in first-seen order, weighed against `PLAIN` at flush over the whole chunk,
and written one way throughout. That comparison needs what the values would occupy `PLAIN`, which
is `presentCount × width` for every fixed-width column and a running total in `BinaryValueEncoder`
for the rest — neither of which needs a call per value from `ColumnChunkBuffer`.

The probe schedule stands: it stops a high-cardinality column hashing a row group's worth of
values into a table the flush comparison would reject, which is a CPU bound that byte accounting
does not address. A dictionary counts against `rowGroupBufferTargetBytes` like everything else the
writer holds, so it needs no size cap of its own.

## Defaults

`rowGroupBufferTargetBytes` keeps 128 MiB and `rowGroupTargetRows` defaults to 1,048,576, which
is where both Arrow implementations cap a row group's records; DuckDB caps lower, at 122,880.
The two divide the space between
them: the row target binds for narrow records, the byte target for wide ones. A narrow schema is
therefore banded by its record count and a wide one by what it holds, and neither can run away.

Measured on a flat three-column fixture of four million records:

| Row target | Row groups | File |
| --- | --- | --- |
| none | 1 | 4,998,402 B |
| 1,048,576 | 4 | 5,012,122 B (+0.27%) |
| 122,880 (DuckDB's) | 33 | 5,207,613 B (+4.2%) |

The cost of banding is the per-row-group dictionary and metadata that each new group repeats.
At 1Mi records it is a quarter of a percent for four times the banding; DuckDB's number costs
fifteen times as much for eight times finer.

## How other writers control this

| Writer | Layout control | Memory control |
| --- | --- | --- |
| parquet-java | block size, 128 MB, counted as **compressed** bytes of completed pages | the
same number, plus a `MemoryManager` that scales it down across concurrent writers |
| Arrow C++ / PyArrow | `max_row_group_length`, 1Mi rows | — |
| arrow-rs | `max_row_group_size`, 1Mi rows | — |
| DuckDB | `ROW_GROUP_SIZE`, 122,880 rows | `ROW_GROUP_SIZE_BYTES`, `row_group_size × 1024`,
over its own in-memory format |
| Hardwood | `rowGroupTargetRows` | `rowGroupBufferTargetBytes` |

Three of the four target rows for layout. Only parquet-java's number is close to on-disk size,
and it is close because of when that writer compresses rather than because of what it counts.

## Successor: on-disk targeting

A control that means bytes on disk requires the encode to stream: each page compressed as it is
cut, so a row group's produced size accumulates as it fills and the group can be closed on it.
That also collapses the two controls into one honest number, because what is held is then the
compressed bytes rather than the values behind them.

What it costs is the flush-time encoding decision of stage 18: a chunk's encoding would have to
be settled before its first page is produced, which gives up the uniform per-chunk encoding, the
choice made from true cardinality, and the exact `distinct_count`. parquet-java recovers most of
the size benefit with a first-page probe — a size comparison over the first page's values rather
than a byte limit — which is the mechanism to adopt if this stage is taken.

It is a separate stage, and one worth taking only on evidence that on-disk row-group size
matters to callers more than the encoding quality it costs.

## Page limits

The same two questions apply one level down, and the answers differ because a page is produced
at flush rather than decided during accumulation.

**A page is cut on the bytes it encodes to, at flush.** A page's cut point is decided while the
chunk is being written out, not while records arrive. By then the chunk's encoding is settled and
every width the type or the dictionary fixes is known: a dictionary-encoded value costs the
index that will
represent it, a `PLAIN` fixed-width value its width, a `PLAIN` `BYTE_ARRAY` value its length from
the packed store's own offsets. The page is cut before the entry that would cross the target, so
the target is a ceiling; only a single value larger than the whole target can breach it, a value
not being divisible across pages. Levels are charged the width their stream encodes at, which the
RLE beats on any column whose levels run, so a levelled page comes out at or under the target.

The three delta encodings are the exception, and deliberately. Their width is a property of the
values, not of the type, so a page's cost under one can only be had by encoding it — and doing so
was measured at **+55% encode time** on the named-delta path to recover **0.02% of the file**
(1,035 bytes in 5.16 MB, all of it the page headers that the resulting 26 fewer pages did not
write). The cut charges the width the type would have taken `PLAIN`, which no delta encoding
exceeds, so those pages land under the target. `AUTO` never chooses a delta encoding, so this is
reached only by a caller who named one.

Deciding while records arrive cannot reach that. It has no encoding to measure against — the
choice between a dictionary and `PLAIN` is made from the whole row group — so it can only count
what the values would occupy `PLAIN`, which is the wrong number for every chunk that wins a
dictionary. And a per-file entry count derived from the widest column's *estimated* `PLAIN` width
is wrong in the other direction for `BYTE_ARRAY`, whose values have no nominal width: measured at
the shipped defaults with one `BYTE_ARRAY` column written `PLAIN`,

| Value width | From an entry count | Cut at flush |
| --- | --- | --- |
| 16 B | 1,048,560 B (1×) | 1,048,560 B (1×) |
| 1 KiB | 53,895,984 B (51×) | 1,048,560 B (1×) |
| 8 KiB | 134,225,892 B (128×) | 1,040,892 B (1×) |
| 64 KiB | 134,225,920 B (128×) | 983,100 B (1×) |

At 8 KiB and above the row group ended before the entry count was reached, so the whole chunk was
one page — 134 MB that a reader has to read and decompress to reach any value in it.

Measuring the encoding rather than the values also makes the pages of a dictionary column the
size they were asked for instead of a fraction of it, which is worth more than it sounds: on the
three-column fixture it takes the file from 5,054,010 to 4,332,957 bytes, a seventh smaller, in
page headers and in what a codec can do with a full block.

**A page value limit is worth 20,000, but only once pages can be pruned.** Both writers that
emit a page index cap a page's rows there — parquet-java's `DEFAULT_PAGE_ROW_COUNT_LIMIT` and
arrow-rs's `DEFAULT_DATA_PAGE_ROW_COUNT_LIMIT` are both exactly 20,000 — and parquet-java keeps
a separate safety ceiling of 2^30-1 values beside it. The number buys pruning granularity: a
`ColumnIndex` carries one entry per page, so a page holding a million values is a page a reader
cannot prune within.

Measured, cutting pages at 20,000 entries instead of the current 52,102 costs **8.6%** in file
size on the three-column fixture (5,054,010 B to 5,487,919 B), the codec getting 80 KiB blocks
instead of 1 MiB ones. That cost buys nothing until the writer emits something to prune on, and
it emits neither a page index nor inline per-page statistics today. The limit therefore belongs
with page index writing (stage 24), not before it.

Two details for whoever takes it: parquet-java and arrow-rs both count **rows**, where this
writer's page cut counts **entries** — the same for a flat column, and different for a repeated
one — and the page index records row ranges, so rows is the unit that matches what the limit is
for.

## Neighbouring stages

- **Page index writing (#984, stage 24).** Where the 20,000-value limit belongs.
- **Caller-controlled boundaries (#985, stage 30).** `endRowGroup()` is a third trigger
  alongside these two, for a boundary neither expresses.
