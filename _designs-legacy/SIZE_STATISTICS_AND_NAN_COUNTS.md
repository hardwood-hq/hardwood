# Plan: Parse `SizeStatistics` and NaN counts (#607)

**Status: Implemented.** Tracking issue: #607.

## Context

Parquet 2.10 added a second family of per-chunk and per-page summaries alongside the
existing `Statistics`:

- **`SizeStatistics`** — the unencoded size of `BYTE_ARRAY` data, plus repetition- and
  definition-level histograms, which give null and empty-list counts without decoding a
  level stream.
- **NaN counts** — NaN sits outside the min/max total order, so a count of NaN values is
  the only way to reason about a floating-point unit from its statistics alone.

arrow-cpp writes both by default, so they are present in a large share of files produced
today. Hardwood read neither: all six Thrift fields fell through to `skipField`.

| Struct | Field | Thrift type | Surfaced as |
|---|---|---|---|
| `ColumnMetaData` | 16 `size_statistics` | `SizeStatistics` | `ColumnMetaData.sizeStatistics` |
| `Statistics` | 9 `nan_count` | `i64` | `Statistics.nanCount` |
| `ColumnIndex` | 6 `repetition_level_histograms` | `list<i64>` | `ColumnIndex.repetitionLevelHistograms` |
| `ColumnIndex` | 7 `definition_level_histograms` | `list<i64>` | `ColumnIndex.definitionLevelHistograms` |
| `ColumnIndex` | 8 `nan_counts` | `list<i64>` | `ColumnIndex.nanCounts` |
| `OffsetIndex` | 2 `unencoded_byte_array_data_bytes` | `list<i64>` | `OffsetIndex.unencodedByteArrayDataBytes` |

`ColumnIndex` field 7 belongs here because #613 removed its mis-numbered geospatial branch
and left the parse to this plan.

## Out of scope

Every consumer of the new data:

- Promoting floating-point `StatsDecision` results using `nanCount`. `ALWAYS_MATCHES` is
  hardcoded `false` for `FLOAT` / `FLOAT16` / `DOUBLE` in `StatisticsFilterSupport`;
  changing that alters which rows the filter evaluates and belongs to a follow-up on #795.
- Deriving null or empty-list counts from the histograms for read planning.
- Surfacing any of it in `hardwood dive`.
- Writing these structures.

No read path consults the new fields, so no file reads differently than it did before.

## Design

### The `SizeStatistics` record

A public record in `dev.hardwood.metadata` mirroring the Thrift struct: a nullable
`Long unencodedByteArrayDataBytes` and two nullable `long[]` histograms.

A histogram is a small dense run of counts, so it is carried as a primitive array rather
than a `List<Long>`: boxing a per-page histogram costs one object per level per page, and
nothing in the record's contract needs `List`. `ColumnIndex.nullCounts` moves to `long[]`
with them, so a `ColumnIndex` presents its four per-page count arrays the same way. The
arrays are not copied on the way in or out.

Every field is optional in the format and surfaces as `null` when absent. Absent must stay
distinct from present-but-empty, and not only in principle: PyArrow records the
repetition-level histogram of a non-repeated column as **present but empty** in
`SizeStatistics` and as **absent** in the `ColumnIndex` for that same column. The same
rule governs `nanCount`, where the distinction carries a conclusion — only a recorded
zero proves a chunk holds no NaN.

### Histogram layout

A chunk-level histogram in `SizeStatistics` has `maxLevel + 1` entries; entry *i* is the
number of values at level *i*. The `ColumnIndex` histograms hold the same per page,
**flattened** into one list of `pageCount × (maxLevel + 1)` entries, page-major: page *p*
occupies `[p * (maxLevel + 1), (p + 1) * (maxLevel + 1))`.

The flattened layout is carried through unchanged, and `ColumnIndex` slices a page out of it
through `repetitionLevelHistogram(int)` / `definitionLevelHistogram(int)`. No schema
reference is needed for that: the record knows its own page count from `nullPages`, and the
concatenation holds `maxLevel + 1` entries per page, so the stride is
`histograms.length / getPageCount()` and the column's maximum level follows from the two
lengths. A length that is not a whole number of pages has no stride describing it and
raises `IllegalStateException`; an absent histogram stays `null`, as it is on the
whole-chunk accessor.

The slice is a copy. The whole-chunk accessors hand out the array the file was read into,
so a caller that wants to avoid the copy indexes the flat array directly.

### Additions to existing records

`ColumnMetaData` gains `sizeStatistics`; `Statistics` gains `nanCount`; `ColumnIndex`
gains the two histograms and `nanCounts`; `OffsetIndex` gains
`unencodedByteArrayDataBytes`. Each is appended, and each changes its record's canonical
constructor — binary-incompatible additions to `dev.hardwood.metadata`, reported by
japicmp. `ColumnIndex.nullCounts` changes type in the same pass, which japicmp reports
alongside them.

`ColumnMetaData` is reachable from public API through `FileMetaData` → `RowGroup` →
`ColumnChunk`. `ColumnIndex` and `OffsetIndex` are public records with no public reader
entry point — they are reached through `internal.thrift`, as `hardwood-cli` does. This
plan does not change that asymmetry, so only the chunk-level surface is documented under
`docs/content/`.

## Validation strategy

Reading stays tolerant, consistent with every other optional metadata field. A field whose
Thrift type tag does not match the spec is skipped rather than rejected, and a histogram
whose length does not match `pageCount × (maxLevel + 1)` is surfaced as read — the records
carry no schema reference to check a stride against, and rejecting a malformed histogram
would fail a read that succeeds today. `nan_count` and the histogram entries use `readI64`
rather than `readNonNegativeI64`, whose guards exist where a negative value would drive an
allocation or a file offset.

Tolerance stops at the point where it would corrupt the parse. The shared `list<i64>` read
checks the **collection header's element type**, not just the field's type code, and skips
element-wise when it is anything else. A `list<struct>` at a histogram field is the shape
found in the wild by #608; decoding those bytes as varints leaves the cursor mid-struct
and silently corrupts every field that follows.

Elements are skipped through `skipElement`, which differs from `skipField` for exactly one
type. A `bool` field carries its value in the type nibble of its own header and has no
payload; a `bool` element has no header and is a bare byte. Skipping a `list<bool>` by
field rules would consume none of it and leave the cursor on the first element, which is
the desync the element-type check exists to prevent.

`readListHeader` rejects a long-form element count larger than the bytes left in the
buffer, so an `ArrayList` presized from it is bounded by the footer's own length. The new
list fields inherit that bound from the existing `nullCounts` and `page_locations` cases.

## Testing

The reader unit tests build Thrift structs by hand, covering each field present, absent,
present-but-empty, and mistyped. `size_statistics_test.parquet` then asserts against
writer-produced bytes: a `BYTE_ARRAY` column with nulls, a `LIST`, and a `DOUBLE`
containing NaN, written by PyArrow 24.0.0 with the page index enabled.

PyArrow writes **no** NaN count for any column, including the one holding a NaN, and no
other available writer emits `Statistics` field 9 or `ColumnIndex` field 8 either. Those
two fields have positive coverage only in the unit tests; the fixture asserts them absent,
pinning that an omitted count reads back as `null` rather than zero.
