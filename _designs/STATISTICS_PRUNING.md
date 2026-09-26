# Statistics pruning

How a file's metadata decides, for a row group or a page, whether no row, some rows or every row can match a resolved predicate. It covers the three statistics a unit carries (min/max, null count, definition level histogram), how bounds are judged readable, the row-group absence probes (bloom filters and dictionaries), and page pruning through the column index and inline page statistics. It does not cover the predicate itself or its resolution ([PREDICATE_MODEL.md](PREDICATE_MODEL.md)), per-row evaluation and how an always-matching row group is consumed ([RECORD_FILTERING.md](RECORD_FILTERING.md)), page fetch mechanics ([FETCH_PLANNING.md](FETCH_PLANNING.md)), or the Thrift parse rules for the metadata structures ([FILE_METADATA.md](FILE_METADATA.md)).

## Decisions

`FilterDecision` (`internal.predicate`) is the answer every pruning question returns:

| Decision | Meaning | Proven by |
|---|---|---|
| `CANNOT_MATCH` | no row of the unit matches; skip it | min/max, null count, histogram, bloom filter, dictionary, geospatial bounding box |
| `MIGHT_MATCH` | undecided; evaluate the rows | the default wherever metadata is absent, partial or untrusted |
| `ALWAYS_MATCHES` | every row matches; per-row evaluation is redundant | min/max, null count and histogram only |

Bloom filters and dictionaries answer "is this value present?". That bounds what a unit can hold and says nothing about what every row does hold, so neither ever raises a decision to `ALWAYS_MATCHES`.

The contract that everything below serves: pruning never disagrees with the per-row matchers. A unit is dropped only when the matchers would return none of its rows, and promised in full only when they would return all of them. Every predicate of the literal corpus is read through the row reader, the record path, the column reader, and both with metadata filtering off, and checked against an oracle; `tools/predicate-audit` runs the same comparison over a larger matrix in the PR build.

`hardwood.metadata-filtering=false` turns off every metadata-driven decision: no row group or page is dropped and no row group is promised in full (`RowGroupIterator.filterRowGroups`).

Tests: `PredicatePathAgreementTest`, `RowGroupDecideTest`, `MetadataFilteringOptionTest`.

## Units

A unit is one column's statistics over a stretch of rows. `UnitStats` is a sealed interface with one implementation per place Parquet carries those statistics:

| Implementation | Sourced from | `rowCount()` |
|---|---|---|
| `ChunkStats` | `ColumnMetaData.statistics()` and `sizeStatistics()` | `RowGroup.numRows()` |
| `IndexPageStats` | the `ColumnIndex` entry at one page index, with the `OffsetIndex` locating it | next page's `firstRowIndex` minus this page's; the row group's row count minus it for the last page |
| `InlinePageStats` | `Statistics` on a `DataPageHeader` or `DataPageHeaderV2` | `DataPageHeaderV2.numRows()`; for v1, `numValues()` on a column with no repeated node above it, `UNKNOWN_ROW_COUNT` below one |

Sourcing resolves where a statistic comes from once, and absorbs every guard against it not being there: a column ordinal the row group does not carry, absent `ColumnMetaData`, absent `Statistics`, an absent null count or histogram. A unit that cannot be sourced has every statistic unknown and proves nothing. Every proof that needs a row count yields `MIGHT_MATCH` under `UNKNOWN_ROW_COUNT`.

`UnitStats.decide(leaf, logContext)` is the single entry point. It names every leaf type of `ResolvedPredicate` in an exhaustive switch, so a new leaf does not compile until it states which statistic answers it, and in particular whether a null can satisfy it. `AND` and `OR` passed to it throw `IllegalArgumentException`. Row-group filtering, column-index page filtering and the inline page drop all go through it, so a chunk and a page carrying the same statistics prove the same thing.

What is shared is the leaf decision only. The `AND`/`OR` fold stays per evaluator, because the row-group side folds `FilterDecision`s and the page side folds `RowRanges`. The absence probes stay in `RowGroupFilterEvaluator`, because Parquet carries bloom filters, dictionaries and geospatial statistics per column chunk only.

Tests: `UnitStatsTest`, `PageDropPredicatesTest`.

## The three statistics

### Min/max

`MinMaxStats` holds a unit's bounds decoded once, as the primitives the comparisons use: `IntStats`, `LongStats`, `UnsignedIntStats`, `UnsignedLongStats`, `BooleanStats`, `FloatStats` (which also carries `FLOAT16`), `DoubleStats` and `BinaryStats`. `MinMaxStats.of(Statistics, …)` sources a chunk or inline page, `MinMaxStats.ofPage(ColumnIndex, …)` a column-index page.

A pair the filter layer cannot compare against never becomes a typed variant. It becomes `NoBounds`, which drops nothing and promises nothing, and carries the reason:

| Reason | When |
|---|---|
| none | the file wrote no pair, or only one half; the leaf reads no bounds; an `INT96` column; a `STORED_BYTES` comparison |
| `DEPRECATED_SORT_ORDER` | only the deprecated `min`/`max` fields (1, 2) are present, which compare signed |
| `UNKNOWN_SORT_ORDER` | `BoundsReadability` marks the column unreadable (see below) |
| `NOT_A_NUMBER` | a floating-point bound is `NaN` |
| `INVERTED` | `min` sorts above `max` in the column's order |
| `NOT_THE_COLUMN_WIDTH` | a `FIXED_LEN_BYTE_ARRAY(12)` timestamp bound of another width |

A discarded pair is reported per unit through `reportIfDiscarded`, naming the file, row group, column and page. Deciding usability where the bounds are sourced keeps every comparator free of the question.

An unsigned integer column (`INT(bitWidth, isSigned = false)`) biases bounds and literal by `Integer.MIN_VALUE` / `Long.MIN_VALUE` before the signed comparisons, the same reordering the writer applies when it records them; `IN` probes compare with `compareUnsigned` in place. A binary column compares in the order its `Comparison` names, so a signed decimal is not read as an inverted pair.

The per-operator rules live in `StatisticsFilterSupport`, as pure functions of a literal `v` and bounds `[min, max]`:

| Operator | `CANNOT_MATCH` when | Interval satisfies it when |
|---|---|---|
| `EQ` | `v < min` or `v > max` | `min == max == v` |
| `NOT_EQ` | `min == max == v` | `v < min` or `v > max` |
| `LT` | `min >= v` | `max < v` |
| `LT_EQ` | `min > v` | `max <= v` |
| `GT` | `max <= v` | `min > v` |
| `GT_EQ` | `max < v` | `min >= v` |
| `IN` | no probe lies in `[min, max]` | `min == max` and that value is a probe |

Truncated bounds need no handling: truncation only widens the interval, and an interval that proves something widened proves it narrowed.

Tests: `MinMaxStatsTest`, `InvertedStatisticsFilterTest`, `UnsignedIntegerFilterTest`, `FilterDecisionTest`.

### Null counts

`NullStats` carries a unit's null count against its row count and answers a leaf null predicate:

- `IS NULL`: `CANNOT_MATCH` on `nullCount == 0`, `ALWAYS_MATCHES` on `nullCount == rowCount`.
- `IS NOT NULL`: `CANNOT_MATCH` on `nullCount == rowCount`, `ALWAYS_MATCHES` on `nullCount == 0`.
- A value predicate: `CANNOT_MATCH` on `nullCount == rowCount`, `NOT_EQ` included, since a null satisfies none of them. This is tested before the bounds, which on a page the column index flags null-only are placeholders.

`nullCount == rowCount` counts rows only because the leaf writes one entry per row and is null exactly where the named node is absent. `FilterPredicateResolver` guarantees that for every leaf predicate that reaches `NullStats`: it refuses a repeated column to a directly named leaf, and it gives a null predicate on a group separated from its leaf by a definition level the levels that send it to the histogram in `decide` (see the gate below).

`IndexPageStats` reads a set `ColumnIndex.nullPages[i]` as `nullCount == rowCount()`, so the flag and the count are one rule.

Tests: `UnitStatsTest`, `PageFilterEvaluatorTest`.

### Definition level histograms

`DefinitionLevelStats` carries one histogram with one bucket per definition level up to the leaf's maximum. A null predicate on a node at definition level `d` splits the buckets at `d`: entries below were written with the node absent, entries at or above with it present.

- `CANNOT_MATCH` when no bucket on the predicate's side holds an entry.
- `ALWAYS_MATCHES` when no bucket on the other side holds an entry **and** the histogram totals `rowCount()` entries. The total is what makes "every entry" mean "every row"; a leaf below a `LIST` or `MAP` writes one entry per element.
- `MIGHT_MATCH` when the histogram is absent, or its length is not `leafDefinitionLevel + 1` (`sizedFor`).

The histogram is the only statistic that separates an absent group from a present group whose leaf is null. A leaf predicate is the case `definitionLevel == leafDefinitionLevel`.

### Routing: the group gate

```
IS NULL / IS NOT NULL
  group()  (definitionLevel < leafDefinitionLevel)  -> histogram only; MIGHT_MATCH without one
  leaf, histogram sizedFor(leafDefinitionLevel)     -> histogram
  leaf, otherwise                                   -> null count
value predicate                                     -> null count (all-null), then min/max
EveryNonNullRowPredicate                            -> null count, as IS NOT NULL
NoRowPredicate                                      -> CANNOT_MATCH
GeospatialPredicate                                 -> MIGHT_MATCH (no unit carries it)
```

A group predicate must never reach the null-count rules. The leaf it is answered from may be repeated, so its null count mixes absent groups with null elements over more entries than rows. The gate is `definitionLevel < leafDefinitionLevel`, tested once, in `decide`. It follows the schema's structure rather than its annotations: every repeated node adds a definition level, so every `LIST` and `MAP` encoding, legacy ones included, passes as a group; a group with only required nodes down to its leaf has equal levels and is answered as the leaf.

The length guard protects the leaf branch. A leaf predicate carrying levels `0` on an optional column would read a two-bucket histogram at the wrong level and count every entry as present. Only a histogram of length 1 is sized for level `0`, and that belongs to a required column whose real level is `0`; any other length sends the predicate to the null count.

Tests: `UnitStatsTest`, `PageFilterEvaluatorTest`.

### `ALWAYS_MATCHES` and composition

A value leaf is `ALWAYS_MATCHES` when its bounds are usable, the whole interval satisfies the operator, and `NullStats` proves `nullCount == 0`. `MinMaxStats.decideLeaf` takes the null-free fact as a parameter, so a caller cannot promise every row while leaving null rows unaccounted for. A unit recording no null count is not null-free.

Floating-point leaves (`FLOAT`, `DOUBLE`, `FLOAT16`) never yield `ALWAYS_MATCHES` at any granularity: `NaN` sits outside the bounds, and a recorded `nan_count` of zero is not used to promote a fully satisfying interval.

Composition on the row-group side (`FilterDecision.and` / `or`, folded in `RowGroupFilterEvaluator`):

| | `AND` | `OR` |
|---|---|---|
| `CANNOT_MATCH` | any child is | every child is |
| `ALWAYS_MATCHES` | every child is | any child is |
| empty composite | `MIGHT_MATCH` | `MIGHT_MATCH` |

`ResolvedPredicate` has no `NOT`; negation is pushed into the leaves during resolution ([PREDICATE_MODEL.md](PREDICATE_MODEL.md)).

Tests: `FilterDecisionTest`, `UnitStatsTest`, `RowGroupDecideTest`.

## Bounds readability

Comparing a literal against `min`/`max` is sound only in the order the bounds were written in. `BoundsReadability` answers, per leaf ordinal, whether that order is known. A column's bounds are unreadable when:

- its annotation names no order: `INTERVAL`, `NULL`, `VARIANT`, `GEOMETRY`, `GEOGRAPHY`, `LIST`, `MAP`;
- the file's `column_orders` entry is a union member this build does not recognize (`ColumnOrder.UNKNOWN`), which `parquet.thrift` says to treat as "ignore min and max";
- the reader dropped the column's annotation, because this build does not recognize it or the physical type cannot carry it. The column reads as its physical type, but the writer ordered the bounds by the annotation.

Readability is a property of the file that wrote the bounds. `BoundsReadability.of(schema, footer)` is built once per file when the file is prepared (`FileMetadataCache`), indexed by that file's own leaf ordinals, and carried on `FileColumnOrdinals` beside the predicate translated to the same ordinals. An ordinal outside the file's schema is a wiring error and throws `IllegalStateException`.

`MinMaxStats` consults readability after establishing that a pair exists and before decoding it, so an unordered column whose writer recorded no bounds reports no discard. Only the min/max half is withheld on the chunk and column-index paths: the null count needs no order, and bloom filters and dictionaries test exact stored values. The inline page path withholds every AND-necessary leaf of such a column instead (`RowGroupIterator` hands `SequentialFetchPlan` an empty list), and `PageDropPredicates.canDropPage` then passes `BoundsReadability.ALL`. Untested.

`BoundsReadability.namesAnOrder` is an exhaustive switch over `LogicalType`. It mirrors `StatisticsOrder#supportsBounds` on the write side without delegating to it: the writer asks whether to record bounds, the reader whether to trust recorded ones, and both fail to compile on a new annotation until each states its answer. `FilterPredicateResolver` asks the same method whether a column takes ordered operators at all.

`Statistics` keeps its bounds either way; metadata surfaces such as `hardwood inspect` report what the file holds.

Tests: `UnreadableSortOrderTest`, `UnitStatsTest`.

### Column order and signed zeros

`FileMetaData.column_orders` names each leaf's order as `TYPE_DEFINED_ORDER` or `IEEE754_TOTAL_ORDER`; an absent list means the type-defined order throughout. The order describes the statistics only. Matchers compare floating-point values with `Float.compare` / `Double.compare` whatever a file declares, so the order decides how bounds are read, never which rows match.

The two orders differ for pruning at `NaN` and at `±0`. Under the type-defined order the spec leaves zeroes interchangeable: a `+0` minimum may hide `-0`, a `-0` maximum may hide `+0`. `StatisticsFilterSupport.canDropFloat`, `canDropDouble` and the `IN` variants widen a zero minimum to `-0.0` and a zero maximum to `+0.0` before comparing, and `MinMaxStats` applies the same widening before its inversion test so that `(+0, -0)` is not read as inverted.

The widening applies under **every** declared order. A predicate is resolved once per read and applied to every file's row groups, and the files of one read may declare different orders (PyArrow writes the type-defined order, parquet-java from 1.18.0 the total order for floating-point columns); a flag taken from one file would skip the widening on a sibling that needs it. On a total-order column the widening costs at most a unit the record filter then empties, never a matching row.

Tests: `UnreadableSortOrderTest`, `MixedColumnOrderPruningTest`, `NaNStatisticsFilterTest`, `InvertedStatisticsFilterTest`, `TotalOrderFloatReadTest` (parquet-testing-runner).

### NaN

A usable floating-point pair describes the unit's non-`NaN` values only. A pair holding `NaN` is discarded where it is sourced (`NOT_A_NUMBER`), so no comparator sees one.

Only a recorded `nan_count` of zero proves a unit holds no `NaN`; an absent count proves nothing. Where the unit is not proven `NaN`-free, an operator a `NaN` row satisfies never drops. In the `Float.compare` total order, where `NaN` equals `NaN` and sorts above everything:

| Operator | a `NaN` row satisfies it when the literal is |
|---|---|
| `EQ`, `LT_EQ` | `NaN` |
| `NOT_EQ`, `GT` | not `NaN` |
| `GT_EQ` | anything |
| `LT` | never |

A floating-point `IN` list containing a `NaN` probe never drops on bounds. Chunk units read `Statistics.nanCount`, column-index pages `ColumnIndex.nanCounts[i]`.

Tests: `NaNStatisticsFilterTest`, `PageFilterEvaluatorTest`.

### INT96 and stored-byte comparisons

`parquet.thrift` leaves the type-defined order of `INT96` undefined, and the `INT96_TIMESTAMP_ORDER` it adds (day, then nanoseconds) does not follow the instant, since the nanoseconds of the day are not bounded by one day. `INT96` bounds are never read and nothing is reported as discarded. The same holds for a `STORED_BYTES` comparison, which tests equality on raw bytes of a column whose bounds are in value order. Type-specific comparison rules are in [PREDICATE_MODEL.md](PREDICATE_MODEL.md#per-column-type).

Tests: `UnreadableSortOrderTest`.

## Statistic sources

What pruning relies on from the parsed metadata. Parse tolerance itself is in [FILE_METADATA.md](FILE_METADATA.md).

- **Absent is distinct from present-but-empty.** Every optional statistic surfaces as `null` when absent. PyArrow writes a non-repeated column's repetition-level histogram present but empty in `SizeStatistics` and absent in the `ColumnIndex`. An empty definition-level histogram fails `sizedFor` and proves nothing. For `nan_count` the distinction carries the conclusion: only a recorded `0` proves no `NaN`.
- **Histogram layout.** `SizeStatistics` holds one histogram of `maxLevel + 1` entries per chunk. `ColumnIndex` holds one per page, concatenated page-major; `definitionLevelHistogram(i)` slices page `i` at stride `length / pageCount`. `ColumnIndexReader` rejects a histogram whose length is not a whole number of entries per page; a divisible length with the wrong stride is caught by `sizedFor`.
- **Per-page arrays agree.** `ColumnIndexReader` rejects `min_values`, `max_values`, `null_counts` or `nan_counts` whose length differs from `null_pages`. `PageFilterEvaluator` rejects a `ColumnIndex` and `OffsetIndex` that disagree on the page count, with a `ParquetReadException` naming the column. Page filtering indexes all of them with one page index. An `OffsetIndex` that locates no page for a chunk whose `num_values` is positive is rejected as it is parsed against the chunk's metadata (`OffsetIndexReader`), for page filtering and fetch planning alike; page filtering would otherwise keep none of the chunk's rows.
- **Null pages.** The `min_values` and `max_values` entries of a null page are placeholders and are not decoded.

Tests: `SizeStatisticsMetadataTest`, `MalformedMetadataValidationTest`, `PageFilterEvaluatorTest`, `EmptyOffsetIndexTest`.

## Row groups

`RowGroupIterator.filterRowGroups` decides every row group of a file when the file is planned, before any of its row groups is read. `RowGroupFilterEvaluator.planRowGroup` folds the predicate; each leaf is decided in two steps:

1. `ChunkStats.of(rowGroup, column, readability).decide(leaf, …)`.
2. If that is not `CANNOT_MATCH`, the absence switch in `RowGroupFilterEvaluator` probes the chunk's bloom filter. A leaf the statistics drop reaches no probe, so bloom filter I/O happens only where the decision needs it. Untested.

`GeospatialPredicate` is decided in the fold from `ColumnMetaData.geospatialStatistics().bbox()`, which lives on the chunk alone ([LOGICAL_TYPES.md](LOGICAL_TYPES.md)).

A `CANNOT_MATCH` row group is dropped; a surviving one carries whether it is `ALWAYS_MATCHES` and the `LeafDecisions` recorded for each leaf. Dictionaries are probed later, in `RowGroupIterator.computeSharedMetadata`, when the read reaches the row group: `refineWithDictionaries` replays each recorded leaf decision and reads a dictionary only for a leaf left open, which re-reads no bloom filter and repeats no statistics warning. A leaf planning never reached, behind an `OR` branch that already always matched, replays as `MIGHT_MATCH`. The dictionaries read are kept in the row group's shared metadata and handed to its fetch plans, so the dictionary page is read once. The refined decision is used only to drop the row group; the always-match flag comes from planning.

The absence switch names every leaf type, so a new leaf does not compile until it states what proves its literal absent. Only `EQ` and `IN` are probed, and a binary leaf only where its `Comparison` is `byteExact()`, meaning the leaf matches by stored bytes. A value comparison on a `BYTE_ARRAY` `DECIMAL` (`VARIABLE_DECIMAL`) or an `INT96` (`INT96_INSTANT`) is never probed; a `byte[]` literal on such a column resolves to a `STORED_BYTES` leaf, which is. A `BOOLEAN` leaf is never probed; its bounds decide it. Signed and unsigned integer columns probe alike, since both shortcuts test stored bits.

Tests: `DictionaryPushDownTest`, `DictionaryPushDownIoTest`, `BinaryDecimalFilterTest`.

### Bloom filters

`RowGroupBloomFilterSource` reads one chunk's split-block bloom filter lazily on first probe and caches it, absence included, for the row group's evaluation. Hashing is XXH64 with seed 0 over the value's plain encoding: `INT32`/`INT64` little-endian, `FLOAT`/`DOUBLE` their raw IEEE-754 bits, binary its bytes (`XxHash64`). Hash and membership agree with parquet-java's `BlockSplitBloomFilter`.

| Rule | Reason |
|---|---|
| `EQ` drops when the value is absent | a clear bit is definitive |
| `IN` drops only when every probe is absent | one possibly present value keeps the group |
| a `NaN` probe is never bloom-pruned; one `NaN` in an `IN` list disables the list's bloom check | raw-bit hashing separates `NaN` payloads that `Float.compare` treats as equal |
| `±0` is hashed at its own bits | matchers separate `-0.0` from `+0.0` too |
| a `FLOAT16` value leaf is never bloom-probed (a `byte[]` literal on the column resolves to a `STORED_BYTES` leaf, which is) | the filter hashes the 2-byte stored form; narrowing the literal to binary16 is lossy and could prove the wrong value absent. Untested. |
| a missing, non-positive `bloom_filter_offset` or a header naming an unimplemented algorithm, hash or compression means no filter | the file is readable; only the shortcut is lost |
| an `IOException` reading a declared filter fails the read | a filter the footer declares and the file cannot deliver is corruption. Untested. |
| a chunk whose data lives in another file (`file_path`) fails | reading its offset in this file would prune on unrelated bytes |

An absent `bloom_filter_length` is handled by probing the header to learn the length.

Tests: `BloomFilterPushDownTest`, `BloomFilterParquetJavaOracleTest`.

### Dictionaries

A dictionary proves absence only when it enumerates every non-null value of the chunk. `RowGroupDictionaryFilterSource` reads it only for a chunk whose `encoding_stats` record at least one `DICTIONARY_PAGE`, at least one data page, and no data page in an encoding other than `PLAIN_DICTIONARY` or `RLE_DICTIONARY`. `INDEX_PAGE` entries are ignored; an unrecognized page type or absent `encoding_stats` makes the chunk ineligible. A writer that falls back to plain pages leaves a dictionary covering a prefix of the data.

The dictionary page is the chunk's first page, located at `dictionary_page_offset` when the file declares a positive one and at `data_page_offset` otherwise; absence of the former is ordinary. The page length comes from its own header; the gap to `data_page_offset` only sizes the opening read, since writers have understated `data_page_offset` (DuckDB before duckdb/duckdb#10829 left out the dictionary page's header). A first data page preceding the dictionary page, and a header length running past the chunk, are rejected with `ParquetReadException`.

Membership uses the matchers' comparison, so pruning agrees with the rows a read returns:

- `INT32`, `INT64`, binary: value equality.
- `FLOAT`, `DOUBLE`: `Float.compare` / `Double.compare`. A dictionary holding only `-0.0` proves `+0.0` absent, and a dictionary holding any `NaN` reports a `NaN` probe present. The `±0` ambiguity of statistics concerns bounds, not stored values, so dictionary pruning does not consult `ColumnOrder`.
- `FLOAT16`: each 2-byte entry is widened to `float` and compared with `Float.compare`; the literal is never narrowed, so a literal binary16 cannot represent matches nothing, as a full scan would find.

Tests: `RowGroupDictionaryFilterSourceTest`, `DictionaryPushDownTest`.

## Pages

Page pruning reads `CANNOT_MATCH` only; see Boundaries for why the always-match half is discarded.

### Column index

`PageFilterEvaluator.computeMatchingRows` runs once per surviving row group when the read reaches it, and yields the `RowRanges` that might match. A leaf is decided page by page over `IndexPageStats`, and a page is kept unless it is `CANNOT_MATCH`; kept pages become row ranges through `RowRanges.fromPages`. Columns have independent page boundaries, so composition works in row space: `AND` intersects its children's ranges, `OR` unions them. A leaf whose column has no column index or offset index, a `GeospatialPredicate`, and an empty `OR` yield `RowRanges.all`.

`IS NULL` drops column-index pages like any other leaf here, since a page this path skips is masked out of every decoded column rather than replaced. Each decoded column (the projected columns and any filter-only column) then fetches the pages overlapping the ranges, and a row group where any decoded column cannot apply row masks falls back to all rows; both are [FETCH_PLANNING.md](FETCH_PLANNING.md).

Tests: `PageFilterEvaluatorTest`, `RowRangesTest`.

### Inline page statistics

A column chunk with no offset index is read through `SequentialFetchPlan`, which walks page headers and can drop a page from the `Statistics` in its header. The offset index decides the path: a chunk that has one takes the column-index path, so a chunk with an offset index but no column index is pruned by neither its own column index nor its inline statistics.

- **Leaves.** `PageDropPredicates.byColumn` collects the AND-necessary leaves per column: it recurses into `AND`, skips every `OR` subtree, and keeps leaves. Falsifying such a leaf falsifies the predicate.
- **Placeholders.** A dropped page is replaced by `PageInfo.nullPlaceholder` with the rows the page contributes (the masked count where a row mask applies), decoded as an all-null page. Sibling columns stay row-aligned and the per-row filter rejects the null rows. Hence only a leaf a null row fails may drop a page: `IS NULL` is never collected.
- **Optional columns only.** A required column (`maxDefinitionLevel == 0`) cannot represent the placeholder's nulls and decodes every page. Untested.
- **Decision.** `canDropPage` builds an `InlinePageStats` and drops the page if any leaf is `CANNOT_MATCH`, from bounds or from a null count equal to the page's row count. A page header carries no histogram, so group null predicates are never decided here.

Tests: `PageDropPredicatesTest`, `PredicatePushDownTest`, `InlineNullPageDropTest`.

## Observability

| Event | Emitted | Reports |
|---|---|---|
| `RowGroupFilterEvent` | once per file, when its row groups are planned under a predicate | `totalRowGroups`, `rowGroupsKept`, `rowGroupsSkipped` by statistics and bloom filters, `rowGroupsFullyMatching` |
| `RowGroupDictionaryFilterEvent` | once per row group its dictionaries drop, when the read reaches it | `file`, `rowGroupIndex` |
| `PageFilterEvent` | once per projected column chunk with an offset index, in a row group whose rows the column index narrowed | `totalPages`, `pagesKept`, `pagesSkipped` |

A row group a dictionary drops counts as kept in `RowGroupFilterEvent`, which is committed before any dictionary is read. `PageFilterEvent` is emitted with `pagesSkipped == 0` when the predicate kept every page, and not at all for an unfiltered read, a chunk without an offset index, a row group the column index did not narrow, a closed mask gate, or pages dropped only by `tail(N)`. Inline-statistics drops emit no page event, because the sequential plan knows no page total without reading the headers it set out to avoid.

Tests: `RowGroupDictionaryFilterEventTest`, `RowGroupFilterEventTest`, `PageFilterEventTest`.

## Boundaries

- **Page-level `ALWAYS_MATCHES` is not read.** Units compute it and page filtering discards it. Reading it would save only per-row evaluation inside row groups the statistics left undecided, and for a range predicate on sorted data only the row group holding the cutoff is undecided; a kept page is fetched and decoded either way. The flag it would set changes value mid-page, since each column's page boundaries are its own, so every column's worker would have to split batches at that row, and `FlatRowReader` and `NestedRowReader` read the flag from one column's batch on the understanding that all columns flush at the same row. The per-row-group always-match decision and a column the predicate references but the projection does not are in [RECORD_FILTERING.md](RECORD_FILTERING.md).
- **Inline page drops are per column.** A page dropped on its inline statistics saves that column's decompression and decode only; sibling columns fetch and decode the same rows, because the drop is found while walking the column's page headers, after every fetch plan is built.
- **Absence probes are row-group scoped.** Parquet carries no per-page bloom filter or dictionary, so they stay outside `UnitStats`.
- **Geospatial `intersects` is row-group only.** `GeospatialStatistics` lives on `ColumnMetaData`; page units report it unknown ([LOGICAL_TYPES.md](LOGICAL_TYPES.md)).
- **Floating-point full matches and all-`NaN` units** (#898). A recorded `nan_count` of zero rules out a `NaN` row but does not promote a satisfying interval to `ALWAYS_MATCHES`, and a unit whose bounds are `NaN` (an all-`NaN` unit under the total order) is discarded rather than pruned.
- **Declared sort order is unused** (#838). `ColumnIndex.boundaryOrder` and `sorting_columns` are not consulted; page decisions walk every page.
- **Filter regions outside the file** (#1135). A bloom filter or dictionary chunk the footer places past the end of the file fails with `InputFile.readRange`'s bounds error rather than a `ParquetReadException` naming the footer field.
