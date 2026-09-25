# CLI value rendering

This document covers how the `hardwood` CLI and the `dive` TUI turn Parquet values and metadata figures into text: the shared value renderer for row values, dictionary entries and min/max statistics; the spelling of sizes, counts, percentages and absent values; and how chunk-level `SizeStatistics` (unencoded byte-array size, repetition and definition level histograms) are derived and presented. It does not cover colour and visual tiers, navigation or viewport virtualization, which are in [DIVE_UI_RULES.md](DIVE_UI_RULES.md), nor the dive screen model, which is in [DIVE_ARCHITECTURE.md](DIVE_ARCHITECTURE.md). How size statistics are parsed is in [FILE_METADATA.md](FILE_METADATA.md), and how they are used for pruning in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md); logical-type decoding itself belongs to [LOGICAL_TYPES.md](LOGICAL_TYPES.md). The user-facing rules are in `docs/content/reference/cli.md` (sections "Convert output", "Value rendering", "Absent values", "Binary values").

## The rule

A value or figure has one spelling on every surface. A reader moves between `print`, `convert`, the `inspect` commands, `info` and `dive` on the same file, so a timestamp, a decimal, a compressed size or an absent bound reads the same wherever it appears. A surface differs only where it has a stated reason: a terminal-cell budget, the physical-type toggle, or an export format.

The rule holds because every surface draws its text from a small set of helpers in `dev.hardwood.cli.internal`, and none spells a value on its own:

| Helper | Owns |
|---|---|
| `ValueFormatter` | Parquet values from every source: rows, dictionaries, statistics |
| `BinaryValues` | bytes the schema gives no interpretation for |
| `Strings` | the absent marker, the ellipsis, control-character sanitising, cell-width truncation and wrapping |
| `Sizes` | byte sizes and compression |
| `Encodings` | the data-page encoding label and dictionary cardinality |
| `LevelSummary` | everything derived from `SizeStatistics` |
| `Fmt` | `String.format` pinned to `Locale.ROOT` |

`RowTable` and `StreamedTable` hold table layout only; cell text comes from the helpers above.

## Value rendering

Two questions decide how a value renders: who reads the output, and whether its bytes passed core's decoders.

| Principle | Consequence |
|---|---|
| One spelling per logical type | A timestamp, decimal, interval, UUID or INT96 reads the same on every surface. Output formats differ only in how they quote and escape that text. |
| Display surfaces may transform, visibly | `print`, `dive`, `inspect` and `info` are read in a terminal. They sanitise control characters, truncate long values behind a marker (`…`, `…+N`), and write nested values in the display grammar. |
| Exports round-trip | `convert` output is read by programs. It never sanitises or truncates, and writes nested values as JSON: native objects and arrays in JSON output, JSON text in a CSV cell. The display grammar never appears in an export. |
| Never render a value as something it is not | Row values pass core's decoders, and `LogicalTypeValidator` rejects a fixed-width annotation on the wrong length when the schema is read, so a malformed row value reaching the renderer is a defect and throws. Statistics and dictionary entries come from the file unchecked; one that does not decode as its type renders in its stored form. |
| The physical toggle means the same everywhere | With `dive`'s logical-type toggle off, every source renders the stored value, list elements included: integers as integers, byte arrays through `BinaryValues`. |

### Entry points

`ValueFormatter` is a final class of static entry points, one per value source:

| Entry point | Source | Callers |
|---|---|---|
| `formatReader` | typed accessors on a `RowReader` | dive preview cells and record modal; `convert --format json` fields that are not JSON scalars |
| `formatValue` | a materialised value | `print` cells, `convert` CSV cells |
| `formatDictionary` | a raw primitive from a parsed `Dictionary` | dive dictionary screen |
| `formatBytes` | raw min/max statistics bytes | `inspect pages`, `inspect dictionary` byte-array entries, the dive pages, column index, column chunk detail and column-across-row-groups screens |
| `formatDecoded` | an already-decoded numeric dictionary entry | `inspect dictionary` numeric entries |

Two predicates are shared with `convert`. `isJsonScalar(PrimitiveNode)` holds for a `BOOLEAN`, `INT32`, `INT64`, `FLOAT` or `DOUBLE` with no annotation or an `INT` one, for top-level fields and nested leaves alike. `isNested(SchemaNode)` holds for groups and repeated primitives.

The `LogicalType` switch in `formatReader` is exhaustive over the sealed hierarchy with no `default` arm, so a new subtype fails to compile until it is handled. The dictionary and statistics switches keep a `default` arm that throws on a logical type the entry's physical type cannot carry.

### Styles

`ValueFormatter.Style` fixes the layout of nested values and whether text may carry control characters:

| Style | Layout | Control characters | Callers |
|---|---|---|---|
| `COMPACT` | one line, every element | sanitised | `print` |
| `PREVIEW` | one line, at most three entries per collection (the rest marked `…+N`) and three levels deep (deeper values marked `…`) | sanitised | dive preview cells |
| `EXPANDED` | one entry per line, two-space indent per level | sanitised | dive record modal |
| `EXPORT` | one line, every element; nested values as JSON | verbatim | `convert` |

### Nested values

`formatReader` and `formatValue` hand `PqStruct`, `PqList`, `PqMap` and `PqVariant` values to one walker, so a nested value reads the same from both. The walker resolves each child's schema node from its parent (struct fields by name, the list element, a repeated primitive as its own element, the map's key and value) and renders leaves against that node: annotated byte arrays decode as strings, UUIDs, decimals, intervals and timestamps, and unsigned `INT` annotations render unsigned. A value whose schema node does not resolve walks schema-less. A group schema paired with a scalar value throws.

The display styles use the display grammar: `{ a : 1 }` for structs, maps and Variant objects, `[1, 2]` for lists and Variant arrays, unquoted Variant keys and strings, `{}` and `[]` when empty. `EXPANDED` writes `name: value` entries one per line.

`EXPORT` writes JSON: a struct as an object keyed by field name, a list as an array, a map as an object keyed by the map key's rendered text, a Variant through the Variant JSON writer. A leaf inside an exported value is a JSON number or boolean where `isJsonScalar` holds for its schema node, and a JSON string otherwise; a non-finite float is always a string. A top-level leaf renders as plain text for the output format to quote.

### Spellings

| Value | Text |
|---|---|
| `TIMESTAMP` adjusted to UTC, `INT96` | `Instant.toString()`, e.g. `2025-01-01T00:00:00Z` |
| `TIMESTAMP` not adjusted to UTC; Variant timestamp without a time zone | `LocalDateTime.toString()` |
| `DATE`, `TIME` | `LocalDate` / `LocalTime` text |
| `DECIMAL` | `BigDecimal.toPlainString()`, never scientific notation |
| `UUID` | canonical 8-4-4-4-12 form |
| `INTERVAL` | non-zero parts as `3mo 2d 500ms`; all zero as `0ms` |
| `FLOAT16` | the widened `Float.toString` |
| unsigned `INT` | the unsigned decimal |
| `STRING`, `ENUM`, `JSON`, `BSON` | UTF-8 text |
| unannotated byte array, `GEOMETRY`, `GEOGRAPHY` | `BinaryValues`: text when the bytes are displayable text, else `0x` hex |
| `null` | `null`; JSON `null` inside an exported value |
| `INT96` under the physical toggle | `0x` hex |

`BinaryValues` decodes strictly: well-formed UTF-8 with no control characters that does not start with `0x` is text; anything else renders as `0x`-prefixed lowercase hex. A rendered `0x…` therefore always means bytes.

### Values that do not decode

A UUID that is not 16 bytes, an INTERVAL or INT96 that is not 12, a FLOAT16 that is not 2, a `FIXED_LEN_BYTE_ARRAY(12)` timestamp outside the Java range, a numeric statistic whose width does not match its physical type, or a TIME outside a day is not the value its type claims:

| Path | Behaviour |
|---|---|
| `formatReader`, `formatValue` | a wrong-length UUID, INTERVAL, INT96 or fixed-width TIMESTAMP throws `IllegalArgumentException` naming the field |
| `formatDictionary`, `formatBytes`, `formatDecoded` | renders the stored form (bytes as `0x` hex, a TIME as its integer), so a damaged bound or entry shows in its cell and the rest of the screen stays readable |

### Budgets and truncation

The budget unit is terminal display cells: a wide glyph counts two, a combining mark zero. `BinaryValues.NO_LIMIT` (`-1`) is the single unlimited sentinel; finite budgets must be at least 1, and anything else throws `IllegalArgumentException`. A budget bounds only hex building for binary payloads, so a multi-megabyte blob costs a cell rather than its own size. The hex runs one byte past the budget, so the caller sees the value is longer and marks the cut. A budget never cuts text.

Truncation is the caller's step, through `Strings.truncateRight`, which measures cells, never splits a surrogate pair, and counts the trailing `…` inside the width. Tables in `print`, `inspect dictionary` and `inspect pages` pass their column cap (`-w`) as the budget when truncating and `NO_LIMIT` under `--no-truncate`. Dive preview cells pass `PREVIEW_CELL_BUDGET`, a fixed budget wider than any terminal, because the rendered rows are cached before layout knows the width. Modals and exports pass `NO_LIMIT`.

### Control characters

`Strings.sanitizeControls` replaces each ISO control character with `·`; text made only of control characters becomes `0x` plus the hex of its UTF-8 bytes. The display styles apply it to every string leaf: reader strings, annotated byte arrays, Variant strings, Variant object keys and struct field names. The dictionary and statistics paths apply it to every decoded string, and `info` and the dive Overview to key-value metadata keys and values (`info --kv-key` prints the raw value). `EXPORT` writes strings verbatim; JSON escapes control characters through `JsonStrings`, and the CSV writer quotes a field containing a comma, a quote, a line feed or a carriage return.

Tests: `ValueFormatterTest` (cli), `BinaryValuesTest` (cli), `StringsTest` (cli), `JsonStringsTest` (cli), `PrintCommandTest` (cli), `ConvertCommandTest` (cli).

## Figures

| Figure | Spelling | Owner |
|---|---|---|
| Byte size | binary units with one decimal: `422 B`, `1.5 KiB`, `12.4 MiB`, `3.0 GiB`; never `1024.0` of a unit | `Sizes.format` |
| Byte size with exact count | `1.5 KiB  (1,536 B)`; the parenthetical is dropped below 1 KiB | `Sizes.dualFormat` |
| Compression | compressed as a percentage of uncompressed, one decimal: `39.0%`; lower is better | `Sizes.compression` |
| Share of a total | percentage with one decimal | callers, via `Fmt` |
| Dictionary cardinality | whole percent after the encoding label; `<1%` for a non-zero share that rounds to zero, since `0%` beside `DICT` would read as "no dictionary" | `Encodings.label` |
| Count | `%,d` with `Locale.ROOT` grouping: `1,048,576` | `Fmt`, `Plurals` |
| Count with a noun | `1 page`, `96 pages`; zero takes the plural | `Plurals.format` (dive) |
| Mean | two decimals: fan-out, average list length | callers, via `Fmt` |

Compression is always a percentage, on every screen and command. A `×` factor would describe the same quantity inverted. The one `×` on any surface is the unencoded size as a multiple of the compressed size, on the dive column chunk detail, which is a different quantity.

All formatting goes through `Fmt.fmt`, which pins `Locale.ROOT`, so the decimal point and grouping separator do not follow the host locale.

### Absent, empty and inapplicable

`Strings.ABSENT_VALUE` (`—`, U+2014) marks a quantity the file could carry and does not: a page count without an offset index, a bound without statistics, a compression with no uncompressed size, a key-value entry with no value. Every command and every dive screen uses it; a script matches it to detect an absent cell.

Absent is distinct from empty. `0 B`, `0` and `""` are values the writer recorded. An empty statistics value renders `""` on a `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` column so it is not mistaken for a blank cell.

Absent is distinct from inapplicable. A quantity that cannot exist for the row at hand reads `N/A` (an index page's data encoding in `inspect pages`). Structural states that are neither absent nor a value read as words in parentheses: `(null page)`, `(deprecated)`.

Whether a chunk carries an optional structure (a dictionary page, a column index, an offset index) reads `present` or `absent` on every surface.

### Output encoding

`Main` replaces `System.out` and `System.err` with UTF-8 streams when the platform charset is not UTF-8. The level bars, `—` and `⚠` are non-ASCII, and a native image fixes its default charset at build time, so no runtime `LANG` would otherwise reach it. Untested.

Tests: `SizesTest` (cli), `EncodingsTest` (cli), `FmtTest` (cli), `PluralsTest` (cli), `DiveRenderTest` (cli).

## Size statistics

`SizeStatistics` is the only metadata that separates an absent field from an empty list: `null_count` lumps both together. A raw definition-level histogram says nothing without the schema to name its buckets, and those names follow from the column's path alone. The CLI presents the chunk-level statistics in `ColumnMetaData` on two surfaces: the dive column chunk detail screen and `hardwood inspect columns`. The per-page copies in `ColumnIndex` and `OffsetIndex` are read only to report whether they exist.

### `LevelSummary`

Both surfaces build one `LevelSummary` per chunk with `LevelSummary.of(schema, column, metaData)`. It is a record holding the derived scalars, the labelled level rows and the consistency verdict. It performs no I/O; the bar rendering and level-row layout live in it so both surfaces draw the same characters.

The factory always returns a summary. A chunk with no `SizeStatistics` has a shape too, and for a fixed-width column the unencoded size follows from the value count. `hasSizeStatistics()` reports whether the file recorded one. Optional quantities are a `has…()` / value pair. `records()` and `presentValues()` throw when their `has…()` is false, because the fallback that serves a non-repeating or required column would count level slots as records, or nulls as values, for any other.

`LevelSummary.hasPageLevelHistograms(ColumnIndex)` and `hasPageUnencodedSizes(OffsetIndex)` answer whether the page index carries per-page size statistics.

### Level labels

Walk the column's path from the schema root and collect the nodes whose repetition is `OPTIONAL` or `REPEATED`; there are exactly `maxDefinitionLevel` of them, `d₁…d_maxDef`. Definition level `i` names the node the value failed to reach:

| Condition | Label |
|---|---|
| `i < maxDef`, `d(i+1)` is `REPEATED` | `<enclosing field> empty` |
| `i < maxDef`, `d(i+1)` is `OPTIONAL` | `<node name> null` |
| `i == maxDef` | `<leaf name> present` |

A repeated node is named for its enclosing field because the empty collection is a fact about the field the user knows: `websites empty`, not the synthetic `list` node of a LIST annotation, and a MAP's `key_value` names the map field. A top-level unannotated repeated field has no enclosing field and uses its own name. Repetition level 0 is `new record`; level `i` is the dotted path of the `i`-th repeated node.

### Derived quantities

| Quantity | Definition | Absent when |
|---|---|---|
| Records | `rep[0]`; `num_values` for a column that cannot repeat | repeated, no repetition histogram |
| Present values | `def[maxDef]`; `num_values` for a required column | nullable, no definition histogram |
| Nulls | `null_count` where written, else `num_values − present`; zero for a required column whatever was written | neither source available |
| Unencoded | `unencoded_byte_array_data_bytes` for `BYTE_ARRAY`; present values × width for fixed widths (`BOOLEAN` rounds up to the byte) | `BYTE_ARRAY` without the field, or present values unknown |
| Length prefixes | `4 × present values` for `BYTE_ARRAY` (the field excludes them); zero otherwise | present values unknown |
| Fan-out | `sum(def) / records`; `1.0` for a column that cannot repeat | records unknown or zero |
| Avg list length | non-empty elements ÷ records with a non-empty list, subtracting the buckets below the first repeated node from both sides | `maxRep ≠ 1`, or either histogram missing |
| Avg value size | unencoded ÷ present values | not `BYTE_ARRAY`, where it would restate the width |
| Level rows | count, share of the histogram total, bar | histogram absent, empty, or of the wrong length |

`LevelSummary.nullCount(Statistics)` decides the null count for every surface. A required column holds no nulls whether or not `null_count` was written, and reporting `—` there would contradict the present-value count printed from the same schema fact.

The unencoded size is the figure that predicts read-side cost: compressed and uncompressed both measure the encoded form, so a dictionary-encoded column looks cheap beside what it costs to materialise. No surface offers a verdict on the encoding from it. Comparing it with the uncompressed size yields a difference in uncompressed bytes, while the question a reader has is about compressed ones, and answering that needs a re-encode.

`Avg list length` is defined for one level of repetition only: with nested repetition a single average has no unambiguous referent, so the row is omitted rather than computed against an arbitrarily chosen level.

A histogram that is absent and one that is present but empty are both legitimate (a writer emits an empty definition histogram for a required, non-repeated column); neither is indexed into.

### Consistency check

A chunk checks itself. Every recorded histogram must have `maxLevel + 1` buckets; `num_values` must equal `sum(def)` and `sum(rep)`; `null_count` must equal `num_values − def[maxDef]`. The first failure becomes `mismatch()`, a one-line description such as `values 6,488,062, sum(def) 6,488,050` or `def histogram has 3 buckets, max def 3 needs 4`. A histogram of the wrong length is dropped from the level rows rather than paired with the wrong labels. Dive paints the mismatch as `⚠ Declared vs actual` in `Theme.error()`; `inspect` prints `⚠` and the wording, one line per offending row group under the table.

### Encoding label

`Encodings.dataPages(metaData)` names what the data pages use, from `encoding_stats` where written: that separates dictionary-encoded data pages from the dictionary page and exposes a mid-chunk fallback, `PLAIN+DICT`. Without `encoding_stats` it falls back to the declared list minus the level encodings. `Encodings.label` abbreviates (`DICT`, `DELTA`, `DELTA_LEN`, `DELTA_BA`, `BSS`) and joins with `+` in enum order, so the same chunk reads the same across runs. Where the chunk uses a dictionary and its entry count is known, the cardinality follows as a percentage of the present values, since nulls never reach a dictionary. `Encodings.dictionaryEntries` reads only the dictionary page header for that count; a chunk without a dictionary, one whose data lives in another file, or one whose page at the dictionary offset is not a dictionary page has no count; a header that fails to read raises with its file, row group and column, so a damaged dictionary is reported rather than hidden as a missing figure. The ranked table pays that one short read per column chunk because the percentage is the only figure from which a reader can find the column whose dictionary is a second copy of its data. `100%` says the dictionary is a second copy of the column; it is a number, not a recommendation.

### Dive column chunk detail

The facts pane groups rows under Identity, Storage, Content, the two level blocks, and Layout. A derived figure that qualifies a count rides on that count's row as a parenthetical: compression on `Compressed`, cardinality on `Encoding` (with the entry and value counts), fan-out on `Values`, shares on `Present` and `Nulls`, the multiple of compressed and the length prefixes on `Unencoded`.

| Row | Content |
|---|---|
| `Size statistics` | `chunk + N pages` when either page-index field is present (the histograms live in `ColumnIndex`, the unencoded sizes in `OffsetIndex`, and a required `BYTE_ARRAY` column has only the latter to write), `chunk only` otherwise, `— (not written)` without `SizeStatistics` |
| `Chunk encodings` | the declared list, shown only where `encoding_stats` exists and so can differ from `Encoding` |
| `Records`, `Present` | dropped where they would restate `Values` (a column that cannot repeat, cannot be null) or are unknown |
| `Avg list length`, `Avg value size` | dropped where undefined |
| `Def levels`, `Rep levels` | behind the `l` toggle, off by default because the derived rows above are the summary and the raw buckets a deliberate step further, advertised in the key bar only where a histogram exists |

A chunk with no usable histogram shows one advisory row per block (`— (required, every value present)`, `— (not repeated)`, `— (not written)`) and no toggle, since there is nothing to collapse. Level rows drop the bar first, then the percentage, as the pane narrows. The level blocks are bounded by the maximum levels, so they need no viewport virtualization. The pane is a cursor pane, and its title carries a line range whenever anything is hidden.

The drill menu hints `present · levels` on `Column index` and `present · unencoded` on `Offset index` when the page index carries per-page size statistics.

The column-across-row-groups screen is the interactive twin of `inspect columns --column` and carries `Unencoded` and `Nulls` from the same `LevelSummary`.

### `hardwood inspect columns`

The ranked table has a fixed column set: `Rank`, `Column`, `Type`, `Codec`, `Compressed`, `Share`, `Compression`, `Encoding`, `Unencoded`, `# Pages`. `Unencoded` and the dictionary cardinality sum per column path across row groups; a column reports `—` for `Unencoded` unless every chunk yields a figure, since a partial sum reads as a total. The `Encoding` cell is the union across row groups, so a fallback in any one of them shows. `# Pages` comes from the offset index: `—` for a chunk without one, and an offset index that fails to read fails the command, as a damaged dictionary header does.

`--column <path>` prints one row per row group (`RG`, `Values`, `Nulls`, `Records`, `Present`, `Fan-out`, `Codec`, `Compressed`, `Compression`, `Encoding`, `Unencoded`), then the mismatches, then the definition and repetition blocks. The column set is fixed whatever the column's shape, so runs compare and parse. Unlike dive, the table prints `Records`, `Present` and `Fan-out` wherever they are known, including where they restate `Values`. Level histograms sum element-wise across row groups (`LevelSummary.combineLevels`), so the file-wide block is exact; `--row-group <n>` narrows table and blocks to one row group. A block with no histogram is omitted. The blocks render at a fixed width, independent of the terminal, so output diffs cleanly.

Tests: `LevelSummaryTest` (cli), `EncodingsTest` (cli), `InspectColumnsCommandTest` (cli), `DiveRenderTest` (cli).

## Styling boundary

This document fixes the text. Colour is added only in dive and follows the tiers in [DIVE_UI_RULES.md](DIVE_UI_RULES.md): level rows and counts are body content, advisory `—` rows are dim, a mismatch is `Theme.error()`. Colour never carries information the text lacks, so the monochrome `inspect` output and the dive pane carry the same facts, and bars encode magnitude by length alone.

## Boundaries

- A `BSON` value renders as UTF-8 text on every surface, while core hands it back as bytes (#1206).
- An annotation the physical type cannot carry makes `print`, `convert` and `dive` fail instead of rendering the stored value (#1090).
- The column chunk detail's `Size statistics` row restates what the drill menu already shows (#951).
- `inspect` and dive do not report per-page size statistics beyond their presence; per-page level histograms have no display.
