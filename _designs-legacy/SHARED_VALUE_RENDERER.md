# Design: shared value renderer

**Status: Implemented.** Tracking issue: #1021 (also closes #865).

## Goal

Every Parquet value renders as one canonical text per logical type on every
CLI surface — `print`, `convert`, `inspect pages` / `inspect columns` /
`inspect dictionary` and `dive` — except where a surface has a stated reason to
differ: a terminal-cell budget, the physical-type toggle, or an export format.

## Principles

Two questions decide how a value renders: who reads the output, and whether
its bytes passed core's decoders.

| Principle | Consequence |
|---|---|
| One spelling per logical type | A timestamp, decimal, interval, UUID or INT96 reads the same on every surface. Output formats differ only in how they quote and escape that text. |
| Display surfaces may transform, visibly | `print`, `dive`, `inspect` and `info` are read in a terminal. They sanitise control characters, cap or truncate long values behind a marker (`…`, `…+N`), and write nested values in the display grammar. |
| Exports round-trip | `convert` output is read by programs. It never sanitises, caps or truncates, and writes nested values as JSON: native objects and arrays in JSON output, JSON text in a CSV cell. The display grammar never appears in an export. |
| Never render a value as something it is not | Row values pass core's decoders, and `LogicalTypeValidator` rejects a fixed-width annotation on the wrong length, so a malformed row value reaching the renderer is a defect and throws. Statistics and dictionary entries come from the file unchecked; one that does not decode as its type renders in its stored form: bytes as `0x` hex, an out-of-range integer as the integer. A rendered `0x…` always means bytes, since `BinaryValues` renders text that starts with `0x` as hex too. |
| The physical toggle means the same everywhere | With the toggle off, every source renders the stored value, list elements included: integers as integers, byte arrays through `BinaryValues`. |

## The value renderer

`dev.hardwood.cli.internal.ValueFormatter` is a final class of static entry
points, one per value source:

| Entry point | Source | Used by |
|---|---|---|
| `formatReader(RowReader, int fieldIndex, SchemaNode, boolean useLogicalType, Style, int budget)` | typed accessors on a `RowReader` | dive preview cells, dive record modal, `convert --format json` fields that are not JSON scalars |
| `formatValue(Object value, SchemaNode, Style, int budget)` | materialised values | `print` cells, `convert` CSV cells |
| `formatDictionary(Object raw, ColumnSchema, boolean useLogicalType, int budget)` | raw primitive out of a parsed `Dictionary` | dive dictionary screen |
| `formatBytes(byte[], ColumnSchema, boolean useLogicalType, int budget)` (+ `NO_LIMIT` overload) | raw statistics bytes | `inspect pages`, `inspect dictionary` byte-array entries, dive page, index and chunk screens |
| `formatDecoded(int\|long\|float\|double, …)` | decoded dictionary entries | `inspect dictionary` numeric entries |

Two predicates are shared with `convert`: `isJsonScalar(PrimitiveNode)` decides
which values are JSON numbers or booleans — a `BOOLEAN`, `INT32`, `INT64`,
`FLOAT` or `DOUBLE` with no annotation or an `INT` one — for top-level fields
and nested leaves alike; `isNested(SchemaNode)` decides which fields render as
nested values: groups and repeated primitives.

The `LogicalType` switch in `formatReader` is exhaustive over the sealed
hierarchy with no `default` arm, so a new subtype fails to compile until it is
handled there. The dictionary and statistics switches keep a `default` arm that
rejects a logical type the entry's physical type cannot carry.

### Styles

`Style` fixes the layout of nested values and whether text may carry control
characters:

| Style | Layout | Control characters | Used by |
|---|---|---|---|
| `COMPACT` | one line, every element | sanitised | `print` |
| `PREVIEW` | one line, at most `MAX_NESTED_ELEMENTS = 3` entries per collection (the rest marked `…+N`) and `MAX_NESTED_DEPTH = 3` levels (deeper values marked `…`) | sanitised | dive preview cells |
| `EXPANDED` | one entry per line, two-space indent per level | sanitised | dive record modal |
| `EXPORT` | one line, every element; nested values as JSON | verbatim | `convert` CSV cells and JSON fields |

### One nested walker

`formatReader` and `formatValue` hand nested values — `PqStruct`, `PqList`,
`PqMap`, `PqVariant` — to one walker. It resolves each child's schema node
from its parent (struct fields by name, the list element, a repeated
primitive as its own element, the map's key and value) and renders leaves
against that node: annotated byte arrays decode as strings, UUIDs, decimals,
intervals and INT96 timestamps, and unsigned `INT` annotations render
unsigned. A value whose schema node does not resolve walks schema-less. With
the physical toggle off, struct fields and map entries are read through their
raw accessors and byte arrays render as bytes. A group schema paired with a
scalar value throws.

The display styles use the display grammar: `{ a : 1 }` for structs, maps and
Variant objects, `[1, 2]` for lists and Variant arrays, unquoted Variant keys
and strings. Empty collections render `{}` and `[]`. `EXPANDED` writes
`name: value` entries one per line.

`EXPORT` writes JSON: a struct as an object keyed by field name, a list as an
array, a map as an object keyed by the map key's text, a Variant through the
Variant JSON writer. A leaf inside an exported value is a JSON number or
boolean when `isJsonScalar` holds for its schema node, a JSON string
otherwise, and a string when it is a non-finite float. A top-level leaf renders
as plain text for the output format to quote.

### Budget contract

The budget unit is terminal display cells. `BinaryValues.NO_LIMIT` (`-1`) is
the single unlimited sentinel; finite budgets must be `≥ 1`, and `0` or `≤ -2`
throw `IllegalArgumentException`. A budget bounds hex building for binary
payloads — the hex runs one byte past it, so the caller sees the value is
longer and marks the cut — and never cuts text. `print` passes `maxWidth` when
truncating and `NO_LIMIT` when wrapping; dive preview cells pass
`PREVIEW_CELL_BUDGET = 4096`; modals and exports pass `NO_LIMIT`.

### Null, empty and absent

- A `null` value renders `null`; inside an exported value that is JSON `null`.
- Absent statistics bytes render `-`; the dive screens show `—` for an absent
  bound.
- Empty statistics bytes render `""` on a `BYTE_ARRAY` or
  `FIXED_LEN_BYTE_ARRAY` column and an empty string on any other.

### Canonical forms

| Value | Text |
|---|---|
| `DECIMAL` | `BigDecimal.toPlainString()` from every source |
| `INT96` | `LogicalTypeConverter.int96ToInstant` text in logical mode, `0x` hex under the physical toggle |
| Variant timestamp without a time zone | `LocalDateTime` text, as for a `TIMESTAMP` column not adjusted to UTC |
| non-finite `FLOAT` / `DOUBLE` in JSON | a JSON string, top-level or nested |

### Values that do not decode

A UUID that is not 16 bytes, an INTERVAL or INT96 that is not 12, a FLOAT16
that is not 2, a numeric statistic whose width does not match its physical
type, or a TIME outside a day is not the value its type claims:

| Path | Behaviour |
|---|---|
| `formatReader`, `formatValue` | a wrong-length UUID, INTERVAL or INT96 throws `IllegalArgumentException` naming the field |
| `formatDictionary`, `formatBytes`, `formatDecoded` | renders the stored form — bytes as `0x` hex, a TIME as its integer — so a damaged bound or entry shows in its cell and the rest of the screen stays readable |

### Control characters

`Strings.sanitizeControls(String)` replaces each ISO control character with
`·`; text made only of control characters becomes `0x` + hex of its UTF-8
bytes. The display styles apply it to every string leaf — reader strings,
annotated byte arrays, Variant strings, Variant object keys and struct field
names — and the
dictionary and statistics paths to every decoded string. `info` applies it to
key-value metadata values, while `info --kv-key` prints the raw value.

`EXPORT` writes strings verbatim. JSON escapes control characters, and the
`convert` CSV writer quotes a field containing a comma, a quote, a line feed
or a carriage return.

## Callers

| Caller | Entry point |
|---|---|
| dive `PreviewWindow` (preview cells) | `formatReader(…, toggle, PREVIEW, PREVIEW_CELL_BUDGET)` |
| dive `PreviewWindow` (record modal) | `formatReader(…, toggle, EXPANDED, NO_LIMIT)` |
| dive `DictionaryScreen` | `formatDictionary(raw, col, toggle, maxChars)` |
| `PrintCommand` cells | `formatValue(value, field, COMPACT, budget)` |
| `ConvertCommand` CSV cells | `formatValue(value, schema, EXPORT, NO_LIMIT)` |
| `ConvertCommand` JSON scalar fields | typed accessors, where the field is not nested and `isJsonScalar` holds |
| `ConvertCommand` other JSON fields | `formatReader(reader, i, field, true, EXPORT, NO_LIMIT)`, written as-is when `isNested` holds and quoted otherwise |
| `InspectPagesCommand` bounds | `formatBytes(bytes, col, true, budget)` |
| `InspectDictionaryCommand` numeric entries | `formatDecoded(…)` |
| `InspectDictionaryCommand` byte-array entries | `formatBytes(bytes, col, true, budget)` |
| dive `PagesScreen`, `ColumnIndexScreen`, `ColumnAcrossRowGroupsScreen`, `ColumnChunkDetailScreen` | `formatBytes(…)` |
| `InfoCommand` metadata summary | `Strings.truncateRight(Strings.sanitizeControls(value), 60)` |

`RowTable` holds table and grid layout only; value text comes from
`ValueFormatter`.
