# Design: shared value renderer and width/truncation primitives

**Status: Planned.** Tracking issue: #1021 (width half also closes #866; control-character half also closes #865).

## Goal

Every Parquet value renders as exactly one canonical text per logical type on every
surface — `print`, `convert`, `inspect pages` / `inspect columns` / `inspect dictionary`
and `dive` — except where a surface has a stated reason to differ: a terminal-cell
budget, the physical-type toggle, or the JSON export grammar. Likewise every terminal
width measurement and every string truncation goes through one implementation, so emoji
and combining marks measure and cut correctly everywhere.

## The value renderer

`dev.hardwood.cli.internal.ValueFormatter` is the single dispatch. It is a final class
of static entry points parameterised by `(source, budget, nestedStyle)`; the switches
over `LogicalType` are sealed-exhaustive with no default arm, so a new `LogicalType`
subtype fails to compile until a case is added.

| Entry point | Source | Used by |
|---|---|---|
| `formatReader(RowReader, int fieldIndex, SchemaNode, boolean useLogicalType, NestedStyle, int budget)` | typed accessors on a `RowReader` | dive preview cells (COMPACT, capped, `PREVIEW_CELL_BUDGET`), dive record modal (EXPANDED, `NO_LIMIT`), `convert --format json` leaves |
| `formatValue(Object value, SchemaNode, int budget)` | materialised values | `print` cells, `convert` CSV cells |
| `formatDictionary(Object raw, ColumnSchema, boolean useLogicalType, int budget)` | raw primitive out of parsed `Dictionary` records | dive dictionary screen |
| `formatBytes(byte[], ColumnSchema, boolean useLogicalType, int budget)` (+ `NO_LIMIT` overload) | raw statistics bytes | `inspect pages` bounds, dive index/chunk screens |
| `formatDecoded(int\|long\|float\|double\|boolean, …)` / `formatDecoded(byte[], ColumnSchema)` | already-decoded dictionary entries | `inspect dictionary` |
| `variantJson(PqVariant)` | export writer | `convert --format json` variant values |
| `formatIntervalBytes(byte[])` | 12-byte INTERVAL payload | byte-backed paths |

`NestedStyle` is `COMPACT` (single line) or `EXPANDED` (multi-line record modal). Every
caller appears exactly once in the table above; no caller keeps local dispatch or schema
lookup.

### Budget contract

The budget unit is **terminal display cells**. `BinaryValues.NO_LIMIT` (`-1`) is the
single unlimited sentinel accepted by every entry point. Finite budgets must be `≥ 1`;
`0` and `≤ -2` throw `IllegalArgumentException` at the `ValueFormatter` entry before
delegation. Per-surface semantics are preserved: `print` values get `maxWidth` when
truncating and `NO_LIMIT` when wrapping; statistics hex is built only to budget (the
one-byte overshoot of `BinaryValues.toHex` is unchanged); dive preview keeps
`PREVIEW_CELL_BUDGET = 4096`; the record modal and dictionary modal use `NO_LIMIT`;
dictionary cells cap with the caller marking the cut.

### Null, empty and absent

- `null` value → the string `null`.
- Absent statistics bytes → `-` (`formatBytes(null, …)`), the documented absent-marker
  until statistics markers are centralised; the dive screens' own `—` guards are
  screen-level and unchanged.
- `byte[0]` → empty (unannotated) or `""` on the string-annotated statistics path.
- Empty string values → `""` (annotated) / empty, as today.

### Canonical forms

- `Decimal` renders `BigDecimal.toPlainString()` on **all** surfaces, including the
  decoded dictionary path (no `1E+7`-style scientific notation anywhere).
- `INT96` renders `LogicalTypeConverter.int96ToInstant` text in logical mode on **all**
  surfaces; under the physical toggle it renders `0x`-prefixed hex via `BinaryValues`,
  consistent with every other byte-backed type.
- Malformed fixed-size payloads (INT96, UUID, INTERVAL with wrong lengths) fail rather
  than truncate, pad or render misleading text.

### Control characters

`Strings.sanitizeControls(String)` is the one sanitiser: each ISO control becomes one
`·` cell; text whose characters are all controls becomes `0x` + hex of the UTF-8 bytes.
It applies to every string leaf of every source — reader string leaves, dictionary
UTF-8 leaves (including ENUM/JSON/BSON), statistics string bounds, display-grammar
Variant STRING leaves, Variant JSON STRING leaves before JSON escaping, and `info`
key-value metadata. **`info --kv-key` deliberately stays raw** so the original bytes
remain pipeable.

Accepted consequence: `convert` string exports (JSON and CSV, including STRING leaves
inside Variant JSON) contain `·` instead of the original control character — exports
are no longer byte-faithful for control characters.

### Display grammar vs export grammar

The unquoted display grammar — structs and maps `{ a : 1 }`, lists `[1, 2]`, unquoted
Variant keys — is the only grammar on table surfaces: `print`, `convert` CSV and `dive`
COMPACT/EXPANDED. `variantJson(PqVariant)` is the export-only JSON writer (quoted
strings and keys, JSON escapes) used solely by `convert --format json`. Nested element
and depth caps (`MAX_NESTED_ELEMENTS = 3`, `MAX_NESTED_DEPTH = 3`) apply only on the
dive-preview COMPACT path; `print` and `convert` render nested values whole.

## Width and truncation

All width math routes through `dev.tamboui.text.CharWidth` via `Strings`:
columns, padding, wrap slicing and minimum widths — including `Strings.truncateLeft`
and `Strings.wordWrap`, `Chrome`, `OverviewScreen`, `DictionaryScreen`,
`DataPreviewScreen`, `RowTable` and `StreamedTable`. `RowTable`'s hand-rolled width
table is deleted. Java's extended-grapheme matcher `\X` (one precompiled `Pattern`)
supplies cluster iteration, because TamboUI exposes no public grapheme iterator:

- `Strings.firstGlyph(String)` — empty → 1; otherwise the first `\X` cluster, width
  `max(1, CharWidth.of(cluster))`. A leading or combining-only zero-cell cluster is
  assigned a one-cell progress floor.
- `Strings.widestGlyph(String)` — `max(1, CharWidth.of(cluster))` over all clusters, so
  a wrapping column is never sized below an indivisible glyph (ZWJ families and
  regional-indicator flags are each one width-2 cluster).
- `Strings.prefixByWidth(String, start, budget)` — consumes whole clusters from the
  cluster boundary `start` while they fit; if the first cluster is wider than a positive
  budget, the whole cluster is returned (documented one-line overflow; never a
  one-code-point fallback).
- `Strings.suffixByWidth(String, budget)` — discards whole leading clusters until the
  suffix fits; returns only at a cluster boundary.

`Strings.truncateRight` and `Strings.truncateLeft` are the two truncation
implementations, and `Strings.ELLIPSIS` is the only ellipsis spelling in the codebase.
`truncateLeft` returns the original when it fits; otherwise it reserves the ellipsis
width and takes `suffixByWidth`, or returns only `ELLIPSIS` if no complete cluster
fits. `Strings.wordWrap` measures cells and chunks long words through `prefixByWidth`.
`StreamedTable` truncates with `Strings.truncateRight` and slices wrapped lines with
`prefixByWidth` (its `displayPrefixEnd` is deleted); `Chrome.columnLeafName` uses
`truncateLeft`; `OverviewScreen.trim`, `DictionaryScreen.formatValue` and
`DataPreviewScreen`'s clip marker and truncate helper use `truncateRight` and
`Strings.ELLIPSIS`; the nested-cap `…+N` markers build from `Strings.ELLIPSIS`.

## Callers

| Caller | Entry point |
|---|---|
| dive `PreviewWindow` (preview cells) | `formatReader(…, COMPACT, toggle, PREVIEW_CELL_BUDGET)` |
| dive `PreviewWindow` (record modal) | `formatReader(…, EXPANDED, toggle, NO_LIMIT)` |
| dive `DictionaryScreen` | `formatDictionary(raw, col, toggle, maxChars)` |
| `PrintCommand` cells | `formatValue(value, field, budget)` |
| `ConvertCommand` CSV cells | `formatValue(value, schema, NO_LIMIT)` |
| `ConvertCommand` JSON variant | `variantJson(variant)` |
| `ConvertCommand` JSON leaves | `formatReader(reader, i, field, true, COMPACT, NO_LIMIT)` |
| `InspectPagesCommand` bounds | `formatBytes(bytes, col, true, budget)` |
| `InspectDictionaryCommand` decoded rows | `formatDecoded(…)` overloads |
| `InspectDictionaryCommand` byte rows | `formatBytes(bytes, col, true, budget)` |
| dive `PagesScreen`, `ColumnIndexScreen`, `ColumnAcrossRowGroupsScreen`, `ColumnChunkDetailScreen` | `formatBytes(…)` (screen-level `—` guards unchanged) |
| `InfoCommand` metadata summary | `Strings.truncateRight(Strings.sanitizeControls(value), 60)` |

`RowValueFormatter`, `IndexValueFormatter`, `RowTable`'s value-rendering half and
`InfoCommand.printable` no longer exist; `RowTable` keeps only table/grid plumbing and
`StreamedTable` keeps its width-based structural wrapping with the shared width source.
