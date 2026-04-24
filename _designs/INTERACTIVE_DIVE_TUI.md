# Design: Interactive `hardwood dive` TUI

**Status: Proposed.**

**Issue:** _to be created before implementation starts (per `CONTRIBUTING.md`)._

## Goal

Add a new top-level CLI subcommand — `hardwood dive -f my.parquet` — that launches a
terminal user interface for interactively exploring the structure of a Parquet file. The
existing `info` / `schema` / `footer` / `inspect` / `print` commands each surface one slice
of the file; `dive` composes those slices into a single navigable experience so a user can
descend from file-level metadata into row groups, into column chunks, into pages, into
page indexes, and into dictionary entries — without re-invoking the CLI with a different
flag set between each step.

The TUI is built on [TamboUI](https://github.com/tamboui/tamboui), a Java library
modelled on Rust's ratatui / Go's bubbletea. TamboUI is immediate-mode, JLine-backed, and
GraalVM-native-image friendly, which matches Hardwood's existing native CLI distribution.

## Non-goals

- **Writing / editing.** Dive is read-only. No flag toggles, no file mutation.
- **Remote files in phase 1.** Local files only at first. S3 / object-store support comes
  later and reuses `FileMixin`'s existing URI-to-`InputFile` plumbing.
- **Replacing batch commands.** `info`, `inspect pages`, etc. remain the primary surface
  for scripting, piping, and CI. `dive` is for human exploration.
- **Pretty-printing row data at scale.** A row preview screen is in scope, but paging
  through millions of rows with filters is a separate feature tracked elsewhere.

## User experience

### Launch

```
hardwood dive -f my.parquet
```

The `-f` flag comes from the existing `FileMixin`; `dive` reuses it unchanged so path
handling (local, S3 URIs, `~` expansion) stays consistent with sibling commands. If
`-f` is omitted, the command prints usage and exits non-zero — same policy as the other
subcommands.

On startup, dive opens the file via `ParquetFileReader.open(InputFile)`, reads the
footer eagerly (so any I/O error surfaces immediately, before the terminal switches to
raw mode), and lands on the **Overview** screen.

### Global chrome

Every screen shares a three-region layout:

```
┌─ hardwood dive ── my.parquet ── 1.4 GB ── 3 row groups ── 12.4 M rows ───────┐
│ Overview › Row groups › RG #1 › Column chunks › c_name                       │  ← breadcrumb
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│                           (screen body)                                      │
│                                                                              │
├──────────────────────────────────────────────────────────────────────────────┤
│ [↑↓] move  [Enter] drill  [Esc] back  [Tab] next pane  [?] help  [q] quit    │  ← keybar
└──────────────────────────────────────────────────────────────────────────────┘
```

- **Top bar** — persistent file identity (path, size, row-group count, row count).
- **Breadcrumb** — the current navigation stack from Overview downwards. Clicking
  `Esc` pops one crumb; `g` jumps back to Overview.
- **Body** — the active screen (see below).
- **Keybar** — screen-specific keys on the left, always-available keys on the right.
  The full key reference lives on the help overlay (`?`).

### Navigation model

Dive maintains a **navigation stack** of screen states. User actions transform the
stack:

| Action                | Effect                                                   |
| --------------------- | -------------------------------------------------------- |
| `Enter` on an item    | Push the drill-down screen for that item                 |
| `Esc` / `Backspace`   | Pop the current screen                                   |
| `Tab` / `Shift+Tab`   | Cycle focus between panes *within* the current screen    |
| `g`                   | Pop all screens back to Overview                         |
| `?`                   | Open help overlay (not a screen — does not push state)   |
| `q` / `Ctrl-C`        | Exit the TUI cleanly                                     |
| `/`                   | Open search within the current screen (where applicable) |

Drilling never skips levels: a row group's pages are reached via row group → column
chunk → pages. This keeps the stack legible in the breadcrumb and the back button
predictable.

### Screens

#### 1. Overview (root)

Purpose: single-glance summary of what the file contains, with four pickable entry
points into deeper screens.

Layout: two columns.

- **Left pane — file facts.** Format version, `created_by`, codec mix, total
  compressed / uncompressed size, compression ratio, key-value metadata (scrollable if
  long). Data source: `ParquetFileReader.getFileMetaData()` aggregates — same fields
  `InfoCommand` already computes.
- **Right pane — navigation menu.** Four selectable items, each annotated with a
  count hinting at what's inside. This is the authoritative home for navigable
  counts — file-facts deliberately omits them so the same number doesn't appear in
  two places:
  1. **Schema** (N columns) — drills into the Schema screen
  2. **Row groups** (N) — drills into the Row Groups screen
  3. **Footer & indexes** (total bytes) — drills into the Footer screen
  4. **Data preview** (N rows) — drills into the Data Preview screen. Named to
     contrast with "Row groups" (structure) — this axis is actual row data read via
     `RowReader`.

`Tab` switches focus between the two panes; `Enter` in the right pane drills. The left
pane is scrollable but has no drill targets.

```
┌─ hardwood dive ─ /data/lineitem.parquet ─ 1.4 GB ─ 3 RGs ─ 12.4 M rows ──┐
│ Overview                                                                 │
├──────────────────────────────────────────────────────────────────────────┤
│ ┌─ File facts ──────────────────┐  ┌─ Drill into ───────────────[·]─┐   │
│ │ Format version   2.9.0        │  │ ▶ Schema              16 cols  │   │
│ │ Created by       parquet-mr   │  │   Row groups           3 RGs   │   │
│ │ Codec            ZSTD         │  │   Footer & indexes  4 KB+120K  │   │
│ │ Uncompressed     4.8 GB       │  │   Data preview    12 400 000 r │   │
│ │ Compressed       1.4 GB       │  │                                │   │
│ │ Ratio            3.4×         │  │                                │   │
│ │                               │  │                                │   │
│ │ key/value meta (2)            │  │                                │   │
│ │  writer.model.name  spark     │  │                                │   │
│ │  spark.sql.schema   {"type":… │  │                                │   │
│ └───────────────────────────────┘  └────────────────────────────────┘   │
├──────────────────────────────────────────────────────────────────────────┤
│ [Tab] pane  [↑↓] move  [Enter] drill           [?] help       [q] quit   │
└──────────────────────────────────────────────────────────────────────────┘
```

#### 2. Schema screen

Purpose: tree-style navigation of the Parquet logical schema.

Layout: single pane, expandable tree. Each node shows name, primitive / group, logical
type, repetition, and (for leaf columns) physical type + column index. Data source:
`FileSchema.getRootNode()` with recursive `SchemaNode.children()` traversal — exactly
what `SchemaCommand` walks today.

Keys:

- `→` / `Enter` on a group node — expand.
- `←` on a group node — collapse.
- `Enter` on a leaf column — push the **Column-across-row-groups** screen (#6) for that
  column. This is a cross-cut: "I want to see how column `c_name` looks across every row
  group." It's the most common exploratory path and deserves a direct jump from schema.

No search in phase 1; added in phase 4.

```
┌─ hardwood dive ─ /data/lineitem.parquet ─ 1.4 GB ─ 3 RGs ─ 12.4 M rows ──┐
│ Overview › Schema                                                        │
├──────────────────────────────────────────────────────────────────────────┤
│ root (message)                                                           │
│ ├─ l_orderkey         INT64        REQUIRED                       [col 0]│
│ ├─ l_partkey          INT64        REQUIRED                       [col 1]│
│ ├─ l_suppkey          INT64        REQUIRED                       [col 2]│
│ ├─▶ l_linenumber      INT32        REQUIRED                       [col 3]│
│ ├─ l_quantity         DECIMAL(12,2) OPTIONAL                      [col 4]│
│ ├─ l_extendedprice    DECIMAL(12,2) OPTIONAL                      [col 5]│
│ ├─ l_shipdate         DATE         OPTIONAL                       [col 8]│
│ ├─ ▼ l_address (group)                                                   │
│ │   ├─ street         STRING       OPTIONAL                       [col11]│
│ │   ├─ city           STRING       OPTIONAL                       [col12]│
│ │   └─ zip            STRING       OPTIONAL                       [col13]│
│ ├─ ▶ l_tags (LIST)                            OPTIONAL                   │
│ └─ l_comment          STRING       OPTIONAL                       [col15]│
├──────────────────────────────────────────────────────────────────────────┤
│ [↑↓] move  [→/Enter] expand · drill column  [←] collapse  [Esc] back     │
└──────────────────────────────────────────────────────────────────────────┘
```

#### 3. Row groups screen

Purpose: tabular list of row groups with aggregate metrics.

Layout: single table. Columns: index, row count, total byte size, total compressed size,
compression ratio, first-column offset. Data source: iterate `FileMetaData.rowGroups()`
and sum per-chunk sizes — a subset of what `InspectRowGroupsCommand` already computes.

`Enter` on a row pushes the **Column chunks** screen (#4) scoped to that row group.

```
┌─ hardwood dive ─ /data/lineitem.parquet ─ 1.4 GB ─ 3 RGs ─ 12.4 M rows ──┐
│ Overview › Row groups                                                    │
├──────────────────────────────────────────────────────────────────────────┤
│ #   Rows         Uncompressed   Compressed    Ratio   First offset       │
│ ─── ───────────  ─────────────  ────────────  ──────  ─────────────────  │
│   0    4 200 000        1.6 GB       480 MB    3.4×   4                  │
│ ▶ 1    4 100 000        1.6 GB       472 MB    3.5×   503 316 480        │
│   2    4 100 000        1.6 GB       478 MB    3.4×   1 001 127 936      │
│                                                                          │
│ (3 row groups)                                                           │
├──────────────────────────────────────────────────────────────────────────┤
│ [↑↓] move  [Enter] drill into chunks  [Esc] back                         │
└──────────────────────────────────────────────────────────────────────────┘
```

#### 4. Column chunks screen (scoped to a row group)

Purpose: list every column chunk inside one row group.

Layout: single table. Columns: column path, physical type, codec, encodings,
compressed size, uncompressed size, compression ratio, value count, null count (from
chunk statistics if present), has-dictionary flag, has-column-index flag,
has-offset-index flag. Data source: `RowGroup.columns()` → `ColumnChunk.metaData()`,
same access path as `InspectRowGroupsCommand`.

`Enter` on a chunk pushes the **Column chunk detail** screen (#5).

```
┌─ hardwood dive ─ /data/lineitem.parquet ─ 1.4 GB ─ 3 RGs ─ 12.4 M rows ──┐
│ Overview › Row groups › RG #1 (4.1 M rows) › Column chunks               │
├──────────────────────────────────────────────────────────────────────────┤
│ Column              Type          Codec  Compressed  Ratio  Dict  CI  OI │
│ ─────────────────── ───────────── ────── ──────────  ─────  ────  ── ── │
│ l_orderkey          INT64         ZSTD      28.1 MB  4.2×   yes   ✓  ✓  │
│ l_partkey           INT64         ZSTD      29.8 MB  4.0×   yes   ✓  ✓  │
│ l_suppkey           INT64         ZSTD      17.2 MB  5.1×   yes   ✓  ✓  │
│ l_linenumber        INT32         ZSTD       3.1 MB  8.3×   yes   ✓  ✓  │
│ l_quantity          DECIMAL(12,2) ZSTD       6.4 MB  6.0×   yes   ✓  ✓  │
│ l_extendedprice     DECIMAL(12,2) ZSTD      41.3 MB  2.1×   no    ✓  ✓  │
│ l_shipdate          DATE          ZSTD       5.9 MB  6.2×   yes   ✓  ✓  │
│ ▶ l_address.street  STRING        ZSTD      88.4 MB  2.8×   no    ✓  ✓  │
│ l_address.city      STRING        ZSTD      12.6 MB  3.1×   yes   ✓  ✓  │
│ l_tags.list.element STRING        ZSTD      19.2 MB  3.3×   yes   ✓  ✓  │
│ l_comment           STRING        ZSTD     208.5 MB  1.8×   no    ✓  ✓  │
├──────────────────────────────────────────────────────────────────────────┤
│ [↑↓] move  [Enter] inspect chunk  [/] filter  [Esc] back                 │
└──────────────────────────────────────────────────────────────────────────┘
```

#### 5. Column chunk detail screen

Purpose: deep dive on one `(row_group, column)` intersection. This is the hub from
which dictionary, pages, column index, and offset index are reached.

Layout: two panes.

- **Left pane — facts.** All fields from `ColumnMetaData`: physical type, codec,
  encodings list, data-page offset, dictionary-page offset, index page offset, bloom
  filter offset, compressed / uncompressed size, value count, null count, min / max
  from chunk statistics. Offsets displayed as absolute byte offsets into the file.
- **Right pane — drill menu.** Up to four items, each visible only if the chunk has
  the corresponding structure:
  1. **Pages** — push **Pages** screen (#7)
  2. **Column index** — push **Column index** screen (#8)
  3. **Offset index** — push **Offset index** screen (#9)
  4. **Dictionary** — push **Dictionary** screen (#10)

When a structure is absent, the menu item is rendered dimmed and is not selectable,
with a short tooltip (`no column index in this chunk`).

```
┌─ hardwood dive ─ /data/lineitem.parquet ─ 1.4 GB ─ 3 RGs ─ 12.4 M rows ──┐
│ … › RG #1 › Column chunks › l_address.street                             │
├──────────────────────────────────────────────────────────────────────────┤
│ ┌─ Chunk metadata ────────────────┐  ┌─ Drill into ───────────────[·]─┐ │
│ │ Path          l_address.street  │  │   Pages               64 pages │ │
│ │ Column idx    11                │  │ ▶ Column index         present │ │
│ │ Physical      BYTE_ARRAY        │  │   Offset index         present │ │
│ │ Logical       STRING            │  │   Dictionary              n/a  │ │
│ │ Codec         ZSTD              │  │                                │ │
│ │ Encodings     PLAIN, RLE        │  │                                │ │
│ │                                 │  │                                │ │
│ │ Data offset      512 318 112    │  │                                │ │
│ │ Dict offset      —              │  │                                │ │
│ │ Index offset     —              │  │                                │ │
│ │ Bloom offset     —              │  │                                │ │
│ │                                 │  │                                │ │
│ │ Values         4 100 000        │  │                                │ │
│ │ Nulls             12 431        │  │                                │ │
│ │ Uncompressed    247.1 MB        │  │                                │ │
│ │ Compressed       88.4 MB        │  │                                │ │
│ │ Min            "10 Abbey Rd"    │  │                                │ │
│ │ Max            "ZZ Top Blvd 9"  │  │                                │ │
│ └─────────────────────────────────┘  └────────────────────────────────┘ │
├──────────────────────────────────────────────────────────────────────────┤
│ [Tab] pane  [↑↓] move  [Enter] drill  [Esc] back                         │
└──────────────────────────────────────────────────────────────────────────┘
```

#### 6. Column-across-row-groups screen

Purpose: show one column's behaviour across every row group. Reached from the Schema
screen.

Layout: single table, one row per row group. Columns: RG index, row count, compressed
size, uncompressed size, compression ratio, encodings, has-dictionary, has-column-index,
value count, null count, min, max.

`Enter` on a row pushes the **Column chunk detail** screen (#5) for that (RG, column).

#### 7. Pages screen

Purpose: list data pages and dictionary pages inside one column chunk.

Layout: single table. Columns: page index, page type (DICTIONARY / DATA_PAGE /
DATA_PAGE_V2), first-row index (from OffsetIndex), value count, encoding, compressed
size, uncompressed size, min, max, null count (last three from ColumnIndex if
available, else inline statistics). Data source: `PageHeaderReader` over the chunk
byte range + optional `ColumnIndexReader` + optional `OffsetIndexReader` — same stack
as `InspectPagesCommand`.

`Enter` on a page opens a modal with the full page header (all Thrift fields, including
the repetition / definition level byte counts for V2 pages). No further drill.

```
┌─ hardwood dive ─ /data/lineitem.parquet ─ 1.4 GB ─ 3 RGs ─ 12.4 M rows ──┐
│ … › l_address.street › Pages                                             │
├──────────────────────────────────────────────────────────────────────────┤
│  #  Type         FirstRow  Values   Encoding  Comp   Min         Max     │
│ ─── ──────────── ─────── ───────── ───────── ─────── ─────────── ─────── │
│   0 DATA_PAGE_V2       0    64 512  PLAIN    1.4 MB  "10 Abbey…" "Alhams"│
│   1 DATA_PAGE_V2   64512    64 512  PLAIN    1.4 MB  "Alham Rd"  "Balti…"│
│   2 DATA_PAGE_V2  129024    64 512  PLAIN    1.4 MB  "Baltimo…"  "Beaco…"│
│ ▶ 3 DATA_PAGE_V2  193536    64 512  PLAIN    1.4 MB  "Beacon…"   "Blake…"│
│   4 DATA_PAGE_V2  258048    64 512  PLAIN    1.4 MB  "Blake…"    "Brook…"│
│   5 DATA_PAGE_V2  322560    64 512  PLAIN    1.4 MB  "Brookly…"  "Camde…"│
│ ⋮                                                                        │
│  63 DATA_PAGE_V2 4063712    36 288  PLAIN    0.8 MB  "Zaragoz…"  "ZZ To…"│
│                                                                          │
│ (64 pages · min/max from ColumnIndex)                                    │
├──────────────────────────────────────────────────────────────────────────┤
│ [↑↓] move  [Enter] page header details  [/] filter  [Esc] back           │
└──────────────────────────────────────────────────────────────────────────┘
```

`Enter` on a page opens a modal:

```
              ┌─ Page #3 header ─────────────────────────────┐
              │ Type                      DATA_PAGE_V2       │
              │ Compressed size           1 463 201          │
              │ Uncompressed size         3 821 504          │
              │ Num values                64 512             │
              │ Num nulls                 192                │
              │ Num rows                  64 512             │
              │ Encoding                  PLAIN              │
              │ Def level encoding        RLE (byte length 8)│
              │ Rep level encoding        RLE (byte length 0)│
              │ Is compressed             true               │
              │ Crc                       0xA41F0B92         │
              │                                              │
              │                              [Esc] close     │
              └──────────────────────────────────────────────┘
```

#### 8. Column index screen

Purpose: per-page statistics as a table.

Layout: single table. One row per page, columns: page index, null_pages (yes/no),
min, max, null_count, repetition_count, definition_count (V2 fields when present),
boundary_order (once, in the header). Data source: `ColumnIndex` via `ColumnIndexReader`.

`/` searches for rows whose min or max match a literal — useful for "does any page
touch this value?" questions.

```
┌─ hardwood dive ─ /data/lineitem.parquet ─ 1.4 GB ─ 3 RGs ─ 12.4 M rows ──┐
│ … › l_address.street › Column index                                      │
├──────────────────────────────────────────────────────────────────────────┤
│ Boundary order: ASCENDING      Null pages present: no                    │
├──────────────────────────────────────────────────────────────────────────┤
│  #  NullPg  Nulls    Min                       Max                       │
│ ─── ─────── ───────  ────────────────────────  ──────────────────────── │
│   0  no          3   "10 Abbey Rd"             "Alham St 214"            │
│   1  no          7   "Alham St 215"            "Baltimore Ave 8"         │
│ ▶ 2  no          1   "Baltimore Ave 9"         "Beacon Hill 4"           │
│   3  no          0   "Beacon Hill 5"           "Blake Row 77"            │
│   4  no          2   "Blake Row 78"            "Brooklyn Bridge 3"       │
│ ⋮                                                                        │
│  63  no         12   "Zaragoza Plz 4"          "ZZ Top Blvd 9"           │
├──────────────────────────────────────────────────────────────────────────┤
│ [↑↓] move  [/] search min/max  [Esc] back                                │
└──────────────────────────────────────────────────────────────────────────┘
```

#### 9. Offset index screen

Purpose: table of page locations. One row per page: page index, offset, compressed size,
first row index. Data source: `OffsetIndex` via `OffsetIndexReader`. No drill; `Enter`
is a no-op.

#### 10. Dictionary screen

Purpose: show dictionary entries for one column chunk.

Layout: single table with index + value columns. The value column renders per physical
type using the existing `IndexValueFormatter`. For byte-array dictionaries, long values
are truncated with an ellipsis; `Enter` on a row opens a modal with the full untruncated
value (and, for UTF-8, a preview of the decoded string).

Data source: `DictionaryParser.parse(...)` — same call as `InspectDictionaryCommand`.

`/` filters the list by a literal substring.

#### 11. Footer & indexes screen

Purpose: raw file layout. Shows file size, footer offset, footer length, magic-byte
positions, aggregate bytes occupied by column indexes, aggregate bytes occupied by
offset indexes, aggregate bytes occupied by bloom filters. Data source: the manual
reading `FooterCommand` already performs, plus offset-index / column-index offsets
gathered from every `ColumnChunk`.

No drill in phase 1 (the per-chunk indexes are reachable through the Column chunk
detail screen). Phase 2 can add a "jump to chunk N" action.

#### 12. Data preview screen

Purpose: preview actual row data. Reached from Overview › Data preview. Named for
contrast with "Row groups" (a structural axis) — this one is values read through
`RowReader`.

Layout: single table. First `N` rows (default 100, configurable via `--rows`), columns
projected from the schema. Truncation rules identical to `PrintCommand`.

Keys: `PgDn` / `PgUp` paginate forward / backward (forward only in phase 1 — no seek
back in a `RowReader`). Phase 2 can add column filtering and a row-index jump.

```
┌─ hardwood dive ─ /data/lineitem.parquet ─ 1.4 GB ─ 3 RGs ─ 12.4 M rows ──┐
│ Overview › Data preview (rows 1–100 of 12 400 000)                       │
├──────────────────────────────────────────────────────────────────────────┤
│   # │ l_orderkey │ l_partkey │ l_linenumber │ l_quantity │ l_shipdate    │
│ ─── ┼ ────────── ┼ ───────── ┼ ──────────── ┼ ────────── ┼ ───────────── │
│   0 │          1 │    155190 │            1 │      17.00 │  1996-03-13   │
│   1 │          1 │     67310 │            2 │      36.00 │  1996-04-12   │
│   2 │          1 │     63700 │            3 │       8.00 │  1996-01-29   │
│ ▶ 3 │          1 │      2132 │            4 │      28.00 │  1996-04-21   │
│   4 │          1 │     24027 │            5 │      24.00 │  1996-03-30   │
│ ⋮                                                                        │
│  99 │          7 │     87654 │            6 │      12.00 │  1996-09-14   │
│                                                                          │
│ Columns 1–5 of 16  (→ scrolls right)                                     │
├──────────────────────────────────────────────────────────────────────────┤
│ [↑↓] move  [←→] columns  [PgDn/PgUp] page  [Esc] back                    │
└──────────────────────────────────────────────────────────────────────────┘
```

#### 13. Help overlay

Full key reference, grouped by screen. Rendered as a dialog; `?` or `Esc` dismisses.

### Flow example

1. `hardwood dive -f sales.parquet` — lands on **Overview**.
2. `Tab` → right pane, `↓` to "Row groups", `Enter` — **Row groups screen**.
3. `↓↓` to RG #2, `Enter` — **Column chunks for RG #2**.
4. `↓` to `line_item.product_name`, `Enter` — **Column chunk detail**.
5. `Tab` → drill menu, `↓` to "Column index", `Enter` — **Column index screen**.
6. `/` → search `widget`, `Enter` — first matching row highlighted.
7. `Esc Esc Esc` — back to Overview, no state lost along the way.

## Architecture

### Module organisation

Keep dive in the existing `cli` module, adding one new package:

```
cli/src/main/java/dev/hardwood/cli/
├── command/
│   ├── DiveCommand.java          (new — picocli entry point, registered on HardwoodCommand)
│   └── …
└── dive/                          (new package — TUI implementation)
    ├── DiveApp.java               (wires TuiRunner, owns file handle, dispatches events / renders)
    ├── NavigationStack.java       (stack of ScreenState; push/pop/peek)
    ├── ScreenState.java           (sealed interface — one record per screen)
    ├── ParquetModel.java          (read-through cache: metadata, lazily-loaded indexes)
    ├── internal/                  (non-public screen renderers + event handlers)
    │   ├── OverviewScreen.java
    │   ├── SchemaScreen.java
    │   ├── RowGroupsScreen.java
    │   ├── ColumnChunksScreen.java
    │   ├── ColumnChunkDetailScreen.java
    │   ├── ColumnAcrossRowGroupsScreen.java
    │   ├── PagesScreen.java
    │   ├── ColumnIndexScreen.java
    │   ├── OffsetIndexScreen.java
    │   ├── DictionaryScreen.java
    │   ├── FooterScreen.java
    │   ├── DataPreviewScreen.java
    │   └── HelpOverlay.java
    └── internal/widget/           (reusable chrome — breadcrumb, keybar, table with scroll)
```

Rationale for *not* spinning up a new Maven module:

- `dive` is a CLI command and has to be wired into `HardwoodCommand`'s
  `@Command(subcommands = …)` — that forces picocli visibility.
- The new tamboui dependency has no transitive impact on the core reader, so there's no
  API-surface reason to isolate it.
- Native-image reachability metadata stays in one place (`cli/`).

The `internal` sub-packages keep screens off the public API surface, matching Hardwood's
existing `internal` convention (`FileMixin` is public, the screen renderers are not).

### Model / view / event

Immediate-mode TamboUI renders the entire frame from current state on each tick. That
fits a Redux-ish shape:

- **State** — `ParquetModel` (file-scoped, built once at startup) + `NavigationStack`
  (per-frame mutable).
- **Event router** — a single `TuiEventHandler` that pattern-matches on the active
  `ScreenState`'s type and delegates to the matching screen's `handle(event, model,
  stack)` method. Each screen's handler may: mutate the top of the stack (move
  selection, toggle expansion), push a new `ScreenState`, pop, or signal quit.
- **Renderer** — `DiveApp`'s render callback dispatches on the active `ScreenState`
  type, renders chrome (breadcrumb + keybar) around the screen body, and overlays the
  help dialog if open.

Each screen is thus two files of logic — a record for its state and a class for its
render + handle methods — plus chrome provided by the host.

### Data access

`ParquetModel` wraps the open `ParquetFileReader` and caches:

- Footer metadata + schema: loaded eagerly at startup.
- Per-chunk `OffsetIndex` / `ColumnIndex`: loaded lazily on first navigation into a
  screen that needs them, then memoised for the session. These reads are small but
  chatty (one per chunk), so caching matters — a 200-column, 10-row-group file has
  2 000 potential index reads.
- Per-chunk `Dictionary`: loaded lazily; typically large, so we cache with a small
  LRU keyed on `(rowGroupIndex, columnIndex)` rather than indefinitely.
- Per-chunk page headers: loaded lazily when the Pages screen opens for a chunk; not
  cached across sessions of that screen (cheap to re-read, memory-sensitive).

All I/O happens on the main thread in phase 1. A spinner widget is shown while a load
is in flight (TamboUI's frame model makes this a single boolean on the screen state).
Async loading via a worker thread can come later if profiling shows the main thread
blocks long enough to feel sluggish.

### Dependency

TamboUI 0.2.0 is on Maven Central under `dev.tamboui`, MIT-licensed. The module set
we depend on:

| Artifact                       | Purpose                                               |
| ------------------------------ | ----------------------------------------------------- |
| `tamboui-core`                 | buffer, layout, style, terminal, event primitives     |
| `tamboui-widgets`              | Block, Paragraph, Table, ListWidget, etc.             |
| `tamboui-tui`                  | `TuiRunner`, `EventHandler`, `Renderer`, event types  |
| `tamboui-jline3-backend` (rt)  | JLine 3-based terminal backend                        |

The `tamboui-bom` POM is imported into `hardwood-bom` so the four artifacts share a
single pinned version. `cli/pom.xml` then declares the compile-scope artifacts and the
runtime backend:

```xml
<dependency>
  <groupId>dev.tamboui</groupId>
  <artifactId>tamboui-core</artifactId>
</dependency>
<dependency>
  <groupId>dev.tamboui</groupId>
  <artifactId>tamboui-widgets</artifactId>
</dependency>
<dependency>
  <groupId>dev.tamboui</groupId>
  <artifactId>tamboui-tui</artifactId>
</dependency>
<dependency>
  <groupId>dev.tamboui</groupId>
  <artifactId>tamboui-jline3-backend</artifactId>
  <scope>runtime</scope>
</dependency>
```

Native-image compatibility: TamboUI advertises GraalVM support. The existing
`cli/src/main/resources/META-INF/native-image/` reachability metadata will need
augmentation for tamboui's reflection entries (JLine terminal detection uses
reflection). Covered by the existing `NativeBinarySmokeIT` — extending it with a
`dive --smoke-render` mode (one frame, then exit) proves the TUI classes survive
native-image.

### Testing strategy

Three layers:

1. **Pure-state tests** — every screen's `handle(event, model, stack)` is a pure
   function of input state and event, returning the next state. Unit-tested with no
   terminal, no file I/O — just fixture `ParquetModel`s built from existing test
   Parquet files under `core/src/test/resources/`. This is where most of the coverage
   lives.
2. **Render snapshot tests** — for each screen, render once against a fixture
   `ParquetModel`, capture the frame buffer as a string, assert against a golden file.
   TamboUI exposes a headless backend for exactly this; if the snapshots drift, the
   failing diff is the review artefact.
3. **Smoke tests** — the existing `NativeBinarySmokeIT` pattern, extended with a
   hidden `--smoke-render` flag that runs one render pass and exits 0. Proves native
   image reachability and that the command wires up end-to-end.

UI flows (drill-down chains) are tested at layer 1 by sequencing events against the
state machine.

### Quarkus / picocli integration

`DiveCommand` follows the existing subcommand shape (`FooterCommand`,
`InspectPagesCommand`): `@CommandLine.Command(name = "dive", …)`, implements
`Callable<Integer>`, mixes in `FileMixin` and `HelpMixin`. One new option in phase 1:

| Flag              | Default | Meaning                                                      |
| ----------------- | ------- | ------------------------------------------------------------ |
| `-f, --file PATH` | —       | Input file (via `FileMixin` — S3 URIs supported transparently) |
| `--rows N`        | 100     | Initial page size for the Data preview screen                |
| `--no-color`      | false   | Disable ANSI colour (forwards to tamboui)                    |

Registered on `HardwoodCommand` by adding `DiveCommand.class` to the `subcommands` list.

## Phasing

Landing this in one PR would produce a ~3 000-line change. Split into four:

### Phase 1 — Skeleton + top-level navigation

- `DiveCommand` registered on `HardwoodCommand`.
- `DiveApp` + event router + navigation stack.
- Screens: Overview (#1), Schema (#2), Row groups (#3), Column chunks (#4), Column
  chunk detail (#5), Help overlay (#13).
- Chrome: top bar, breadcrumb, keybar.
- Tests: layer 1 (state) + layer 3 (smoke).

Ships a usable product: user can open a file, see structure, descend into one chunk's
metadata.

### Phase 2 — Indexes

- Pages (#7), Column index (#8), Offset index (#9), Footer (#11), Column-across-row-
  groups (#6) screens.
- `ParquetModel` lazy-load paths for OffsetIndex / ColumnIndex.
- Layer 2 render snapshot tests stand up now that the screen zoo is bigger.

### Phase 3 — Data

- Dictionary (#10) and Data preview (#12) screens.
- Dictionary LRU cache.
- `PgDn`/`PgUp` pagination in Data preview via repeated `RowReader` creation (honest
  about the one-way traversal limitation — no false "back" affordance).

### Phase 4 — Polish

- `/` search on Schema, Dictionary, Column index.
- Column-chunk detail "jump to chunk N" from Footer screen.
- Profiling pass; async load off the main thread if needed.
- Documentation: `docs/content/` page under *CLI* walking through the dive flow with
  screenshots.

Each phase produces a merge-worthy PR with tests and docs. ROADMAP.md is updated at
the end of each phase to move entries from `[ ]` to `[x]`.

## Open questions

1. **Colour theme.** Does Hardwood want a colour convention shared with the existing
   `StreamedTable` / `RowTable` output, or does `dive` define its own palette? Lean
   toward the second: the non-TUI tables are optimised for pipe-to-`less` readability;
   dive can use richer colours because it owns the terminal.
2. **Window-resize behaviour.** TamboUI handles resize events; screens should clip
   gracefully. Tables with many columns in narrow terminals need a policy — elide
   columns, horizontal scroll, or force-wrap. Propose: horizontal scroll with `←` /
   `→` arrows in table screens, a column priority list chosen per screen.
3. **Very large files.** Eager footer load is fine even for huge files (footers are
   small). Dictionary loads for wide `BYTE_ARRAY` columns can be hundreds of MB —
   should we cap the dictionary-screen load and show a warning above a threshold?
   Propose: 64 MB soft cap, configurable via `--max-dict-bytes`.

These are resolvable during phase 1 review; noting them so they aren't forgotten.
