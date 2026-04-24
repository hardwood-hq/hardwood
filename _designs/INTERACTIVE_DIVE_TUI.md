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

## Follow-ups

Items surfaced during phase-1 through phase-4 implementation that were scoped out of
those commits. Check them off as they land.

### Native image

- [x] **Add `hardwood dive --smoke-render` to `NativeBinarySmokeIT`** so the compiled
  native binary is exercised alongside the JVM suite — catches class-init / reflection
  regressions that the surefire test can't.
- [x] **Reflect-config for tamboui.** Adapted from tamboui's `basic-demo` and scoped
  to JNI-only providers: registered `BackendProvider` / `JLineBackendProvider` /
  `JLineBackend` (required by `SafeServiceLoader.load(BackendProvider.class)`), plus
  the `org.jline.terminal.*` and `org.jline.nativ.*` classes the JLine `TerminalBuilder`
  reflects on. Done by hand rather than the tracing agent — that path is still a
  valid option if further reflection regressions surface.
- [x] **Extra `--initialize-at-run-time` entries.** Added `org.jline.terminal.impl.jni`
  (covers the Windows / Linux / macOS / FreeBSD `NativeWin*` / `*NativePty` classes
  that call `Kernel32.init()` at `<clinit>`) alongside the existing
  `org.jline.nativ` entry.

### Bugs

- [x] **Pages modal: show compressed / uncompressed sizes in both
  human-readable and raw-byte form.** Fixed via a `dualSize(long)`
  helper in `PagesScreen` that concatenates `Sizes.format(...) + "  (N B)"`,
  applied to both `Compressed size` and `Uncompressed size` lines of
  the page-header modal. Matches the dual form used on
  `FooterScreen.File size`.
- [x] **Footer screen: dual rendering on every size line.** Added the
  same `dualSize(long)` helper to `FooterScreen` and applied it to
  `File size` (previously the only dual line, now factored through the
  helper), `Column indexes`, `Offset indexes`, `Compressed data`, and
  `Uncompressed data`.
- [x] **Modals don't fully occlude the screen behind them** (bleed-through).
  Fixed in all three modal call sites (`PagesScreen.renderHeaderModal`,
  `DictionaryScreen.renderValueModal`, `HelpOverlay.render`) by rendering
  `dev.tamboui.widgets.Clear.INSTANCE` over the modal Rect before drawing
  the Block+Paragraph. Clear zeroes every cell in its area, so trailing
  cells beyond each line's last character no longer show the underlying
  screen's content. Symptom report was `DATA_PAGE_V22` on the Pages
  page-header modal (the stray `2` being a digit from the Pages table
  behind the modal).
- [x] **Data preview: `ArrayIndexOutOfBoundsException` on nested schemas.**
  Fixed in `DataPreviewScreen.loadPage`: iterate `getRootNode().children()`
  (top-level fields) instead of `model.columnCount()` (leaf count), build
  column header from field names, and use the field count in the render /
  handle paths too (`state.columnNames().size()`). Nested values fall back
  to `Object.toString()` on the returned `PqStruct` / `PqList` / `PqMap` /
  `PqVariant` wrapper — a logical-type-aware formatter is the separate
  "logical-type-aware value formatting" item. Regression test
  (`dataPreviewLoadsNestedSchemaWithoutIndexOutOfBounds`) opens
  `nested_struct_test.parquet` through Data preview and asserts the
  loaded rows are shaped by field count, not leaf count.

### Screen / UX polish

- [x] **Search on Schema and Column index.** Both screens now mirror Dictionary's `/`
  pattern — state carries `filter` + `searching`, DiveApp's input-mode switch covers
  them, and the screens filter their row list case-insensitively. Schema filters
  match leaf field paths and collapse the tree view to a flat match-list; Column
  index matches against each page's formatted min / max.
- ~~"Jump to chunk #N" from the Footer screen.~~ **Dropped.** The original
  design-doc phase-4 line proposed this as a convenience drill, but there is no
  natural selection context on the Footer to drive the jump: the Footer shows
  file-level aggregates (file size, footer trailer offset, index bytes,
  compressed totals), not per-row-group or per-chunk rows. Making it a per-chunk
  table would duplicate the Overview → Row groups → Column chunks path, which
  already exposes first-offset per RG and is the canonical drill into any
  specific chunk. The Footer's value is the aggregate view itself — keeping it
  aggregate-only is the right shape.
- [x] **Data preview test fixture.** Swapped in `column_index_pushdown.parquet`
  (10 000 rows × 2 cols, 1 RG / ~10 pages, has column index) — covers pagination,
  schema navigation, and column-index drills from one fixture. Removed the
  adaptive skip on the PgDown test; added a page-boundary assertion proving
  contiguous rows load.
- [ ] **Expand truncated key/value metadata entries.** The Overview file-facts
  pane truncates each KV value to ~32 chars (`OverviewScreen.trim`) with an
  ellipsis; there is no way to see the full payload.
  **Decided:** inline modal on the Overview file-facts pane. Tab focuses the
  KV list, arrow keys select, Enter opens a modal with the full value —
  mirrors the Dictionary full-value modal pattern. No separate "Metadata"
  drill; most files have a handful of KV entries and a dedicated screen is
  overkill until we see files with 20+ keys.
- [ ] **Decode well-known metadata formats in the full-value view.** Several
  common KV entries are encoded: `ARROW:schema` is base64-encoded Arrow IPC
  (FlatBuffers — starts with `/////` which decodes to the `0xFFFFFFFF` IPC
  continuation marker); `org.apache.spark.sql.parquet.row.metadata` is JSON.
  **Decided:** hand-rolled JSON pretty-printer (~50 lines, no new dep; the
  known payload is shallow enough that a non-validating formatter is fine).
  For `ARROW:schema`, render base64-decoded bytes with a format hint — full
  FlatBuffers decode would require the Arrow library, out of scope. Fallback
  to raw text for unknown keys.
- [ ] **Schema: expand / collapse all shortcut.**
  **Decided:** `E` expands all group nodes, `C` collapses all. Populates /
  clears the `expanded` set in bulk instead of walking the tree node by node.
- ~~Schema: move `[col N]` before the type annotation.~~ **Dropped.**
  The primary task on the Schema screen is "find a column by name, then
  drill in." Names are the handle; `[col N]` is metadata and belongs in the
  secondary-info region (right). The vertical-alignment item below does more
  for scannability than re-ordering would.
- [x] **Row groups list: add CI / OI coverage indicators.** Added two
  columns to `RowGroupsScreen`, rendering `N/M` where M is the chunk
  count and N is how many of that RG's chunks carry ColumnIndex /
  OffsetIndex. Sourced from each `ColumnChunk.columnIndexOffset()` /
  `offsetIndexOffset()`. Makes it possible to spot RGs written without
  page indexes at a glance.
- [ ] **Dedicated Row group detail screen.** Today `Enter` on a row
  group jumps straight to the Column chunks table, bypassing any
  RG-level overview. Add a new screen between Row groups and Column
  chunks: two-pane layout with (left) RG-level facts — row count,
  total byte size, compressed / uncompressed totals and ratio,
  encoding mix across chunks, codec mix, aggregate column-index /
  offset-index / bloom-filter bytes for *this* RG specifically —
  and (right) a drill menu: **Column chunks** (current behaviour)
  and **Indexes for this RG** (new, see below).
  **New screen: Indexes for this RG** — a tabular view with one
  row per column chunk showing `Column | CI offset | CI bytes | OI
  offset | OI bytes | Bloom offset | Bloom bytes`. Makes the
  contiguous per-RG index region visible and addressable. The design
  rationale: Parquet stores page indexes *per row group*, not per
  chunk or globally (see the Parquet layout notes earlier in the
  file — each RG has its own contiguous ColumnIndex and OffsetIndex
  regions following its data pages). The UI currently mirrors the
  *logical* hierarchy but hides the *physical* grouping; an RG
  overview + per-RG indexes screen lets users see the layout they
  actually have on disk. User-visible change: `Enter` on Row groups
  now takes one extra keypress to reach Column chunks (stop at the
  RG detail first) — the trade-off is the new pane plus the
  Indexes drill. Worth flagging in the commit message.
- [x] **Overview drill menu: spell out hints + axis annotation.**
  Applied. Schema hint reads `"16 columns   · browse by column"`; Row
  groups reads `"3 groups    · browse by row group"` (both via
  `Plurals.format`). Footer hint (size) and Data preview hint
  (`Plurals.format(rows, "row", "rows")`) unchanged in shape.
  Two coupled changes to the drill menu hints:
    - **Spell out the counts.** `"16 cols"` → `"16 columns"` /
      `"1 column"`; `"3 RGs"` → `"3 groups"` / `"1 group"`. Uses
      `Plurals.format` from the helper item below. `RGs` is redundant
      anyway since the menu label already reads "Row groups".
    - **Add axis annotations** on `Schema` and `Row groups` so users
      see the two-dimensional-navigation model at the decision point.
      Render a dim `· browse by column` after the Schema hint and
      `· browse by row group` after the Row groups hint. Keeps Parquet
      terminology (Schema / Row groups stay as the labels) while
      addressing the "why are there two ways to get to chunk detail"
      confusion. Footer hint (size) and Data preview hint (`N rows`)
      are unchanged — they aren't axis alternatives.
  Final shape:
  ```
  ▶ Schema              16 columns   · browse by column
    Row groups          3 groups     · browse by row group
    Footer & indexes    158.0 KB
    Data preview        10,000 rows
  ```
- [x] **Pluralization + thousand-separator helper.** Introduced
  `dev.hardwood.cli.dive.internal.Plurals.format(long, String, String)`
  that returns forms like `"1 entry"`, `"131,706 entries"`, `"0 rows"`
  — grouping separator unconditional, zero follows plural. Applied at:
  top bar (`Chrome` — RG count), Overview drill menu (columns, groups,
  rows), Schema title + search bar (leaf columns / leaves), Column
  index title + search bar (pages), Offset index title (pages),
  Dictionary title + search bar (entries), Column-across-RGs title
  (RGs), Chunk-detail drill hint (pages). Didn't touch:
  `RowGroupsScreen` title (shows raw count, not noun), Data preview
  "cols N–M of K" title (already uses `%,d` and "cols" is column-window
  shorthand not a noun count), Chrome `formatRowCount` (retains the
  "12.4 M rows" compact form for the top bar where horizontal space is
  at a premium).
- [ ] **Dictionary modal: gate on actual truncation.** Pressing Enter on
  a dictionary entry currently always opens a modal with the "full"
  value, even when nothing was truncated — a numeric dictionary entry
  like `2` shows the same character in a bigger frame. Compare the
  row-displayed string (the truncated version) to the full value in
  `DictionaryScreen.handle`; if equal, Enter is a no-op. Update the
  keybar hint so `[Enter] full value` shows only on rows where there is
  something extra to reveal. Works uniformly across physical types —
  short BYTE_ARRAY entries also benefit.
- [x] **Logical-type-aware value formatting in Data preview and Dictionary.**
  Implemented in `dev.hardwood.cli.internal.RowValueFormatter`, sibling to
  the existing `IndexValueFormatter`. Two entry points share one dispatch
  core: `format(RowReader, int, SchemaNode)` for Data preview (uses the
  reader's typed accessors — `getTimestamp` / `getDate` / `getDecimal` /
  `getUuid` / `getString` — and falls back to `getValue().toString()` for
  group fields) and `formatDictionaryValue(Object, ColumnSchema)` for
  Dictionary (dispatches on logical type over raw `Integer` / `Long` /
  `Float` / `Double` / `byte[]` drawn from the Dictionary record's
  primitive arrays). TIMESTAMP drops the trailing `Z` when
  `isAdjustedToUTC=false`; DECIMAL routes through `BigDecimal.toPlainString`;
  UTF-8 and UUID handled from raw bytes; unsigned INT variants rendered
  via `Long.toUnsignedString`. 7 unit tests lock the main cases.
  Both screens currently render values ignoring the column's logical type:
    - **Data preview** calls `String.valueOf(reader.getValue(c))` — for an
      INT64 `TimestampType[isAdjustedToUTC=false, unit=MICROS]` that
      produces `1735689600123456` instead of a timestamp.
    - **Dictionary** dispatches on physical type only
      (`IntDictionary.values()`, `LongDictionary.values()`, etc.) and
      returns `Long.toString(rawMicros)` for the same column. Same class
      of problem on a different screen.
  Dispatch on `ColumnSchema.logicalType()`:
    - `TIMESTAMP` → render as `Instant.toString()` (ISO-8601); for
      `isAdjustedToUTC=false` drop the trailing `Z`.
    - `DATE` → `LocalDate.toString()`.
    - `TIME` → `LocalTime.toString()`.
    - `DECIMAL` → `BigDecimal.toPlainString()`.
    - `UUID` → `UUID.toString()`.
    - `STRING` / `ENUM` / `JSON` / `BSON` → the UTF-8 text.
    - `INT_8..64` / `UINT_8..64` → signed / unsigned integer text.
    - Others (LIST / MAP / VARIANT / nested groups) — Data preview only —
      `toString()` on the `PqStruct` / `PqList` / `PqMap` / `PqVariant`
      wrapper, as in the nested-schema bug fix.
  Canonical (machine-reparseable) rendering, not localized pretty-printing —
  these screens are inspection tools.
  **Two entry points** for one shared dispatch core:
    - `RowValueFormatter.format(RowReader reader, int fieldIndex, ColumnSchema col)`
      — Data preview path; uses the typed `RowReader` accessors
      (`getTimestamp`, `getDate`, `getDecimal`, `getUuid`, `getString`).
    - `RowValueFormatter.format(Object rawPrimitive, ColumnSchema col)`
      — Dictionary path; converts raw primitive (`long` micros →
      `Instant`, `byte[]` → UTF-8 string, etc.) before formatting. No
      `RowReader` available because dictionary values come straight out
      of the Dictionary records' primitive arrays.
  Sits as a sibling of the existing `IndexValueFormatter` (which already
  handles the byte[]-stats case for min/max).
- [x] **Data preview: add `Shift+↓` / `Shift+↑` as aliases for PgDn / PgUp.**
  Added a short-circuit in `DataPreviewScreen.handle`: the PgDn branch
  now also fires on `hasShift() && isDown()`, and PgUp mirrors with
  `isUp()`. PgDn/PgUp still work for users with dedicated keys; the
  Shift-arrow alias is the one-hand chord for macOS laptops. Keybar
  updated to list both.
- [ ] **Pages screen: column additions and width tuning.**
  Current columns: `# | Type | First row | Values | Encoding | Comp | Min | Max`.
  Several coordinated tweaks on the same table:
    - **Add Nulls column** sourced from `ColumnIndex.nullCounts()` when
      present (renders `—` otherwise). Useful for scanning for null-heavy
      pages at a glance.
    - **Add Uncompressed column** sourced from
      `PageHeader.uncompressedPageSize()`. Helps spot pages where the
      codec isn't earning its keep; sits next to the existing `Comp`.
    - **Widen `Encoding` column.** Currently `Constraint.Length(12)`,
      which truncates `RLE_DICTIONARY` (14) → `RLE_DICTIONA`, and longer
      names like `DELTA_BINARY_PACKED` (19), `DELTA_LENGTH_BYTE_ARRAY`
      (23), `BYTE_STREAM_SPLIT` (17). Switch to
      `Constraint.Min(14).Max(23)` — natural width for common cases,
      room to grow for the long encodings.
    - **Tighten `Min` / `Max` width.** They are currently
      `Constraint.Fill(1)` each, so they split all remaining horizontal
      space even when values are short or `—`. Use `Min(N).Max(M)` so
      they stay useful for long string values without hogging space
      when they're narrow.
    - **Hide `Min` / `Max` when the chunk has no ColumnIndex** — every
      row would be `—` and the columns are pure noise. Add a
      `(no column index)` hint to the screen header so the absence is
      explicit.
  Landing order: the additions plus the width retune want to coexist
  with the narrow-terminal elide policy; elide priority for this table
  is `Uncompressed → Nulls → Min/Max`.
- [ ] **Column chunk detail: clarify the Pages drill-menu hint when
  OffsetIndex is absent.** Today the menu shows `N pages` when the chunk
  has an OffsetIndex (count read cheaply from the index) and `…` when it
  doesn't. Replace the `…` with `—` to match the "not available from
  cheap metadata" convention used elsewhere (e.g. missing stats).
  Computing the real page count without an OffsetIndex requires walking
  page headers over the whole chunk, which is the same blocking I/O the
  *Async I/O pass* follow-up defers; showing a real count here should
  land together with that work.
- [x] **Long column paths overflow on Column chunk detail and friends.**
  Fixed across three surfaces:
    - **Breadcrumb** (`Chrome.breadcrumbLabel`) now returns `[col N]`
      for `ColumnChunkDetail` and `[col N] across RGs` for
      `ColumnAcrossRowGroups`. Breaks the triple redundancy — the body
      of each of those screens already displays the path.
    - **Block title** — both `ColumnChunkDetailScreen` and
      `ColumnAcrossRowGroupsScreen` now pass the path through a
      `truncateLeft(s, 40)` helper that emits `"…countries.list.element"`
      when the path exceeds 40 chars. Leaf segment stays visible.
    - **Facts pane `Path` row** — a `pathLines` helper on
      `ColumnChunkDetailScreen` renders the key + path inline when the
      path is short, and splits onto two lines (key on its own, value
      indented below) for long paths. Threshold is 27 chars of value
      budget beside the 22-char padded key.
- [x] **Keymap: `g` / `G` for jump-to-top / jump-to-bottom; move
  return-to-Overview to `o`.** `DiveApp`'s global `g` intercept moved
  onto `o` (mnemonic for Overview); `g` / `G` now handled per-screen
  via a shared `Keys.isJumpTop` / `isJumpBottom` helper. Applied on
  the eight navigable-list screens (Schema, RowGroups, ColumnChunks,
  ColumnAcrossRowGroups, Pages, ColumnIndex, OffsetIndex, Dictionary).
  Data preview's `g` / `G` reload at `firstRow=0` / `firstRow=max(0,
  total-pageSize)` respectively, matching PgDn/PgUp's reload-on-move
  semantics. Screens with a search-edit mode (Schema, Dictionary,
  ColumnIndex) correctly fall through to `DiveApp.isTopInInputMode`
  so typed `g`/`G` during filter entry extend the filter instead of
  navigating. Help overlay + docs updated; two regression tests lock
  the semantics for Row groups and Data preview.
- [ ] **Column chunks screen: add Logical type column.** Currently shows
  `Column | Type | Codec | Compressed | Ratio | Dict`. Add a `Logical`
  column right after `Type`, sourced from `model.schema().getColumn(i).logicalType()`
  (render `—` when null), mirroring the Physical / Logical grouping on the
  Column chunk detail screen. Depends on the narrow-terminal elide policy in
  *Rendering / performance* — this pushes the table past 80 cols, so either
  land the elide first (Dict elides first) or ship both in one change.
- [ ] **Schema: vertically align the type / repetition columns.** The current
  renderer concatenates `"  " + typeInfo` after each node name, so the type
  columns shift around because names vary in length.
  **Decided:** pad each visible row's name to the longest name **in the
  currently-visible row set** (visible-max per frame), not the whole schema.
  Adapts to the tree's current expansion state without wasting horizontal
  space on narrow terminals; the "jump" on expand is expected feedback, not
  jitter. Apply the same treatment to the `[col N]` column.

### Rendering / performance

- [ ] **Coalesce per-row-group index reads.** Parquet stores all
  ColumnIndex structures for a row group's chunks in one contiguous
  region, and likewise OffsetIndex. `ParquetModel.columnIndex(rg, col)`
  and `offsetIndex(rg, col)` currently issue one `inputFile.readRange`
  per chunk, so drilling through N chunks of the same RG on an S3-backed
  file is N HTTP round-trips instead of 1. Prefetch the RG's whole
  index span in a single range read when entering the Column chunks
  screen (or whichever screen first touches a chunk in that RG),
  cache the block, and serve per-chunk requests from the cache.
  **Implementation:** reuse whatever coalescing primitive core provides
  — `_designs/COALESCED_OFFSET_INDEX_READS.md` is the relevant core
  work. The CLI is allowed to depend on `dev.hardwood.internal.*`
  classes directly (same pattern as `ParquetModel`'s existing use of
  `ColumnIndexReader` / `DictionaryParser` / `HardwoodContextImpl`),
  so internal-only APIs are fine — no promotion step needed. If core
  has no coalescing primitive at all, implement directly in
  `ParquetModel` using the offsets already on `ColumnChunk`. Distinct
  from the Dictionary and Data preview performance items above — this
  one is about I/O round-trips on remote storage rather than in-memory
  recomputation or sequential skip cost.
- [ ] **Dictionary screen: Up/Down feels sluggish on large dictionaries.**
  - [x] **Filter-index cache.** One-slot memoisation in
    `DictionaryScreen` keyed on `(dict reference, filter)`. Navigation
    keystrokes hit the cache (Dictionary reference is stable because
    `ParquetModel.dictionary` caches by `(rg, col)`); filter edits
    invalidate. Eliminates the per-keystroke double-walk.
  - [ ] **Windowed row materialization.** Even with cached filter
    results, every render materializes a tamboui `Row` for every
    filtered entry and passes the full `List<Row>` to
    `Table.builder().rows(...)`. For ~500 k entries that allocation
    pass still dominates. Fix is only build rows for the currently
    visible slice (plus a small overscan); blocked on checking whether
    tamboui's `Table` exposes a row-provider / lazy API before we
    reimplement viewport math in the screen.
- [x] **Data preview: pagination re-reads from row 0 every time.** Fixed
  by moving the `RowReader` cursor onto `ParquetModel` via a new
  `readPreviewPage(firstRow, pageSize, Consumer<RowReader>)` method
  that reuses a forward-only cursor across calls. Forward moves skip
  ahead without reopening; backward moves (`PgUp`, `g` jump-to-top)
  close and recreate. The cursor is closed on `model.close()` so no
  leak. `G` jump-to-end still pays the forward-read cost over a large
  gap when the cursor is near row 0 — a row-group-level seek primitive
  in core would make it cheap but isn't available yet; noted for a
  future follow-up if the jump feels slow on huge files.
- [ ] **Async I/O pass.** All index / page-header / dictionary reads currently block
  the render thread. **Decided: defer** until we have profiling evidence of a
  real problem. A spinner without async is cosmetic; a full async refactor
  without knowing which screens are slow risks moving the wrong loads. When a
  report or benchmark surfaces a slow flow, move those specific reads via
  `runner.runOnRenderThread` and propagate a loading flag through the screen
  record.
- [ ] **Narrow-terminal column policy for tables.** Column chunks,
  Column-across-RGs, and Data preview each have >6 columns that don't fit at
  80×24. Data preview already has `←/→` column scroll; the others truncate
  implicitly via tamboui.
  **Decided:** elide low-priority columns by default — define a per-screen
  priority list, drop the lowest-priority columns below a width threshold,
  show a `…N more` indicator in the header. Fall back to `←/→` scroll on
  Pages and Column index where every column is load-bearing and the user
  shouldn't have us picking which to hide.

### Data exposure gaps

- [x] **Pages screen: fall back to inline page statistics when ColumnIndex
  is absent.** `PagesScreen` now reads min/max from `DataPageHeader.statistics`
  / `DataPageHeaderV2.statistics` via an `inlineStats(PageHeader)` helper
  that prefers v2 when both are present. Applied in both the list (fallback
  branch of the existing ColumnIndex check) and the page-header modal
  (new "Inline statistics" section showing Min / Max / Nulls when
  available). ColumnIndex remains authoritative when present. Related
  core work: `_designs/INLINE_PAGE_STATS_FALLBACK.md` (filter pushdown).
- [ ] **Bloom filter bytes in the Footer screen.** Blocked on #325 (expose
  `bloomFilterLength()` on `ColumnChunk` in hardwood-core). Once that lands,
  add a `Bloom filters` line to `FooterScreen` alongside the existing column
  / offset index aggregate lines. The earlier placeholder has been removed
  from the screen — the section simply won't mention bloom filters until
  the data is available.

### Documentation

- [ ] **Screenshots in `docs/content/cli.md`.** The current docs use ASCII sketches;
  capture terminal screenshots of the main flows.
  **Decided:** six PNG screen grabs — (1) Overview landing; (2) Schema tree
  expanded and collapsed; (3) Row groups → Column chunks → Column chunk
  detail (three-shot mini-sequence); (4) Pages with the page-header modal
  open; (5) Dictionary with `/` search active and some matches; (6) Data
  preview with columns scrolled right. PNG format (not asciinema — mkdocs
  theming for an embedded player is overkill for six stills; revisit if we
  later want interaction demos).

### Design-doc open questions still unresolved

- [x] **Colour palette decision** (*Open question 1*). Extracted the two
  colours in use (`Color.CYAN` for accents / focused borders, `Color.GRAY`
  for dim secondary text and unfocused borders) into
  `dev.hardwood.cli.dive.internal.Theme.ACCENT` / `Theme.DIM`. Every
  screen now uses the Theme constants; only `Theme.java` references
  `Color.*` directly. No visual change. Future retheme (light-terminal
  variant, `--no-color`) now lives in one file.
- [ ] **Dictionary soft-cap flag** (*Open question 3*). Phase 3 caps the
  chunk-read at a fixed 4 MiB (`ParquetModel.DICTIONARY_READ_CAP_BYTES`)
  with no CLI knob.
  **Decided:** raise the default to 16 MiB (middle ground between "always
  works" and "memory-safe") and add a `--max-dict-bytes N` flag on
  `DiveCommand`. On overflow, the Dictionary screen does **not** silently
  truncate — it renders a `Dictionary is ~N MB. Load anyway? [Enter to
  load, Esc to cancel]` prompt so the memory cost is visible and the user
  opts in inline without restarting.
