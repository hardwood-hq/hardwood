# Design: Interactive `hardwood dive` TUI

**Status: Implemented.** Tracking issue: #324. PRs: #326 (phases 1–4 + review follow-ups).

## Goal

`hardwood dive -f my.parquet` launches a terminal user interface for interactively
exploring a Parquet file's structure. The existing `info` / `schema` / `footer` /
`inspect` / `print` commands each surface one slice; `dive` composes those slices into
a single navigable experience so a user can descend from file-level metadata into row
groups, into column chunks, into pages, into page indexes, and into dictionary
entries — without re-invoking the CLI between each step.

The TUI is built on [TamboUI](https://github.com/tamboui/tamboui), a Java library
modelled on Rust's ratatui / Go's bubbletea. TamboUI is immediate-mode, JLine-backed,
and GraalVM-native-image friendly, which matches Hardwood's existing native CLI
distribution.

## Non-goals

- **Writing / editing.** Dive is read-only.
- **Replacing batch commands.** `info`, `inspect pages`, etc. remain the primary
  surface for scripting, piping, and CI; `dive` is for human exploration.
- **Pretty-printing row data at scale.** A row preview screen is in scope, but paging
  through millions of rows with filters is a separate feature.

## User experience

### Launch

```
hardwood dive -f my.parquet
```

The `-f` flag comes from the existing `FileMixin`; `dive` reuses it unchanged so path
handling (local files, S3 URIs, `~` expansion) stays consistent with sibling commands.
Dive opens the file via `ParquetFileReader.open(InputFile)`, reads the footer eagerly
(so any I/O error surfaces before raw mode), and lands on the **Overview** screen.

### Global chrome

Every screen shares a four-region layout:

```
 hardwood dive │ my.parquet │ 1.4 GB │ 3 RGs │ 12.4 M rows
 Overview › Row groups › RG #1 › Column chunks › l_address.street
┌──────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│                           (active screen body)                               │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
 [↑↓] move  [Enter] open  [Esc] back                       [?] help   [q] quit
```

- **Top bar** — file identity (basename, file size, RG count, row count).
- **Breadcrumb** — the navigation stack from Overview downwards. For per-chunk leaf
  screens (Pages, Column index, Offset index, Dictionary) reached without an upstream
  context-bearing frame (typically Footer → FileIndexes), the leaf label is enriched
  with `(RG #N · column-path)` so the user can still tell which chunk they're in.
- **Body** — the active screen.
- **Keybar** — left side: screen-specific keys, dynamically built from the current
  state so only live keys appear; right side: always-available `[?] help [q] quit`.
  When a modal is open the keybar hides screen keys (the modal carries its own hint).

### Navigation

Dive owns a **navigation stack** of `ScreenState` records. User actions transform the
stack:

| Action                                | Effect                                                   |
| ------------------------------------- | -------------------------------------------------------- |
| `↑` / `↓`                             | Move selection / cursor                                  |
| `PgDn` / `PgUp` (or `Shift-↓ / ↑`)    | Page by viewport stride                                  |
| `g` / `G`                             | Jump to first / last item                                |
| `Enter`                               | Open / view / expand the selected item                   |
| `Esc` / `Backspace`                   | Pop one screen                                           |
| `Tab` / `Shift-Tab`                   | Cycle focus between panes within the current screen      |
| `o`                                   | Pop all the way back to Overview                         |
| `t`                                   | Toggle logical / physical value rendering (where shown)  |
| `e` / `c`                             | Expand-all / collapse-all (Schema; Data preview modal)   |
| `/`                                   | Inline search (Schema, Column index, Dictionary)         |
| `?`                                   | Toggle help overlay                                      |
| `q` / `Ctrl-C`                        | Quit                                                     |

The keybar advertises only the keys that are meaningful in the current state. While a
screen is in text-input mode (e.g. typing into the `/` filter), `q` / `o` / `?` are
passed through as printable characters; only `Ctrl-C` always quits.

`[Enter]` is canonicalised across screens:

- `open` for navigation drills (Overview menu, RowGroups, RowGroupDetail,
  ColumnChunks, ColumnChunkDetail, ColumnAcrossRowGroups, RowGroupIndexes,
  FileIndexes, Footer, Schema leaves).
- `view <noun>` for modal-only displays (Pages → "view page header", Dictionary →
  "view full value", Overview facts pane → "view entry", Column index → "view
  min/max", Data preview → "view record").
- `expand` for in-place expansion (Schema groups).

### Screens

15 `ScreenState` variants plus a help overlay. Drilling never skips levels: a row
group's pages are reached via row group → row-group detail → column chunks → chunk
detail → pages, so the breadcrumb stays legible and `Esc` is predictable.

#### 1. Overview (root)

Two panes. Left: file facts (format version, `created_by`, total uncompressed /
compressed bytes, compression ratio, key/value metadata as a scrollable list). Right:
a four-item drill menu — **Schema** (N columns), **Row groups** (N), **Footer &
indexes** (total bytes), **Data preview** (N rows). `Tab` switches focus.

`Enter` on a key/value entry opens a modal with the formatted value. Two formatters
are wired in: pretty-printed JSON for Spark-style schema entries, and a base64-decoded
hex dump for `ARROW:schema` (a FlatBuffers blob; full decode is tracked as a
follow-up).

#### 2. Schema

Expandable tree of groups + primitive leaves. Each row shows name, type tag (logical
type when present, otherwise physical), repetition, and column index. `→` / `Enter`
on a group expands; `←` collapses; `Enter` on a leaf drills into the
**Column-across-row-groups** screen for that column. `e` expands all groups; `c`
collapses all. `/` filters leaf columns by substring match on the field path; while
the filter is active the tree collapses to a flat list of matches.

#### 3. Row groups

Tabular list of row groups: index, row count, total uncompressed / compressed size,
compression ratio, first-column offset. `Enter` pushes the **Row group detail**
screen.

#### 4. Row group detail

Two panes for one row group. Left: facts (rows, total uncompressed / compressed,
ratio, first-column offset). Right: a drill menu with two items — **Column chunks**
(per-chunk metadata for this RG) and **Indexes** (column / offset index regions in
this RG).

#### 5. Row group indexes

One row per `(chunk × index-kind)` pair: `Column · Index type · Offset · Size`.
Chunks without either index are filtered out; a dim-bordered empty-state banner
appears when the RG has no index regions at all. `Enter` drills into the matching
**Column index** or **Offset index** screen.

#### 6. Column chunks (scoped to a row group)

Tabular list of column chunks within one row group. Columns: column index (`#`),
path, physical type, codec, encodings, compressed / uncompressed, ratio, value
count, null count (from chunk statistics if present), has-dictionary, has-column-
index, has-offset-index. `Enter` pushes the **Column chunk detail** screen.

#### 7. Column chunk detail

Two panes for one `(row-group, column)` pair. Left: facts (path, column index,
physical type, logical type, codec, encodings, all relevant offsets, value count,
nulls, uncompressed / compressed, min, max). Right: a drill menu — **Pages**,
**Column index**, **Offset index**, **Dictionary**. Items for absent structures
appear dimmed and are skipped over by the cursor; selection auto-snaps to the first
enabled item. `t` toggles logical-vs-physical rendering for the facts pane's
min / max.

#### 8. Column-across-row-groups

Reached from Schema. One row per row group for the selected column: RG index, row
count, compressed / uncompressed, ratio, encodings, has-dictionary, value count,
null count, page count, min, max. `Enter` pushes the **Column chunk detail** screen
for that `(RG, column)`. `t` toggles logical types.

#### 9. Pages

Lists data + dictionary pages for one column chunk. Columns: index, type
(`DICTIONARY` / `DATA_PAGE` / `DATA_PAGE_V2`), first-row index (from OffsetIndex),
value count, encoding, compressed / uncompressed, min, max, null count (last three
from ColumnIndex if available, else inline statistics). `Enter` opens a modal with
the full page header (all Thrift fields, including rep / def level byte counts for
V2 pages). Long min / max values are visually truncated with `…` and the modal
carries the full value. `t` toggles logical types when the cursor is on a data page;
the toggle is suppressed on dictionary pages.

#### 10. Column index

Per-page statistics for one chunk: page index, null-page flag (spelled "Null page"
for selected rows), null count, min, max. Boundary order shown above the table.
`Enter` opens a modal with the full untruncated min / max only when at least one of
them is actually truncated in the table cell; otherwise no modal is offered. `/`
filters by substring on the formatted min / max; `t` toggles logical types.

#### 11. Offset index

Page locations for one chunk: page index, absolute file offset, compressed page
size, first row index. No drill.

#### 12. Footer & indexes

Four-section anchored body — File layout (footer offset / length, magic bytes),
Encodings (histogram across the file), Codecs (histogram), Page indexes (aggregate
bytes for column / offset indexes). Three of the anchors (Column indexes, Offset
indexes, Dictionaries) are drillable. `↑` / `↓` cycles only between *enabled*
anchors; disabled sections are skipped. `Enter` pushes **All indexes** for the
selected kind.

#### 13. All column / offset / dictionary indexes (FileIndexes)

File-wide list of every chunk that has the chosen index kind. One row per chunk:
RG index, column path, offset, byte size (and, for dictionaries, dictionary +
data offset + dictionary span). `Enter` drills into the per-chunk **Column index**,
**Offset index**, or **Dictionary** screen. A dim-bordered empty-state banner
appears when no chunks carry the chosen kind.

#### 14. Dictionary

Per-chunk dictionary entries: index + value. Long values are truncated with `…` in
the table and `Enter` opens a modal with the full value. `/` filters by substring;
`t` toggles logical types (also active inside the modal). For dictionaries above
`--max-dict-bytes` (default 16 MiB) the screen renders a confirm prompt instead of
auto-loading; the user opts in with `Enter`.

#### 15. Data preview

Projected rows read via `RowReader`. Per-frame the screen knows the viewport height
and shows that many rows. `←` / `→` scrolls the visible column window with the row
number column pinned; `PgDn` / `PgUp` flips pages forward (and backward via cursor
re-creation). The title shows `rows X-Y of T · cols A-B of C · physical|logical`.
`Enter` opens a row-detail modal with one line per field; in the modal `↑` / `↓`
moves the cursor across fields, `Enter` toggles inline expansion of the focused
field's full value (cursor is hidden when no field is expandable and content fits).
`e` / `c` expand / collapse all fields. `t` toggles logical types both in-table
and in the modal. Long cell values get `…` indicators sized to the actual rendered
column width.

#### Help overlay

Full key reference (rendered as a floating dialog). `?` toggles, `Esc` dismisses.
The overlay is not a screen — it doesn't push state — so `Esc` from inside the
overlay returns to whatever screen was underneath.

### Pane titles

List screens that display a bounded selection range them in the title (e.g.
`Pages 1-10 of 124`, `Dictionary entries 1-28 of 897`, `All column indexes 1-6 of
14`). The range tracks the visible viewport; `Plurals.rangeOf(selection, total,
viewport)` produces the string consistently across screens.

## Architecture

### Module layout

Dive lives in the existing `cli` module:

```
cli/src/main/java/dev/hardwood/cli/
├── command/
│   └── DiveCommand.java          (picocli entry point on HardwoodCommand)
└── dive/
    ├── DiveApp.java              (TuiRunner wiring; dispatchKey / render)
    ├── NavigationStack.java      (push / pop / replaceTop / clearToRoot)
    ├── ScreenState.java          (sealed interface; one record per screen)
    ├── ParquetModel.java         (file handle + lazy + cached metadata access)
    └── internal/                 (screen renderers + handlers + helpers)
        ├── Chrome.java                   (top bar / breadcrumb / keybar layout)
        ├── Keys.java                     (key predicates + viewport observation
        │                                  + Hints builder)
        ├── Plurals.java                  (count + noun, range-of formatting)
        ├── Strings.java                  (padRight / truncateLeft + ellipsis)
        ├── Theme.java                    (colour constants)
        ├── KvMetadataFormatter.java      (JSON pretty + ARROW hex dump)
        ├── HelpOverlay.java
        ├── OverviewScreen.java
        ├── SchemaScreen.java
        ├── RowGroupsScreen.java
        ├── RowGroupDetailScreen.java
        ├── RowGroupIndexesScreen.java
        ├── ColumnChunksScreen.java
        ├── ColumnChunkDetailScreen.java
        ├── ColumnAcrossRowGroupsScreen.java
        ├── PagesScreen.java
        ├── ColumnIndexScreen.java
        ├── OffsetIndexScreen.java
        ├── FooterScreen.java
        ├── FileIndexesScreen.java
        ├── DictionaryScreen.java
        └── DataPreviewScreen.java
```

The `internal` sub-package keeps screens off the public API surface. Modules within
this repository (e.g. `hardwood-cli`) may depend on `dev.hardwood.internal.*`
directly per the project's CLAUDE.md convention; that lets `RowValueFormatter` reach
into `PqVariant` and friends without duplicating logic into a sibling module.

### State / event / render

Each screen contributes three things:

- **State record** — a variant of the sealed `ScreenState` interface. Records are
  immutable; transitions go through `NavigationStack.replaceTop(...)` /
  `push(...)` / `pop()`.
- **Handler** — `static boolean handle(KeyEvent, ParquetModel, NavigationStack)`.
  Returns whether the screen claimed the event. Handlers are pure with respect to
  the model (they read it, never mutate it) and mutate only the stack.
- **Renderer** — `static void render(Buffer, Rect, ParquetModel, ScreenState.X)`.
  Pure draw of the body region.
- **Keybar text** — `static String keybarKeys(ScreenState.X, ParquetModel)`,
  producing a dynamically-built hint string via `Keys.Hints`.

`DiveApp` owns the dispatch:

- `dispatchKey(KeyEvent) → Action.{HANDLED, IGNORED, QUIT}`. Applies the global
  gates (input-mode suppression, `?` toggle, `o` clearToRoot, `q` / Ctrl-C),
  delegates to the active screen's `handle`, and falls back to a generic
  `Esc → pop` if no screen claimed Esc. The runtime loop calls `runner.quit()`
  on `QUIT`; tests drive `dispatchKey` directly without a real `TuiRunner`.
- `render(Frame)` splits the area via `Chrome.split`, computes the keybar text
  before the body so the first frame after a screen change uses the right
  viewport stride (pre-seeded via `Keys.observeViewport`), then renders the
  chrome around the active screen body. The help overlay is drawn last when
  open.

### Data access

`ParquetModel` wraps the open `ParquetFileReader` and caches:

- **Footer + schema** — eager at startup.
- **Per-RG index region** — `RowGroupIndexBuffers` (the core's existing coalescing
  primitive). The first call to `columnIndex(rg, col)` or `offsetIndex(rg, col)`
  prefetches the entire contiguous index region for that RG in one `readRange`;
  later accesses for other columns in the same RG hit the cache. On S3 this
  collapses N HTTP round-trips per RG drill into one.
- **Per-chunk `ColumnIndex` / `OffsetIndex`** — memoised per `(rg, col)`.
- **Per-chunk `Dictionary`** — bounded LRU (capacity 4) keyed on `(rg, col)`.
  Dictionaries can be hundreds of MB; the cap prevents a long session from
  retaining many of them.
- **Per-chunk page headers** — bounded LRU (capacity 8) keyed on `(rg, col)`. Same
  motivation as dictionaries; wide tables have many chunks.
- **Forward-only `RowReader` cursor** — Data preview re-uses one reader and
  remembers its file position; backward moves close and re-create the reader.

All I/O is on the render thread. An async refactor is a possible follow-up, but
the design defers it until profiling shows a screen that genuinely blocks long
enough to feel sluggish.

### Dependency

TamboUI 0.2.0 (`dev.tamboui`, MIT) — four artifacts pinned via `tamboui-bom`:

| Artifact                       | Purpose                                              |
| ------------------------------ | ---------------------------------------------------- |
| `tamboui-core`                 | buffer, layout, style, terminal, event primitives    |
| `tamboui-widgets`              | Block, Paragraph, Table, etc.                        |
| `tamboui-tui`                  | `TuiRunner`, `EventHandler`, `Renderer`, event types |
| `tamboui-jline3-backend` (rt)  | JLine 3-based terminal backend                       |

Native-image: tamboui + JLine reflection / resource needs are encoded under
`cli/src/main/resources/META-INF/native-image/dev.hardwood/hardwood-cli/`. The
hidden `--smoke-render` flag runs one render pass against a 120×40 buffer and
exits 0; `NativeBinarySmokeIT` invokes it on the native binary to prove the
classes survive image build.

## Testing

Three layers:

1. **Layer 1 — state.** `DiveStateTest` drives each screen's `handle(...)`
   directly with synthesised key events against a fixture `ParquetModel`, asserting
   the resulting `NavigationStack`. Pure, fast, no terminal.
2. **Layer 2 — render.** `DiveRenderTest` renders each screen via `RenderHarness`
   to an in-memory `Buffer` and asserts on the captured cells (titles, row
   contents, marker characters, breadcrumb enrichment). Includes a parameterised
   smoke matrix that renders every screen × every fixture so structural
   regressions surface even without an explicit assertion. `PluralsTest` covers
   the boundary semantics of `Plurals.rangeOf` that several screens depend on.
3. **Layer 3 — smoke.** `DiveCommandTest.smokeRender` runs the whole command
   end-to-end against the JVM; `NativeBinarySmokeIT` does the same on the native
   binary.

`DiveAppTest` covers the global key gates in `DiveApp.dispatchKey` (`?` toggle,
Esc closes help, `o` clearToRoot from a deep stack, `q` quits when not in input
mode, Ctrl-C always quits, `q` / `o` are passed through as printable characters
while editing the Dictionary filter).

## CLI integration

`DiveCommand` follows the existing subcommand shape: `@Command(name = "dive", …)`,
implements `Callable<Integer>`, mixes in `FileMixin` and `HelpMixin`. Flags:

| Flag                | Default     | Meaning                                                                     |
| ------------------- | ----------- | --------------------------------------------------------------------------- |
| `-f, --file PATH`   | —           | Input file (via `FileMixin` — S3 URIs supported transparently)              |
| `--max-dict-bytes`  | 16 MiB      | Max chunk size to auto-load on the Dictionary screen; larger needs confirm  |
| `--smoke-render`    | hidden      | Render one frame to a 120×40 buffer and exit 0 (native-image smoke test)    |

Registered on `HardwoodCommand` via the `subcommands` list.

## Open follow-ups

Tracked as GitHub issues; this section is a pointer index, not a backlog.

- [ ] Per-page values inspector — #328
- [ ] `ARROW:schema` FlatBuffers decode (no Arrow dependency) — #64
- [ ] Bloom filter bytes in the Footer screen — blocked on #325
- [ ] Async I/O pass — #331
- [ ] Narrow-terminal column policy for wide tables — #332
- [ ] Screenshots in `docs/content/cli.md` — #333
- [ ] Goto row N in Data preview — #334
- [ ] Predicate-pushdown preview — #335
- [ ] `/` search on Column chunks and Column-across-RGs — #336
- [ ] Diff mode (`--compare a.parquet b.parquet`) — #337
- [ ] Dictionary screen: windowed row materialisation — #338
- [ ] Bundle process-static state into a `DiveSession` — #339
- [ ] Surface `IOException` in `DataPreviewScreen.loadPage` as an error overlay — #342
- [ ] Replace per-screen `withX` factory fan with a builder — #343
- [ ] Document the cache-vs-cursor invariant in `DataPreviewScreen.PAGE_CACHE` — #345
