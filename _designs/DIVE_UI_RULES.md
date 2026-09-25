# Dive UI rules

The rules every pane of the `hardwood dive` TUI follows: how content is styled, how the navigation keys and the `▶` marker behave, and how list-shaped screens bound per-keystroke work to the viewport. A new screen, pane or modal is reviewed against these three sections. The screen stack, the state/handler/render split, `ParquetModel` and the data preview's `PreviewWindow` are in [DIVE_ARCHITECTURE.md](DIVE_ARCHITECTURE.md); how values and figures are spelled is in [CLI_VALUE_RENDERING.md](CLI_VALUE_RENDERING.md). The user-facing keys are listed in [the `dive` section of the CLI reference](../docs/content/reference/cli.md#interactive-exploration-dive).

All classes named here live in `dev.hardwood.cli.dive.internal`.

## Visual hierarchy

Dive has one theme. There is no per-terminal palette, no startup probe and no `--theme` flag. Every styled span goes through `Theme`, whose methods are named for what they mark rather than for a colour, so a retargeted colour changes one method and no call site. The styles are judged against the palettes dive renders on: Solarized Dark, Solarized Light, macOS Terminal, and any terminal that honours either truecolor escapes or named ANSI colours.

### Tiers

| Tier | Marks | Style |
|---|---|---|
| 0 | Validation error | `Theme.error()` |
| 1 | Selection | `Theme.selection()` |
| 2 | Structural caption | `Theme.accent().bold()` |
| 3 | Label, "you-are-here" | `Theme.primary()` |
| 4 | Body content | `Style.EMPTY` (no `Theme` call) |
| 5 | Persistent chrome | `Theme.dim()` |

Tier 0 sits outside the hierarchy: it marks content as wrong, where every other tier marks what a piece of content is. It is unbolded, so an error row is not mistaken for the selected one.

### Decision tree

For new content, walk these steps in order; the first match gives the tier. Persistent chrome is tested before body content, so tier 5 comes before tier 4 in the walk. The tree is exhaustive.

1. A validation error, that is a declared-vs-actual mismatch between metadata fields that the reader should treat as a defect in the file rather than as data → tier 0, `Theme.error()`. It is checked first so a mismatch stays legible when it is the row under the cursor.
2. The active row in a navigable pane: table-row highlight, selected menu row, schema cursor row, footer cursor line, modal cursor → tier 1, `Theme.selection()`.
3. A structural caption: a section heading inside a pane, a table column header, the app brand → tier 2, `Theme.accent().bold()`.
4. A label: a key/value label, the breadcrumb leaf, an enabled menu label that is not selected, the `/` search prompt → tier 3, `Theme.primary()`.
5. Persistent chrome: keybar, modal hint row, non-head breadcrumb segments and the ` › ` separator, empty-state messages, search-result counts, parenthetical advisories, unfocused pane borders → tier 5, `Theme.dim()`.
6. Everything else is body content the user came to read (key/value values, schema row name, type, logical type and repetition, top-bar facts, table data) → tier 4, `Style.EMPTY`.

`dim()` is for chrome only; content the user came to read never uses it. `primary()` is for labels only; body content never uses it for emphasis.

### The `Theme` methods

| Method | Truecolor | Named ANSI |
|---|---|---|
| `primary()` | bold, default fg | same |
| `dim()` | ANSI faint, default fg | same |
| `accent()` | Solarized blue `#268bd2` | `Color.BLUE` |
| `selection()` | bold Solarized yellow `#b58900` | bold `Color.YELLOW` |
| `error()` | Solarized red `#dc322f` | `Color.RED` |

The three hued methods use truecolor RGB pinned to Solarized's accent slots when the terminal supports it, because a truecolor escape bypasses iTerm2's "Use bright colors for bold text" remap, which would otherwise turn bold ANSI blue into Solarized's body foreground. The named colours are the fallback, and on those terminals the remap either does not occur or lands on something readable.

`dim()` uses the faint attribute rather than a grey. Solarized's named greys are traps: `Color.GRAY` maps to `base2`, brighter than the foreground, and `Color.DARK_GRAY` to `base03`, the Solarized Dark background. A terminal that ignores faint renders the text at default fg.

Truecolor support is read on every call, from `$COLORTERM` (`truecolor` or `24bit`). It is never cached in a `static final` field: a native-image build can run a static initialiser at build time and would freeze the build runner's environment into the binary. The system property `hardwood.dive.truecolor` forces the truecolor branch for all three hued methods regardless of `$COLORTERM`. The `screenshots` Maven profile in `cli/pom.xml` sets it, so the checked-in SVGs under `docs/content/assets/cli/` carry the Solarized palette whatever terminal recorded them. It exists for reproducible captures and is not an end-user option.

### What is a smell

A `Color.*` constant, or a literal `Style.EMPTY.bold()`, `Style.EMPTY.fg(...)` or `Style.EMPTY.dim()`, anywhere outside `Theme.java`. Such a span bypasses both the tier it belongs to and the truecolor handling. A site that needs a `Color` rather than a `Style` extracts it with `.fg().orElseThrow()` from a `Theme` method.

### Recurring cases

| Content | Style |
|---|---|
| Key/value pane | label `Theme.primary()`, value `Style.EMPTY` |
| `Table` widget | header row `Theme.accent().bold()`, `highlightStyle(Theme.selection())` |
| Section delimiter inside a pane | `" Storage "` as `Theme.accent().bold()` |
| Menu row | label `Theme.primary()`, or `Theme.selection()` on the cursor row; a hint carrying a fact at `Style.EMPTY`, a trailing annotation (` · browse by column`) at `Theme.dim()` |
| Disabled menu row | same styles as enabled; the hint text (`n/a`, `—`) and the absent `▶` say it is disabled |
| Schema tree | expand/collapse marker `▼` / `▶` as `Theme.accent()` without bold |

### Pane borders

On a screen with focus tracking (`OverviewScreen`, `RowGroupDetailScreen`, `ColumnChunkDetailScreen`), the unfocused pane's `Block` sets `borderStyle(Theme.dim())`. The focused pane, and the only pane of a single-pane screen, sets no border style and renders in default fg. Borders never use `accent()`, which is reserved for textual captions so that blue reads as "heading" and not as decoration.

### Modal dimming

While a modal is open, the body behind it is faded so the modal stands out and the background reads as inert. A screen paints its body, then calls `buffer.setStyle(area, Theme.dim())` on the body area, then renders the modal last. `Buffer.setStyle` patches the faint modifier onto each cell without touching content or other modifiers; the modal's `Clear` then restores full intensity inside its own rectangle. Chrome (top bar, breadcrumb, keybar) is never dimmed. A modal that skips the dim call renders against a full-intensity background, unlike every other modal.

Tests: ThemeTest (cli). The tier assignment of individual spans is untested.

## Navigation

Two rules cover every pane: one for what the keys move, one for what the markers mean. Both are held by shared code rather than by each screen, because every pane that deviated from them had hand-rolled its own key handling.

### Rule 1: every key moves the cursor

`↑`/`↓`, `PgUp`/`PgDn` (aliased `Shift+↑`/`Shift+↓`) and `g`/`G` are one axis at three strides: one unit, one viewport, all the way. They never act on different things within one pane. A key already against its end is handled and does nothing; a key that is not a navigation key falls through to the screen's own handler.

A pane is exactly one of two kinds, chosen by what the reader does with it rather than by its content:

| Kind | Unit | State | Panes |
|---|---|---|---|
| Cursor pane | a row | an index into the rows; the scroll offset only keeps it on screen | list screens, menus, facts panes, schema tree, footer body, record modal |
| Scroll pane | a line | an offset into the lines; no cursor, `Enter` means nothing | page-header, dictionary-value, min/max and key/value modals, help overlay, read-failure overlay |

Every pane a reader can focus is a cursor pane, whether or not `Enter` does anything in it: the cursor is where the reader is, and a facts pane with nothing actionable still gets one so that `↑` means the same thing on it as on the pane beside it. Overlays are scroll panes because they are dismissed rather than navigated, so a position in them would not survive to be worth keeping.

Where a screen holds more than one pane, `Tab` / `Shift-Tab` switches focus, so the navigation keys change meaning only after an explicit move.

The cursor stops on every row, including rows `Enter` cannot act on. A row that is not actionable is still worth reading, and skipping it puts content out of reach of the only keys that reach content. Rule 2 is what tells the reader such a row is inert.

A row is a line that carries content. In a pane whose lines are not uniform (the facts panes and the footer body), section headings and the blank lines around them are decoration: they are painted and read, and the cursor passes over them because there is nothing on them to be at. The pane declares each line as a row or as decoration as it builds it, in `Document`, so no key handler knows which lines are inert. `PgUp`/`PgDn` in such a pane move a viewport of lines and land on the nearest row, so a page never carries the cursor further than a screenful. With the cursor on the first row, the window is pinned to the first line, so a leading heading is never stranded above the top.

### Rule 2: colour marks the cursor, `▶` marks what `Enter` acts on

- The cursor row is styled `Theme.selection()`. Colour is the only signal of where the cursor is.
- `▶` marks actionability. It is drawn on the cursor row whenever that row is actionable, and on other rows only when the pane holds some row `Enter` cannot act on, where the marker distinguishes one row from another. A pane whose rows are uniformly actionable therefore shows a single caret travelling with the cursor rather than a column of identical ones.

The marker column has a fixed width (`"▶ "` or `"  "`), so text starts at the same column whether or not a row is actionable. The keybar lists `Enter` only when it acts on the cursor row, labelled by what it does: `open` for a drill into another screen, `view <noun>` for a modal (`view entry`, `view min/max`), `expand` for in-place expansion. The keybar holds the current screen's keys on the left and the global `[?] help [q] quit` on the right; while a modal is open the screen's keys are dropped, because the modal's own hint row states what acts. With both rules, a non-actionable row is reachable and legible: the cursor can rest on it, and the missing `▶` says why `Enter` does nothing there.

The schema tree carries two markers in two columns: the actionability caret first, then the tree's `▶` / `▼` for a collapsed or expanded group, blank on leaves. Every schema row is actionable, so the first column holds a single travelling caret.

### Shared mechanisms

A screen does not handle navigation keys itself. It passes the key event and its current position to one of three helpers and stores the result in its state record; none of them retains state between calls.

The Data preview table is the exception: it handles its navigation keys itself, because its cursor is an absolute `long` row across page loads and each key picks the alignment of the page it reloads. Its keys follow rule 1 as the reader sees them.

| Helper | For | Supplies |
|---|---|---|
| `CursorPane` | a pane of uniform rows | `select(event, selection, count)`, the `marker(actionable, cursor, mixed)` column, the movement-key hints |
| `Document` | a cursor pane whose lines mix rows and decoration | the lines, which of them are rows, row selection with line-sized paging, the window top that keeps the cursor row visible |
| `ScrollPane` | an overlay | `scroll(event, offset, totalLines, viewport)`, the visible slice, modal geometry and the hint row stating what is out of view |

An overlay's content is wrapped to the modal width (`Strings.hardWrap` to `ScrollPane.modalWidth`) before it is handed to `ScrollPane`, so a value too long for a table cell is shown in full rather than cut at the right edge; showing such values is what the value modals are for. `ScrollPane` clamps a stored offset against the current content before adjusting it, so content that shrinks under a scrolled pane cannot leave a stale offset that swallows the next keypresses. The page stride comes from `Keys.viewportStride()`, which each render records, so `PgDn` moves by what is on screen.

Tests: CursorPaneTest, DocumentTest, ScrollPaneTest, DiveStateTest, DiveRenderTest (all cli).

## Viewport virtualization

Per-keystroke work on a list-shaped screen scales with the viewport, never with the collection. A dictionary with hundreds of thousands of entries, a column chunk with thousands of pages or a wide schema navigates as a small file does. Building a `Row` per item is invisible on small inputs and becomes the dominant per-frame cost on large ones, since the `Table` widget walks its whole row list to find the visible window.

A list screen's `render` builds `Row` objects only for the visible slice:

1. Compute the viewport as the pane height less the screen's chrome.
2. `RowWindow.from(scrollTop, selection, total, viewport)` returns `[start, end)` and `selectionInWindow()`. It clamps a stale `scrollTop` (the row count shrank, the selection jumped) to the nearest position that keeps the selection visible, and treats a non-positive viewport as one row so the cursor is still drawn.
3. Build rows for `[start, end)` only and call `TableState.select(window.selectionInWindow())`, so the table draws the cursor at the right row of the slice and never sees the rest of the collection.

Each screen's state record carries `scrollTop`, the absolute index of the first visible row. The key handler keeps it in step with `RowWindow.adjustTop(prevTop, selection, viewport)`, which scrolls as little as possible: motion inside the viewport leaves `scrollTop` alone, motion off either edge slides it just enough to keep the selection visible. So a step moves the cursor within the visible rows, `PgUp` lands the cursor at the top of the new viewport, and `PgDn` past the bottom keeps it at the bottom. `Document.windowTop` applies the same least-movement rule to facts panes and the footer, over lines.

Work outside the rows may still see the whole collection: the range header (`Plurals.rangeOf`), the search bar and the keybar use the full count, and the schema tree measures column widths over every row so they do not shift while scrolling. What stays O(viewport) is row construction and value formatting.

`DataPreviewScreen` does not use `RowWindow`: its state already holds only the page-sized slice that `PreviewWindow` loads (see [DIVE_ARCHITECTURE.md](DIVE_ARCHITECTURE.md)), and its selection is already slice-relative.

Two screens carry extra state around the window. `PagesScreen` addresses per-data-page statistics by a data-page index that skips dictionary pages, so it recovers that index at `window.start()` by counting the non-dictionary pages in the skipped prefix, a header-type scan rather than formatting work. `DictionaryScreen` memoises its `/` filter as an index array, with `null` standing for the identity mapping so an empty filter allocates nothing, and holds the `Dictionary` in its cache key through a `WeakReference` so an entry evicted from `ParquetModel`'s bounded cache is not pinned.

Tests: DiveStateTest (cli). That no screen builds rows beyond the viewport is untested.

## Boundaries

- How a pane reports content that is off screen (title range vs. hint row) differs between panes: #1009.
