# Dive architecture

How `hardwood dive`, the interactive terminal UI in the `cli` module, is put together: the navigation stack of screen states, the split between state records, key handlers and renderers, the `ParquetModel` that owns the open file and its caches, the Data preview row window, the guard that turns a failed read into an overlay, and the test layers. The visual tiers, the navigation keys and `▶`, the `CursorPane`/`ScrollPane` panes and viewport virtualization with `RowWindow` are in [DIVE_UI_RULES.md](DIVE_UI_RULES.md); how values and figures are spelled on every screen is in [CLI_VALUE_RENDERING.md](CLI_VALUE_RENDERING.md). Screens, keys and user-visible behaviour are listed in [docs/content/reference/cli.md](../docs/content/reference/cli.md#interactive-exploration-dive).

## Structure

Dive is read-only and runs in one process against one file. It is built on TamboUI (`dev.tamboui`), an immediate-mode terminal UI library on a JLine backend that survives GraalVM native-image builds, so dive ships inside the native CLI binary.

| Class | Package | Role |
|---|---|---|
| `DiveCommand` | `cli.command` | Entry point: flags, TTY check, logging, opens the model, runs the app |
| `DiveApp` | `cli.dive` | Event loop wiring, global keys, dispatch to the active screen, frame layout, read-failure guard |
| `NavigationStack` | `cli.dive` | The stack of `ScreenState` records |
| `ScreenState` | `cli.dive` | Sealed interface, one immutable record per screen |
| `ParquetModel` | `cli.dive` | The open file, footer-derived facts, lazily read and cached per-chunk structures |
| `*Screen` | `cli.dive.internal` | One class per screen: handler, renderer, keybar text |
| `Chrome` | `cli.dive.internal` | Top bar, breadcrumb and keybar around the body |
| `PreviewWindow` | `cli.dive.internal` | Row buffer behind Data preview |
| `ReadFailureOverlay`, `HelpOverlay` | `cli.dive.internal` | The two overlays `DiveApp` owns |

Screens live in the `internal` sub-package; `DiveApp`, `NavigationStack`, `ScreenState` and `ParquetModel` are public so the command and the tests in other packages reach them.

## Screen stack

`NavigationStack` holds the reader's drill-down path as a list of `ScreenState` records. Its bottom frame is always `ScreenState.Overview`: the constructor rejects any other root and `pop` never removes it. Four operations change it:

| Operation | Use |
|---|---|
| `push` | Drill into a child screen |
| `pop` | Return to the parent screen |
| `replaceTop` | Record a change to the current screen (cursor, scroll, toggle, modal) |
| `clearToRoot` | Collapse to Overview (`o`) |

There are fifteen `ScreenState` variants. Drills form a tree rooted at Overview, with two routes into the per-chunk leaf screens (Pages, Column index, Offset index, Dictionary):

- Overview → Row groups → Row group detail → Column chunks → Column chunk detail → leaf; Row group detail also leads to Row group indexes → Column index or Offset index.
- Overview → Schema → Column across row groups → Column chunk detail → leaf.
- Overview → Footer & indexes → File indexes (one kind, file-wide) → leaf.
- Overview → Data preview.

The breadcrumb is derived from the stack each frame. A leaf reached without a frame that names its row group and column (the File indexes route) gets `(RG #N · column)` appended to its label, so the breadcrumb always identifies the chunk on screen.

Modals that belong to one screen (the page-header modal, the column index min/max modal, the dictionary full-value modal, the Data preview record modal, the key/value metadata modal) are fields of that screen's record, not stack frames: `Esc` closes them through the screen's own handler before it would pop.

Tests: `DiveStateTest`, `DiveRenderTest` (cli).

## State, handler, renderer

Each screen contributes four things, all static methods on its `*Screen` class:

| Part | Shape | Contract |
|---|---|---|
| State | a `ScreenState` record | Immutable; holds cursor, scroll offsets, parent context (row group, column), toggles and modal flags |
| Handler | `handle(KeyEvent, ParquetModel, NavigationStack) → boolean` | Returns whether it claimed the key; changes the screen only through the stack |
| Renderer | `render(Buffer, Rect, ParquetModel, State)` | Draws the body region from the record and the model |
| Keybar | `keybarKeys(State, ParquetModel) → String` | Lists only the keys that would do something visible in this state, built with `Keys.Hints` |

Display strings and widgets are derived each frame from the record and the model rather than stored. `ScreenState.DataPreview` is the one exception: it carries the formatted rows of its current page, because loading them is I/O and the handler, which moves the page, is where that I/O happens.

A handler reads the model and may cause it to fill a cache, but changes nothing a later frame could observe except through the stack. A renderer receives the state by value and cannot change the stack. This is what lets the handler tests drive screens with synthesised key events and assert on the stack alone, without a terminal.

`DiveApp.dispatchKey` applies the global gates before any screen sees a key, in this order:

1. `Ctrl-C` quits.
2. When the top screen is in text-input mode (the `/` filter on Schema, Column index or Dictionary), `q`, `?` and `o` are passed to the screen as characters. Otherwise `q` quits and `?` toggles the help overlay.
3. While the help overlay is open it takes every key: `Esc` closes it, the navigation keys scroll it.
4. `o` clears any read failure and collapses to Overview.
5. The read-failure gates (see [Read-failure guard](#read-failure-guard)).
6. The active screen's handler gets the key.
7. An unclaimed `Esc` pops one frame.

Screens get first refusal on `Esc` so they can use it to close a modal or cancel a filter.

`dispatchKey` returns `HANDLED`, `IGNORED` or `QUIT`; the runtime loop calls `runner.quit()` on `QUIT`, and tests call `dispatchKey` directly without a `TuiRunner`.

Each frame, `DiveApp.render` computes the keybar before the body, because the keybar's height decides the body's height; `Chrome.split` then carves the frame into top bar, breadcrumb, body and keybar. The help overlay is drawn last, over a dimmed body.

### Process-static side channels

Some state lives outside the stack in static fields:

| State | Where | Why |
|---|---|---|
| Observed viewport rows, terminal width, modal width, Data preview area | `Keys` | A handler needs the geometry the last frame rendered with (page stride, wrap width, which record-modal field is expandable) and receives no `Rect` |
| The row buffer | `DataPreviewScreen.WINDOW` (`PreviewWindow`) | Survives across state records; see [Data preview window](#data-preview-window) |
| Last filter result | `DictionaryScreen` | One-slot memo so a large dictionary is not re-filtered twice per keystroke; holds the dictionary weakly so it does not pin an entry `ParquetModel` evicted |

`DiveApp.render` seeds `Keys.observeViewport` from the frame height before computing the keybar, so the first frame after a screen change uses this frame's stride rather than the previous screen's. Tests that mix render and handler paths reset the observations with `Keys.resetObservedGeometry()`. This state assumes one dive session per process and one UI thread.

Tests: `DiveStateTest`, `DiveAppTest`, `DiveRenderTest` (cli).

## ParquetModel

`ParquetModel` owns the session's view of the file. `ParquetModel.open` opens a `ParquetFileReader`, which reads and parses the footer, and derives the aggregate facts (`Facts`: totals, ratio, key/value metadata) at construction. `DiveCommand` opens the model before the terminal enters raw mode, so a file that cannot be opened fails with an ordinary error message. The model holds the reader open for the session and closes it on `close()`.

Everything below the footer is read lazily, on the first screen that asks, and cached:

| Structure | Key | Read | Retention |
|---|---|---|---|
| Page-index slices | row group | `RowGroupIndexBuffers.fetch` for every column: the row group's ColumnIndexes and its OffsetIndexes, one `readRange` per structure when each is contiguous | Session; bounded by the file's page-index bytes |
| `ColumnIndex`, `OffsetIndex` | (row group, column) | Parsed from that row group's slices | Session; `null` cached for a chunk without one |
| Page headers | (row group, column) | The chunk's compressed extent walked header to header by `PageHeaderWalk`, in bounded `readRange` windows (one read for a chunk that fits one); a window ends at the first page that does not fit, so a body longer than a window is skipped | Bounded LRU of chunks |
| `Dictionary` | (row group, column) | The dictionary page alone, read by `DictionaryParser.readPage` (the read the dictionary filter makes) and parsed by `DictionaryParser` | Bounded LRU of chunks |
| Dictionary entry count | (row group, column) | A bounded probe of the dictionary page header, located as the dictionary load locates it (`Encodings.dictionaryEntries` through `DictionaryParser.readPageHeader`) | Session |
| Dictionary page size | (row group, column) | The same probe (`DictionaryParser.readPageHeader`) | Session |
| Group paths of the schema | — | Walk of the schema | Session |

The LRU bounds exist because page-header lists and dictionaries scale with the data, and a long session over a wide file would otherwise retain one per chunk visited. The page index is fetched per row group so that a screen listing every chunk of a row group costs one round trip per structure on remote storage rather than one per chunk.

The dictionary entry count is asked for only by Column chunk detail. The list screens do not show it: one read per visible row would cost a round trip per row on remote storage, paid again for every row scrolled into view.

`dictionary` refuses a chunk whose dictionary page's compressed size exceeds the read cap (`--max-dict-bytes`) and returns `null`; the Dictionary screen then shows a confirm prompt naming that size, and `dictionaryForced` loads it once the reader opts in. The size is what the load reads and decompresses, so it comes from the page's own header (`dictionaryPageBytes`), not from the footer's chunk size.

A chunk whose `file_path` names another file is refused (`ColumnChunk.requireSameFile`) before its page headers or dictionary are read from its offsets.

Data preview rows go through the read pipeline, not through the byte-level reads above. `readPreviewPage(firstRow, count, consumer)` builds a fresh `RowReader` with `skip(firstRow).head(count)`, hands each row to the consumer and closes the reader before returning. The `head` bound is part of the contract: an unbounded reader lets the pipeline's column workers read ahead of the rows asked for, and that queued read-ahead is fetched later, when some other event unblocks the workers, as I/O the reader did not ask for. `skip` is the row-based seek of [ROW_READER.md](ROW_READER.md#skip), so a jump deep into the file reads only the row groups it lands in.

For an S3 file, `DiveCommand` asks `FileMixin` for `RangeBacking.SPARSE_TEMPFILE`, which caches every fetched range for the session so revisiting a screen does not refetch; see [S3_STORAGE.md](S3_STORAGE.md). The top bar shows the request count and bytes fetched from `ParquetModel.netStats()`.

All I/O happens on the UI thread, inside a handler or a render.

Tests: `DiveStateTest`, `DataPreviewIoTest` (cli). The cache bounds and the one-read-per-row-group index fetch are untested.

## Data preview window

`PreviewWindow` buffers pre-formatted Data preview rows around the viewport so that paging within a horizon does no I/O. It covers a range `[start, end)` of absolute row indices. `slice(model, firstRow, pageSize, logicalTypes)` returns the requested page:

- If the page lies inside `[start, end)`, it is served from memory.
- Otherwise the buffer is replaced by a range reaching a fixed number of pages before and after `firstRow`, clamped to the file, fetched in one `readPreviewPage` call.

A miss replaces the buffer rather than extending it, so memory stays bounded by a fixed number of pages whichever way the reader travels, and each refill buys the same stretch of navigation in both directions. The refill range always contains the requested page. Backward moves and far jumps (`g`, `G`) are ordinary misses; there is no cursor to rewind.

Each cell is formatted four times on fetch: compact and expanded, each with and without logical-type rendering. Toggling `t` selects a different pair of lists and does no I/O. The expanded form feeds the record modal and carries no length budget.

A refill clears the buffer and marks it empty before fetching, so a fetch that fails part way leaves nothing servable and the next request refills again rather than returning stale rows. The window is bound to the `ParquetModel` it was filled from and reinitialises when handed another.

`DataPreviewScreen.fitToViewport` runs before the body is rendered: when the page held in the state does not match the body height, it reloads the page at the right size, so the frame the screen is entered on is full. It does nothing while the record modal is open.

The window is a static instance, not thread-safe, and relies on dive's single UI thread.

Tests: `PreviewWindowTest`, `DataPreviewIoTest`, `DiveRenderTest` (cli).

## Read-failure guard

A damaged file must not end the session. Every screen that reads from the file does so inside its handler, its renderer or its keybar, and an exception escaping any of them would end the TamboUI loop with nothing left to press `Esc` in. `DiveApp` guards all three centrally, so no screen carries an error state and a new screen is covered without opting in.

`DiveApp` holds a nullable failure message and a scroll offset into it. These are app state, not screen state: `ScreenState` describes what a screen shows, and handlers stay functions of `(event, model, stack)`.

| Path | On `RuntimeException` |
|---|---|
| Key dispatch to the screen | Record the failure; the stack is left as it was before the key |
| Keybar | Record the failure; the keybar falls back to `[Esc] back` |
| `fitToViewport` and body render | Record the failure |

When a failure is recorded, the body is cleared and `ReadFailureOverlay` is painted over it in the same frame. Clearing matters: an overlay over a half-drawn body reads as a corrupted display. The top bar and breadcrumb render outside the guard; they read only footer-derived state. Untested.

The guard catches `RuntimeException` as a whole. The type a damaged file raises is decided by the decoder that trips over it (an out-of-range dictionary index raises `ArrayIndexOutOfBoundsException`, an impossible RLE run header `IllegalStateException`, a length that does not fit `ArithmeticException`). The byte-level reads in `ParquetModel` run outside the read pipeline and classify their failures as the pipeline does (below), but a failure raised elsewhere on the screen thread reaches the guard unclassified. A defect in dive's own render code is therefore reported as a read failure, not as a stack trace.

While a failure shows:

- The overlay is a `ScrollPane` modal. When the message is longer than the box, the navigation keys scroll it first and reach the screen once it is at its end.
- `Esc` is taken from the screen: it clears the failure and pops one frame, or at the root only clears it. The screen's own `Esc` handling may read, and a screen that failed to read cannot be relied on to let the reader out.
- `o` clears the failure on its way to the root.
- Other keys reach the screen underneath. The stack still holds the last state that rendered, so paging away from a damaged region (a `PgUp` from a failed Data preview page) loads different data; a key the screen claims clears the failure, and a key that fails again replaces it.

The overlay adds no wording of its own. `ReadFailureOverlay.messageOf` shows the exception's message, unwrapping a bare `UncheckedIOException` whose message is only its cause's `toString()`, and falls back to the simple class name when there is no message. Failures from the read pipeline arrive already placed by file, row group and column. The byte-level reads in `ParquetModel` (column index, offset index, page headers, dictionary, dictionary entry count) bypass the pipeline, so `ParquetModel` classifies and places their failures itself with `ExceptionContext.readFailureAt`: `asReadFailure` first, as the pipeline does, so a decoder's `ArrayIndexOutOfBoundsException` on a corrupt file becomes a `ParquetReadException`, then `addReadContext`, which keeps the resulting type. The commands (`inspect pages`, `inspect dictionary`, `dive` among them) catch `ParquetReadException` to report a broken file cleanly, and a placed failure of another type would escape them. See [EXCEPTION_MODEL.md](EXCEPTION_MODEL.md).

Only failures the reader detects are reported. `PLAIN` data has no redundancy, and every bit pattern of an RLE run header is a valid header, so most corruption of page data decodes to wrong values without an error unless the writer emitted page CRCs.

Tests: `DiveReadFailureTest` (cli) damages a page header, a column index, an offset index read across row groups, a dictionary page header, a dictionary page body and a data page, each at an offset derived from the file's own metadata, and drives the screen that reads it. Scrolling a long message and paging away from a failure are untested.

## Relationship to the non-interactive commands

Dive composes what `info`, `schema`, `footer`, `inspect` and `print` show separately into one navigable session; the batch commands remain the surface for scripts, pipes and agents. The dependency runs one way: `DiveCommand` depends on `cli.dive`, and no other command does.

What dive shares with the commands lives in `dev.hardwood.cli.internal`: value and size formatting (`ValueFormatter`, `Sizes`, `Fmt`, `BinaryValues`, `LevelSummary`, governed by [CLI_VALUE_RENDERING.md](CLI_VALUE_RENDERING.md)), encoding helpers (`Encodings`), the page-header walk (`PageHeaderWalk`) and text helpers (`Strings`). `FileMixin` resolves `-f` for dive as for every command, local path or S3 URI. `InspectPagesCommand` and `InspectDictionaryCommand` parse page headers and dictionaries at the byte level as `ParquetModel` does, and classify and place their failures with the same `ExceptionContext.readFailureAt`; `inspect pages` walks page headers with `PageHeaderWalk` and `inspect dictionary` reads the dictionary page with `DictionaryParser.readPage`, as dive does.

`DiveCommand` adds what an interactive session needs:

- It refuses to start without a console (`System.console() == null`), since a TUI on a redirected stream renders nothing usable.
- It detaches the `dev.hardwood` logger from its parent handlers for the session, since a log line on the terminal would garble the frame; `--log-file` routes `FINE` records to a file instead.
- `--max-dict-bytes` sets the model's dictionary read cap.
- The hidden `--smoke-render` renders one 120×40 frame into a memory buffer and exits 0, bypassing the console check, so the native binary can be checked for classes lost in the image build.

Native-image metadata for TamboUI and JLine is under `cli/src/main/resources/META-INF/native-image/`.

Tests: `DiveCommandTest`, `NativeBinarySmokeIT` (cli).

## Test layers

| Layer | Drives | Asserts on | Classes (cli) |
|---|---|---|---|
| Handler | A screen's `handle` with synthesised `KeyEvent`s against a fixture model | The resulting `NavigationStack` | `DiveStateTest` |
| Global dispatch | `DiveApp.dispatchKey` | Help toggle, `o`, quit, input-mode pass-through | `DiveAppTest` |
| Render | Screens and chrome through `RenderHarness` into an in-memory `Buffer` | Captured cells: titles, rows, markers, breadcrumb; a parameterised matrix renders every screen against every fixture | `DiveRenderTest` |
| Failure | `DiveApp` against deliberately damaged copies of a fixture | Session survives, overlay content, `Esc` and `o` | `DiveReadFailureTest` |
| Data access | `PreviewWindow` and `readPreviewPage` against counting `InputFile`s | Rows returned, I/O per navigation step, bytes skipped by a seek | `PreviewWindowTest`, `DataPreviewIoTest` |
| Panes and helpers | `CursorPane`, `ScrollPane`, `Document`, `Plurals`, `Theme`, `KvMetadataFormatter` | Unit behaviour | `*Test` in `cli.dive.internal` |
| End to end | `hardwood dive --smoke-render` on the JVM and on the native binary | Exit code | `DiveCommandTest`, `NativeBinarySmokeIT` |

Handler tests and render tests share process-static geometry in `Keys`; the `cli.dive` test classes reset it before each test.

## Boundaries

- All reads run on the UI thread, so a slow remote read blocks the frame (#331).
- The process-static state in `Keys`, `DataPreviewScreen` and `DictionaryScreen` assumes one session per process (#339).
- Corrupt page data that decodes without error is shown as values; page CRCs catch it only where the writer emitted them (#1095).
- The non-interactive commands do not share dive's failure containment: a failure mid-read prints a stack trace and leaves partial output (#1094).
