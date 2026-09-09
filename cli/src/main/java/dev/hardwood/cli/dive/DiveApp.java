/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive;

import java.io.UncheckedIOException;
import java.time.Duration;

import dev.hardwood.cli.dive.internal.Chrome;
import dev.hardwood.cli.dive.internal.ColumnAcrossRowGroupsScreen;
import dev.hardwood.cli.dive.internal.ColumnChunkDetailScreen;
import dev.hardwood.cli.dive.internal.ColumnChunksScreen;
import dev.hardwood.cli.dive.internal.ColumnIndexScreen;
import dev.hardwood.cli.dive.internal.DataPreviewScreen;
import dev.hardwood.cli.dive.internal.DictionaryScreen;
import dev.hardwood.cli.dive.internal.FileIndexesScreen;
import dev.hardwood.cli.dive.internal.FooterScreen;
import dev.hardwood.cli.dive.internal.HelpOverlay;
import dev.hardwood.cli.dive.internal.Keys;
import dev.hardwood.cli.dive.internal.OffsetIndexScreen;
import dev.hardwood.cli.dive.internal.OverviewScreen;
import dev.hardwood.cli.dive.internal.PagesScreen;
import dev.hardwood.cli.dive.internal.ReadFailureOverlay;
import dev.hardwood.cli.dive.internal.RowGroupDetailScreen;
import dev.hardwood.cli.dive.internal.RowGroupIndexesScreen;
import dev.hardwood.cli.dive.internal.RowGroupsScreen;
import dev.hardwood.cli.dive.internal.SchemaScreen;
import dev.hardwood.cli.dive.internal.ScrollPane;
import dev.hardwood.cli.dive.internal.Theme;
import dev.hardwood.reader.ParquetReadException;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.terminal.Frame;
import dev.tamboui.tui.TuiConfig;
import dev.tamboui.tui.TuiRunner;
import dev.tamboui.tui.event.Event;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.widgets.Clear;

/// Dispatches events to the active screen's handler and renders the chrome plus
/// active screen body each frame. Owns the [NavigationStack] and the help-overlay
/// flag; delegates everything else to the per-screen classes.
public final class DiveApp {

    private final ParquetModel model;
    private final NavigationStack stack;
    private boolean helpOpen;

    /// Scroll offset into the help overlay, and the line count the last
    /// frame produced — the overlay wraps to the terminal width, so its
    /// length is not known until it has been rendered once.
    private int helpScroll;

    private int helpLineCount;

    /// The read failure the screen is showing instead of itself, and how far
    /// into its message the reader has scrolled; `null` when the screen is
    /// rendering normally.
    ///
    /// App state rather than screen state: every screen that reads from the
    /// file can fail this way, and putting it on [ScreenState] would make each
    /// of them carry a field for something none of them handles.
    private String readFailure;

    private int readFailureScroll;

    public DiveApp(ParquetModel model) {
        this.model = model;
        this.stack = new NavigationStack(ScreenState.Overview.initial());
        this.helpOpen = false;
    }

    /// Runs the TUI loop until the user quits. Uses default backend + alternate screen.
    public void run() throws Exception {
        TuiConfig config = TuiConfig.builder()
                .pollTimeout(Duration.ofMillis(100))
                .noTick()
                .build();
        try (TuiRunner runner = TuiRunner.create(config)) {
            runner.run(this::handleEvent, this::render);
        }
    }

    /// Renders a single frame against the given buffer — used by the smoke-render mode
    /// to prove the app wires up without entering raw mode.
    public void renderOnce(Buffer buffer) {
        render(Frame.forTesting(buffer));
    }

    private boolean handleEvent(Event event, TuiRunner runner) {
        if (!(event instanceof KeyEvent ke)) {
            return false;
        }
        Action action = dispatchKey(ke);
        if (action == Action.QUIT) {
            runner.quit();
            return false;
        }
        return action == Action.HANDLED;
    }

    /// Outcome of a key dispatch — returned by [#dispatchKey] so the
    /// runtime loop can act on `QUIT` (`runner.quit()`) and the test
    /// harness can assert behavior without driving a real TuiRunner.
    public enum Action { HANDLED, IGNORED, QUIT }

    /// Visible for testing: the same key-dispatch logic the runtime loop
    /// uses, minus the TuiRunner coupling. Returns [Action#QUIT] for
    /// `Ctrl-C` and (when not in text-input mode) `q`.
    public Action dispatchKey(KeyEvent ke) {
        if (ke.isCtrlC()) {
            return Action.QUIT;
        }
        boolean textInput = isTopInInputMode();
        if (!textInput && ke.isQuit()) {
            return Action.QUIT;
        }
        if (!textInput && ke.code() == KeyCode.CHAR && ke.character() == '?') {
            helpOpen = !helpOpen;
            helpScroll = 0;
            return Action.HANDLED;
        }
        if (helpOpen) {
            if (ke.isCancel()) {
                helpOpen = false;
                return Action.HANDLED;
            }
            // The overlay is longer than a short terminal can show, so it
            // scrolls like any other document pane while it has the keys.
            int next = ScrollPane.scroll(ke, helpScroll, helpLineCount, Keys.viewportStride());
            if (next != ScrollPane.UNHANDLED) {
                helpScroll = next;
                return Action.HANDLED;
            }
            return Action.IGNORED;
        }
        if (!textInput && ke.code() == KeyCode.CHAR && ke.character() == 'o' && !ke.hasCtrl() && !ke.hasAlt()) {
            stack.clearToRoot();
            return Action.HANDLED;
        }
        // A failure that is longer than its box owns the navigation keys while
        // there is more of it to read; the hint row advertises exactly those.
        if (readFailure != null) {
            int next = ScrollPane.scroll(ke, readFailureScroll,
                    ReadFailureOverlay.lineCount(readFailure), Keys.viewportStride());
            if (next != ScrollPane.UNHANDLED && next != readFailureScroll) {
                readFailureScroll = next;
                return Action.HANDLED;
            }
        }
        // Back-navigation is taken from the screen while a failure is showing.
        // A screen normally gets first refusal on Esc so it can claim it for
        // something of its own, but one that cannot read cannot honour that,
        // and its handler reads too — leaving Esc to it means the only key out
        // of the overlay fails on its way to the door.
        if (readFailure != null && ke.isCancel() && stack.depth() > 1) {
            clearReadFailure();
            stack.pop();
            return Action.HANDLED;
        }
        // Screen gets first crack at the event so it can claim keys like Esc (filter-cancel)
        // before the global back-navigation intercept kicks in.
        //
        // Keys still reach a screen that is showing a failure. That is what
        // lets a reader move off a damaged region rather than only back out of
        // it: the stack holds the last state that rendered, so paging from it
        // loads a different window, and a key that fails again simply replaces
        // one failure with the next.
        // IndexOutOfBounds is in the list because a corrupt file produces it:
        // a dictionary reference past the end of its dictionary is an array
        // index, and reading a damaged page is how you get one. A programming
        // error can raise it too, and that is the cost of not ending the
        // session over a file that is merely broken.
        try {
            if (dispatchToScreen(ke)) {
                clearReadFailure();
                return Action.HANDLED;
            }
        }
        catch (UncheckedIOException | ParquetReadException | IllegalStateException
                | IllegalArgumentException | IndexOutOfBoundsException e) {
            recordReadFailure(e);
            return Action.HANDLED;
        }
        if (ke.isCancel() && stack.depth() > 1) {
            clearReadFailure();
            stack.pop();
            return Action.HANDLED;
        }
        return Action.IGNORED;
    }

    /// Records a read failure for the next frame to show. The scroll offset
    /// resets: a new failure is a new message, and carrying an offset into it
    /// would open it part way down.
    private void recordReadFailure(RuntimeException e) {
        readFailure = ReadFailureOverlay.messageOf(e);
        readFailureScroll = 0;
    }

    private void clearReadFailure() {
        readFailure = null;
        readFailureScroll = 0;
    }

    private boolean isTopInInputMode() {
        return switch (stack.top()) {
            case ScreenState.DictionaryView d -> DictionaryScreen.isInInputMode(d);
            case ScreenState.Schema s -> SchemaScreen.isInInputMode(s);
            case ScreenState.ColumnIndexView c -> ColumnIndexScreen.isInInputMode(c);
            default -> false;
        };
    }

    private boolean dispatchToScreen(KeyEvent ke) {
        return switch (stack.top()) {
            case ScreenState.Overview ignored -> OverviewScreen.handle(ke, model, stack);
            case ScreenState.Schema ignored -> SchemaScreen.handle(ke, model, stack);
            case ScreenState.RowGroups ignored -> RowGroupsScreen.handle(ke, model, stack);
            case ScreenState.RowGroupDetail ignored -> RowGroupDetailScreen.handle(ke, model, stack);
            case ScreenState.RowGroupIndexes ignored -> RowGroupIndexesScreen.handle(ke, model, stack);
            case ScreenState.ColumnChunks ignored -> ColumnChunksScreen.handle(ke, model, stack);
            case ScreenState.ColumnChunkDetail ignored -> ColumnChunkDetailScreen.handle(ke, model, stack);
            case ScreenState.Pages ignored -> PagesScreen.handle(ke, model, stack);
            case ScreenState.ColumnIndexView ignored -> ColumnIndexScreen.handle(ke, model, stack);
            case ScreenState.OffsetIndexView ignored -> OffsetIndexScreen.handle(ke, model, stack);
            case ScreenState.Footer ignored -> FooterScreen.handle(ke, model, stack);
            case ScreenState.ColumnAcrossRowGroups ignored -> ColumnAcrossRowGroupsScreen.handle(ke, model, stack);
            case ScreenState.DictionaryView ignored -> DictionaryScreen.handle(ke, model, stack);
            case ScreenState.DataPreview ignored -> DataPreviewScreen.handle(ke, model, stack);
            case ScreenState.FileIndexes ignored -> FileIndexesScreen.handle(ke, model, stack);
        };
    }

    private void render(Frame frame) {
        Rect area = frame.area();
        Buffer buffer = frame.buffer();
        // Pre-seed Keys.viewportStride before computing the keybar so the
        // first frame after a screen change doesn't use the previous
        // screen's observation (or PAGE_STRIDE=20 fallback) — that
        // mismatch was making Footer's keybar advertise scroll keys for
        // one frame even though the body fit. Estimate body height as
        // area minus chrome (top bar + breadcrumb + 1-row keybar + 2
        // body borders); the screen's own render() refines this on
        // the same frame for the body itself, but the keybar is computed
        // first so it needs the seed.
        Keys.observeViewport(Math.max(1, area.height() - 5));
        Keys.observeViewportWidth(area.width());
        String screenKeys;
        try {
            screenKeys = keybarForActive();
        }
        catch (UncheckedIOException | ParquetReadException | IllegalStateException
                | IllegalArgumentException | IndexOutOfBoundsException e) {
            // The keybar reads from the file too — a screen that reports how
            // many pages a chunk has had to go and count them.
            recordReadFailure(e);
            screenKeys = " [Esc] back";
        }
        String globalKeys = " [?] help   [q] quit";
        int kbHeight = Chrome.keybarHeight(screenKeys, globalKeys, area.width());
        Chrome.Regions regions = Chrome.split(area, kbHeight);

        Chrome.renderTopBar(buffer, regions.topBar(), model);
        Chrome.renderBreadcrumb(buffer, regions.breadcrumb(), stack, model);
        if (readFailure == null) {
            try {
                // Data preview loads exactly the rows its body can paint, so the
                // page has to be fitted before the body is rendered rather than
                // after — otherwise the frame the screen is entered on is short.
                DataPreviewScreen.fitToViewport(model, stack, regions.body());
                renderBody(buffer, regions.body());
            }
            catch (UncheckedIOException | ParquetReadException | IllegalStateException
                    | IllegalArgumentException | IndexOutOfBoundsException e) {
                recordReadFailure(e);
            }
        }
        if (readFailure != null) {
            // A body that failed part way through has left some of itself
            // behind; clear it rather than paint the overlay over a half-drawn
            // screen, which is what makes an escaped exception look like a
            // corrupted display rather than a reported error.
            Clear.INSTANCE.render(regions.body(), buffer);
            ReadFailureOverlay.render(buffer, regions.body(), readFailure, readFailureScroll);
        }
        Chrome.renderKeybar(buffer, regions.keybar(), screenKeys, globalKeys);

        if (helpOpen) {
            // Faint the body so the modal pops; the modal's own
            // Clear+paint restores full intensity inside its area.
            // Chrome (top bar, breadcrumb, keybar) stays unchanged.
            buffer.setStyle(regions.body(), Theme.dim());
            helpLineCount = HelpOverlay.lineCount(area);
            HelpOverlay.render(buffer, area, helpScroll);
        }
    }

    private void renderBody(Buffer buffer, Rect area) {
        ScreenState top = stack.top();
        switch (top) {
            case ScreenState.Overview s -> OverviewScreen.render(buffer, area, model, s);
            case ScreenState.Schema s -> SchemaScreen.render(buffer, area, model, s);
            case ScreenState.RowGroups s -> RowGroupsScreen.render(buffer, area, model, s);
            case ScreenState.RowGroupDetail s -> RowGroupDetailScreen.render(buffer, area, model, s);
            case ScreenState.RowGroupIndexes s -> RowGroupIndexesScreen.render(buffer, area, model, s);
            case ScreenState.ColumnChunks s -> ColumnChunksScreen.render(buffer, area, model, s);
            case ScreenState.ColumnChunkDetail s -> ColumnChunkDetailScreen.render(buffer, area, model, s);
            case ScreenState.Pages s -> PagesScreen.render(buffer, area, model, s);
            case ScreenState.ColumnIndexView s -> ColumnIndexScreen.render(buffer, area, model, s);
            case ScreenState.OffsetIndexView s -> OffsetIndexScreen.render(buffer, area, model, s);
            case ScreenState.Footer s -> FooterScreen.render(buffer, area, model, s);
            case ScreenState.ColumnAcrossRowGroups s -> ColumnAcrossRowGroupsScreen.render(buffer, area, model, s);
            case ScreenState.DictionaryView s -> DictionaryScreen.render(buffer, area, model, s);
            case ScreenState.DataPreview s -> DataPreviewScreen.render(buffer, area, model, s);
            case ScreenState.FileIndexes s -> FileIndexesScreen.render(buffer, area, model, s);
        }
    }

    private String keybarForActive() {
        return switch (stack.top()) {
            case ScreenState.Overview s -> OverviewScreen.keybarKeys(s, model);
            case ScreenState.Schema s -> SchemaScreen.keybarKeys(s, model);
            case ScreenState.RowGroups s -> RowGroupsScreen.keybarKeys(s, model);
            case ScreenState.RowGroupDetail s -> RowGroupDetailScreen.keybarKeys(s, model);
            case ScreenState.RowGroupIndexes s -> RowGroupIndexesScreen.keybarKeys(s, model);
            case ScreenState.ColumnChunks s -> ColumnChunksScreen.keybarKeys(s, model);
            case ScreenState.ColumnChunkDetail s -> ColumnChunkDetailScreen.keybarKeys(s, model);
            case ScreenState.Pages s -> PagesScreen.keybarKeys(s, model);
            case ScreenState.ColumnIndexView s -> ColumnIndexScreen.keybarKeys(s, model);
            case ScreenState.OffsetIndexView s -> OffsetIndexScreen.keybarKeys(s, model);
            case ScreenState.Footer s -> FooterScreen.keybarKeys(s, model);
            case ScreenState.ColumnAcrossRowGroups s -> ColumnAcrossRowGroupsScreen.keybarKeys(s, model);
            case ScreenState.DictionaryView s -> DictionaryScreen.keybarKeys(s, model);
            case ScreenState.DataPreview s -> DataPreviewScreen.keybarKeys(s, model);
            case ScreenState.FileIndexes s -> FileIndexesScreen.keybarKeys(s, model);
        };
    }

    public NavigationStack stack() {
        return stack;
    }

    public boolean helpOpen() {
        return helpOpen;
    }
}
