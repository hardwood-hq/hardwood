/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive;

import java.time.Duration;

import dev.hardwood.cli.dive.internal.Chrome;
import dev.hardwood.cli.dive.internal.ColumnAcrossRowGroupsScreen;
import dev.hardwood.cli.dive.internal.ColumnChunkDetailScreen;
import dev.hardwood.cli.dive.internal.ColumnChunksScreen;
import dev.hardwood.cli.dive.internal.ColumnIndexScreen;
import dev.hardwood.cli.dive.internal.DataPreviewScreen;
import dev.hardwood.cli.dive.internal.DictionaryScreen;
import dev.hardwood.cli.dive.internal.FooterScreen;
import dev.hardwood.cli.dive.internal.HelpOverlay;
import dev.hardwood.cli.dive.internal.OffsetIndexScreen;
import dev.hardwood.cli.dive.internal.OverviewScreen;
import dev.hardwood.cli.dive.internal.PagesScreen;
import dev.hardwood.cli.dive.internal.RowGroupsScreen;
import dev.hardwood.cli.dive.internal.SchemaScreen;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.terminal.Frame;
import dev.tamboui.tui.TuiConfig;
import dev.tamboui.tui.TuiRunner;
import dev.tamboui.tui.event.Event;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;

/// Dispatches events to the active screen's handler and renders the chrome plus
/// active screen body each frame. Owns the [NavigationStack] and the help-overlay
/// flag; delegates everything else to the per-screen classes.
public final class DiveApp {

    private final ParquetModel model;
    private final NavigationStack stack;
    private boolean helpOpen;

    public DiveApp(ParquetModel model) {
        this.model = model;
        this.stack = new NavigationStack(new ScreenState.Overview(ScreenState.Overview.Pane.MENU, 0));
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
        if (ke.isCtrlC()) {
            runner.quit();
            return false;
        }
        boolean textInput = isTopInInputMode();
        if (!textInput && ke.isQuit()) {
            runner.quit();
            return false;
        }
        if (!textInput && ke.code() == KeyCode.CHAR && ke.character() == '?') {
            helpOpen = !helpOpen;
            return true;
        }
        if (helpOpen) {
            if (ke.isCancel()) {
                helpOpen = false;
                return true;
            }
            return false;
        }
        if (!textInput && ke.code() == KeyCode.CHAR && ke.character() == 'g' && !ke.hasCtrl() && !ke.hasAlt()) {
            stack.clearToRoot();
            return true;
        }
        // Screen gets first crack at the event so it can claim keys like Esc (filter-cancel)
        // before the global back-navigation intercept kicks in.
        if (dispatchToScreen(ke)) {
            return true;
        }
        if (ke.isCancel() && stack.depth() > 1) {
            stack.pop();
            return true;
        }
        return false;
    }

    private boolean isTopInInputMode() {
        return stack.top() instanceof ScreenState.DictionaryView d
                && DictionaryScreen.isInInputMode(d);
    }

    private boolean dispatchToScreen(KeyEvent ke) {
        return switch (stack.top()) {
            case ScreenState.Overview ignored -> OverviewScreen.handle(ke, model, stack);
            case ScreenState.Schema ignored -> SchemaScreen.handle(ke, model, stack);
            case ScreenState.RowGroups ignored -> RowGroupsScreen.handle(ke, model, stack);
            case ScreenState.ColumnChunks ignored -> ColumnChunksScreen.handle(ke, model, stack);
            case ScreenState.ColumnChunkDetail ignored -> ColumnChunkDetailScreen.handle(ke, model, stack);
            case ScreenState.Pages ignored -> PagesScreen.handle(ke, model, stack);
            case ScreenState.ColumnIndexView ignored -> ColumnIndexScreen.handle(ke, model, stack);
            case ScreenState.OffsetIndexView ignored -> OffsetIndexScreen.handle(ke, model, stack);
            case ScreenState.Footer ignored -> FooterScreen.handle(ke, model, stack);
            case ScreenState.ColumnAcrossRowGroups ignored -> ColumnAcrossRowGroupsScreen.handle(ke, model, stack);
            case ScreenState.DictionaryView ignored -> DictionaryScreen.handle(ke, model, stack);
            case ScreenState.DataPreview ignored -> DataPreviewScreen.handle(ke, model, stack);
        };
    }

    private void render(Frame frame) {
        Rect area = frame.area();
        Buffer buffer = frame.buffer();
        Chrome.Regions regions = Chrome.split(area);

        Chrome.renderTopBar(buffer, regions.topBar(), model);
        Chrome.renderBreadcrumb(buffer, regions.breadcrumb(), stack, model);
        renderBody(buffer, regions.body());
        Chrome.renderKeybar(buffer, regions.keybar(), keybarForActive(), " [?] help   [q] quit");

        if (helpOpen) {
            HelpOverlay.render(buffer, area);
        }
    }

    private void renderBody(Buffer buffer, Rect area) {
        ScreenState top = stack.top();
        switch (top) {
            case ScreenState.Overview s -> OverviewScreen.render(buffer, area, model, s);
            case ScreenState.Schema s -> SchemaScreen.render(buffer, area, model, s);
            case ScreenState.RowGroups s -> RowGroupsScreen.render(buffer, area, model, s);
            case ScreenState.ColumnChunks s -> ColumnChunksScreen.render(buffer, area, model, s);
            case ScreenState.ColumnChunkDetail s -> ColumnChunkDetailScreen.render(buffer, area, model, s);
            case ScreenState.Pages s -> PagesScreen.render(buffer, area, model, s);
            case ScreenState.ColumnIndexView s -> ColumnIndexScreen.render(buffer, area, model, s);
            case ScreenState.OffsetIndexView s -> OffsetIndexScreen.render(buffer, area, model, s);
            case ScreenState.Footer ignored -> FooterScreen.render(buffer, area, model);
            case ScreenState.ColumnAcrossRowGroups s -> ColumnAcrossRowGroupsScreen.render(buffer, area, model, s);
            case ScreenState.DictionaryView s -> DictionaryScreen.render(buffer, area, model, s);
            case ScreenState.DataPreview s -> DataPreviewScreen.render(buffer, area, model, s);
        }
    }

    private String keybarForActive() {
        return switch (stack.top()) {
            case ScreenState.Overview ignored -> OverviewScreen.keybarKeys();
            case ScreenState.Schema ignored -> SchemaScreen.keybarKeys();
            case ScreenState.RowGroups ignored -> RowGroupsScreen.keybarKeys();
            case ScreenState.ColumnChunks ignored -> ColumnChunksScreen.keybarKeys();
            case ScreenState.ColumnChunkDetail ignored -> ColumnChunkDetailScreen.keybarKeys();
            case ScreenState.Pages ignored -> PagesScreen.keybarKeys();
            case ScreenState.ColumnIndexView ignored -> ColumnIndexScreen.keybarKeys();
            case ScreenState.OffsetIndexView ignored -> OffsetIndexScreen.keybarKeys();
            case ScreenState.Footer ignored -> FooterScreen.keybarKeys();
            case ScreenState.ColumnAcrossRowGroups ignored -> ColumnAcrossRowGroupsScreen.keybarKeys();
            case ScreenState.DictionaryView ignored -> DictionaryScreen.keybarKeys();
            case ScreenState.DataPreview ignored -> DataPreviewScreen.keybarKeys();
        };
    }

    public NavigationStack stack() {
        return stack;
    }

    public boolean helpOpen() {
        return helpOpen;
    }
}
