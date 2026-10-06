/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive;

import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.cli.dive.internal.DataPreviewScreen;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.buffer.Cell;
import dev.tamboui.layout.Rect;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.tui.event.KeyModifiers;

import static org.assertj.core.api.Assertions.assertThat;

/// Smoke tests for the global key-dispatch wiring in [DiveApp]. The
/// per-screen handlers are covered by [DiveStateTest]; these target the
/// global gates (`?`, `o`, `q`/Ctrl-C, input-mode suppression) that the
/// runtime loop applies before delegating to a screen.
class DiveAppTest {

    private ParquetModel model;
    private DiveApp app;

    @BeforeEach
    void openFixture() throws Exception {
        dev.hardwood.cli.dive.internal.Keys.resetObservedGeometry();
        Path path = Path.of(getClass().getResource("/column_index_pushdown.parquet").getPath());
        model = ParquetModel.open(InputFile.of(path), path.toString());
        app = new DiveApp(model);
    }

    @AfterEach
    void closeModel() throws Exception {
        model.close();
    }

    @Test
    void questionMarkTogglesHelpOverlay() {
        assertThat(app.helpOpen()).isFalse();

        DiveApp.Action a = app.dispatchKey(charKey('?'));

        assertThat(a).isEqualTo(DiveApp.Action.HANDLED);
        assertThat(app.helpOpen()).isTrue();

        DiveApp.Action b = app.dispatchKey(charKey('?'));

        assertThat(b).isEqualTo(DiveApp.Action.HANDLED);
        assertThat(app.helpOpen()).isFalse();
    }

    @Test
    void escapeClosesHelpOverlay() {
        app.dispatchKey(charKey('?'));
        assertThat(app.helpOpen()).isTrue();

        DiveApp.Action a = app.dispatchKey(plainKey(KeyCode.ESCAPE));

        assertThat(a).isEqualTo(DiveApp.Action.HANDLED);
        assertThat(app.helpOpen()).isFalse();
    }

    @Test
    void oReturnsToOverviewFromDeepStack() {
        app.stack().push(new ScreenState.RowGroups(0));
        app.stack().push(new ScreenState.RowGroupDetail(0, ScreenState.RowGroupDetail.Pane.MENU, 0));
        app.stack().push(new ScreenState.ColumnChunks(0, 0));
        assertThat(app.stack().depth()).isEqualTo(4);

        DiveApp.Action a = app.dispatchKey(charKey('o'));

        assertThat(a).isEqualTo(DiveApp.Action.HANDLED);
        assertThat(app.stack().depth()).isEqualTo(1);
        assertThat(app.stack().top()).isInstanceOf(ScreenState.Overview.class);
    }

    @Test
    void qQuitsWhenNotInInputMode() {
        DiveApp.Action a = app.dispatchKey(charKey('q'));

        assertThat(a).isEqualTo(DiveApp.Action.QUIT);
    }

    @Test
    void ctrlCAlwaysQuits() {
        // Even mid-search in Dictionary, Ctrl-C bypasses the input-mode gate.
        app.stack().push(new ScreenState.DictionaryView(0, 0, 0, false, "id", true, false, false));

        DiveApp.Action a = app.dispatchKey(new KeyEvent(KeyCode.CHAR, KeyModifiers.CTRL, 'c'));

        assertThat(a).isEqualTo(DiveApp.Action.QUIT);
    }

    @Test
    void qIsNotConsumedWhileEditingDictionaryFilter() {
        // searching=true puts the dictionary screen in input mode; q should
        // append to the filter rather than quit.
        app.stack().push(new ScreenState.DictionaryView(0, 0, 0, false, "", true, false, false));

        DiveApp.Action a = app.dispatchKey(charKey('q'));

        assertThat(a).isNotEqualTo(DiveApp.Action.QUIT);
        // Filter should now contain the typed character.
        ScreenState.DictionaryView top = (ScreenState.DictionaryView) app.stack().top();
        assertThat(top.filter()).isEqualTo("q");
    }

    @Test
    void oIsNotConsumedWhileEditingDictionaryFilter() {
        // o would normally clearToRoot; when typing into the filter it must
        // be passed through as a printable character instead.
        app.stack().push(new ScreenState.DictionaryView(0, 0, 0, false, "", true, false, false));

        app.dispatchKey(charKey('o'));

        assertThat(app.stack().depth()).isEqualTo(2);
        ScreenState.DictionaryView top = (ScreenState.DictionaryView) app.stack().top();
        assertThat(top.filter()).isEqualTo("o");
    }

    @Test
    void colonOpensJumpPromptOnDataPreview() {
        app.stack().push(DataPreviewScreen.initialState(model, 10));

        DiveApp.Action a = app.dispatchKey(charKey(':'));

        assertThat(a).isEqualTo(DiveApp.Action.HANDLED);
        assertThat(app.jumpPrompt()).isNotNull();
        assertThat(app.jumpPrompt().input()).isEmpty();
    }

    @Test
    void colonDoesNothingOnAScreenWithoutAResolver() {
        // Only Data preview and Row groups are jumpable in this PR; the rest
        // gain a resolver in a follow-up.
        app.stack().push(ScreenState.Schema.initial());

        app.dispatchKey(charKey(':'));

        assertThat(app.jumpPrompt()).isNull();
    }

    @Test
    void jumpPromptCollectsDigitsAndIgnoresOtherCharacters() {
        app.stack().push(DataPreviewScreen.initialState(model, 10));

        type(":4a3");

        assertThat(app.jumpPrompt().input()).isEqualTo("43");
    }

    @Test
    void jumpPromptBackspaceTrimsTheTypedTarget() {
        app.stack().push(DataPreviewScreen.initialState(model, 10));

        type(":43");
        app.dispatchKey(plainKey(KeyCode.BACKSPACE));

        assertThat(app.jumpPrompt().input()).isEqualTo("4");
    }

    @Test
    void jumpPromptEscapeClosesWithoutMoving() {
        app.stack().push(DataPreviewScreen.initialState(model, 10));
        app.renderOnce(buffer());
        for (int i = 0; i < 5; i++) {
            app.dispatchKey(plainKey(KeyCode.DOWN));
        }
        app.renderOnce(buffer());
        ScreenState.DataPreview before = (ScreenState.DataPreview) app.stack().top();

        type(":4321");
        app.renderOnce(buffer());
        DiveApp.Action a = app.dispatchKey(plainKey(KeyCode.ESCAPE));
        app.renderOnce(buffer());

        assertThat(a).isEqualTo(DiveApp.Action.HANDLED);
        assertThat(app.jumpPrompt()).as("prompt closed").isNull();
        assertThat(app.stack().top()).as("view stayed put").isEqualTo(before);
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, 4321, 9985, 9999 })
    void jumpKeepsTheTargetSelectedAfterRendering(long target) {
        app.stack().push(DataPreviewScreen.initialState(model, 10));
        app.renderOnce(buffer());

        type(":" + target);
        app.renderOnce(buffer());
        app.dispatchKey(plainKey(KeyCode.ENTER));
        app.renderOnce(buffer());

        assertThat(app.jumpPrompt()).isNull();
        ScreenState.DataPreview state = (ScreenState.DataPreview) app.stack().top();
        assertThat(state.firstRow() + state.selectedRow()).isEqualTo(target);
        assertThat(state.rows().get(state.selectedRow()).get(0)).isEqualTo(Long.toString(target));
    }

    @Test
    void jumpToARowSelectsItAtTheTopOfTheViewport() {
        app.stack().push(DataPreviewScreen.initialState(model, 10));

        type(":4321");
        app.dispatchKey(plainKey(KeyCode.ENTER));

        assertThat(app.jumpPrompt()).as("prompt closed on a successful jump").isNull();
        ScreenState.DataPreview state = (ScreenState.DataPreview) app.stack().top();
        assertThat(state.firstRow()).isEqualTo(4321L);
        assertThat(state.selectedRow()).isZero();
        // `id` in column_index_pushdown.parquet is sorted 0..9999, so the
        // selected row proves the seek landed on the row that was asked for.
        assertThat(state.rows().get(0).get(0)).isEqualTo("4321");
    }

    @Test
    void jumpToARowPastTheLastIsRefusedAndNothingMoves() {
        app.stack().push(DataPreviewScreen.initialState(model, 10));

        type(":10000");
        app.dispatchKey(plainKey(KeyCode.ENTER));

        assertThat(app.jumpPrompt()).as("prompt stayed open").isNotNull();
        assertThat(app.jumpPrompt().input()).as("typed text kept").isEqualTo("10000");
        assertThat(app.jumpPrompt().error()).isEqualTo("Row 10,000 is outside 0–9,999");
        assertThat(((ScreenState.DataPreview) app.stack().top()).firstRow())
                .as("not clamped to the last row").isZero();
    }

    @Test
    void jumpWithNothingTypedIsRefused() {
        app.stack().push(DataPreviewScreen.initialState(model, 10));

        app.dispatchKey(charKey(':'));
        app.dispatchKey(plainKey(KeyCode.ENTER));

        assertThat(app.jumpPrompt().error()).isEqualTo("Type a row number");
    }

    @Test
    void jumpPromptStopsTakingDigitsPastEighteen() {
        // Eighteen digits name more rows than any file holds; a nineteenth
        // could overflow a long.
        app.stack().push(DataPreviewScreen.initialState(model, 10));

        type(":" + "9".repeat(25));
        app.dispatchKey(plainKey(KeyCode.ENTER));

        assertThat(app.jumpPrompt().input()).isEqualTo("9".repeat(18));
        assertThat(app.jumpPrompt().error()).isEqualTo("Row 999,999,999,999,999,999 is outside 0–9,999");
    }

    @Test
    void jumpPromptSwallowsNavigationKeys() {
        // A PgDn that reached the table would move the view out from under
        // the target being typed.
        app.stack().push(DataPreviewScreen.initialState(model, 10));

        type(":43");
        DiveApp.Action a = app.dispatchKey(plainKey(KeyCode.PAGE_DOWN));

        assertThat(a).isEqualTo(DiveApp.Action.HANDLED);
        assertThat(((ScreenState.DataPreview) app.stack().top()).firstRow()).isZero();
        assertThat(app.jumpPrompt().input()).isEqualTo("43");
    }

    @Test
    void qTypedInJumpPromptDoesNotQuit() {
        // Goes through dispatchKey: the global `q` check runs there, before
        // any screen sees the key.
        app.stack().push(DataPreviewScreen.initialState(model, 10));
        app.dispatchKey(charKey(':'));

        DiveApp.Action a = app.dispatchKey(charKey('q'));

        assertThat(a).isEqualTo(DiveApp.Action.HANDLED);
        assertThat(app.jumpPrompt()).as("prompt stayed open").isNotNull();
    }

    @Test
    void colonInsideTheRecordModalIsLeftToTheModal() {
        // The record modal owns the keys while it is open; a prompt opening
        // over it would jump the page out from under the record on show.
        app.stack().push(DataPreviewScreen.initialState(model, 10));
        app.dispatchKey(plainKey(KeyCode.ENTER));

        app.dispatchKey(charKey(':'));

        assertThat(app.jumpPrompt()).as("no prompt over the modal").isNull();
        assertThat(((ScreenState.DataPreview) app.stack().top()).modalRow()).as("modal still open").isZero();
    }

    @Test
    void colonOnRowGroupsSelectsTheTypedGroup() throws Exception {
        Path file = Path.of(getClass().getResource("/filter_pushdown_int.parquet").getPath());
        try (ParquetModel rowGroups = ParquetModel.open(InputFile.of(file), file.toString())) {
            DiveApp rgApp = new DiveApp(rowGroups);
            rgApp.stack().push(new ScreenState.RowGroups(0));

            rgApp.dispatchKey(charKey(':'));
            rgApp.dispatchKey(charKey('2'));
            rgApp.dispatchKey(plainKey(KeyCode.ENTER));

            assertThat(rgApp.jumpPrompt()).isNull();
            assertThat(((ScreenState.RowGroups) rgApp.stack().top()).selection()).isEqualTo(2);
        }
    }

    @Test
    void colonDoesNothingOnTheDataPreviewOfAFileWithoutRows(@TempDir Path tempDir) throws Exception {
        try (ParquetModel empty = openFileWithoutRows(tempDir)) {
            DiveApp emptyApp = new DiveApp(empty);
            emptyApp.stack().push(DataPreviewScreen.initialState(empty, 20));

            emptyApp.dispatchKey(charKey(':'));

            assertThat(emptyApp.jumpPrompt()).isNull();
        }
    }

    @Test
    void colonDoesNothingOnTheRowGroupsOfAFileWithoutRowGroups(@TempDir Path tempDir) throws Exception {
        try (ParquetModel empty = openFileWithoutRows(tempDir)) {
            assertThat(empty.rowGroupCount()).isZero();
            DiveApp emptyApp = new DiveApp(empty);
            emptyApp.stack().push(new ScreenState.RowGroups(0));

            emptyApp.dispatchKey(charKey(':'));

            assertThat(emptyApp.jumpPrompt()).isNull();
        }
    }

    @Test
    void jumpPromptShowsWhatWasTypedWithoutTakingTableRows() {
        app.stack().push(DataPreviewScreen.initialState(model, 5));
        app.renderOnce(buffer());
        ScreenState.DataPreview before = (ScreenState.DataPreview) app.stack().top();
        type(":");
        app.renderOnce(buffer());

        type("7");
        app.renderOnce(buffer());

        assertThat(frameText()).as("the typed target is on screen").contains(": 7");
        assertThat(frameText()).as("the box says what it is").contains("Jump to");
        assertThat(frameText()).as("the prompt says what it accepts").contains("a row");
        assertThat(frameText()).as("the box says how to leave it").contains("Esc cancel");
        assertThat(app.stack().top()).as("prompt leaves the table unchanged").isEqualTo(before);
    }

    @Test
    void jumpPromptShowsWhyARefusedTargetDidNotMove() {
        app.stack().push(DataPreviewScreen.initialState(model, 5));

        type(":99999");
        app.dispatchKey(plainKey(KeyCode.ENTER));
        app.renderOnce(buffer());

        assertThat(frameText())
                .as("the reason is on screen, beside the text that caused it")
                .contains("Row 99,999 is outside 0–9,999");
        assertThat(frameText()).as("the typed target is kept").contains(": 99999");
    }

    @Test
    void colonOnRowGroupsPastTheLastIsRefused() throws Exception {
        Path file = Path.of(getClass().getResource("/filter_pushdown_int.parquet").getPath());
        try (ParquetModel rowGroups = ParquetModel.open(InputFile.of(file), file.toString())) {
            DiveApp rgApp = new DiveApp(rowGroups);
            rgApp.stack().push(new ScreenState.RowGroups(0));

            rgApp.dispatchKey(charKey(':'));
            rgApp.dispatchKey(charKey('3'));
            rgApp.dispatchKey(plainKey(KeyCode.ENTER));

            assertThat(rgApp.jumpPrompt().error()).isEqualTo("Row group 3 is outside 0–2");
            assertThat(((ScreenState.RowGroups) rgApp.stack().top()).selection()).isZero();
        }
    }

    private static ParquetModel openFileWithoutRows(Path tempDir) throws Exception {
        Path file = tempDir.resolve("no_rows.parquet");
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        try (ParquetFileWriter ignored = ParquetFileWriter.create(OutputFile.of(file), schema)) {
        }
        return ParquetModel.open(InputFile.of(file), file.toString());
    }

    private void type(String text) {
        for (char c : text.toCharArray()) {
            app.dispatchKey(charKey(c));
        }
    }

    private static final Rect AREA = new Rect(0, 0, 90, 24);

    private Buffer lastBuffer;

    private Buffer buffer() {
        lastBuffer = Buffer.empty(AREA);
        return lastBuffer;
    }

    private String frameText() {
        StringBuilder sb = new StringBuilder();
        for (int y = 0; y < AREA.height(); y++) {
            for (int x = 0; x < AREA.width(); x++) {
                Cell c = lastBuffer.get(x, y);
                String sym = c.symbol();
                sb.append(sym == null || sym.isEmpty() ? " " : sym);
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private static KeyEvent charKey(char c) {
        return new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, c);
    }

    private static KeyEvent plainKey(KeyCode code) {
        return new KeyEvent(code, KeyModifiers.NONE, '\0');
    }
}
