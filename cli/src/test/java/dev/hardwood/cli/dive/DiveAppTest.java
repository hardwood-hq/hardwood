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

import dev.hardwood.InputFile;
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
        dev.hardwood.cli.dive.internal.Keys.resetObservedViewport();
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

    private static KeyEvent charKey(char c) {
        return new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, c);
    }

    private static KeyEvent plainKey(KeyCode code) {
        return new KeyEvent(code, KeyModifiers.NONE, '\0');
    }
}
