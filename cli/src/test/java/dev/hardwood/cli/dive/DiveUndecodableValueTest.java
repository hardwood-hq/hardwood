/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.cli.TestResources;
import dev.hardwood.cli.dive.internal.DataPreviewScreen;
import dev.hardwood.cli.dive.internal.Keys;
import dev.hardwood.cli.dive.internal.Theme;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Style;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.tui.event.KeyModifiers;

import static org.assertj.core.api.Assertions.assertThat;

/// The data preview over a file whose first row stores, in each annotated column, a value its
/// annotation does not admit: an `INT(8)` of 300, a `UINT(8)` of 4294967295 and a `TIME(MILLIS)`
/// of a full day. Each such cell shows a marker styled as an error; the rest of the row and the
/// next row show as usual.
class DiveUndecodableValueTest {

    private static final Rect AREA = new Rect(0, 0, 160, 20);

    private static final String I8 = "<300: outside INT_8 range>";
    private static final String U8 = "<4294967295: outside UINT_8 range>";
    private static final String TIME = "<86400000: outside a day>";

    private ParquetModel model;

    @BeforeEach
    void setUp() throws Exception {
        Keys.resetObservedGeometry();
        Path path = TestResources.path("/undecodable_values.parquet");
        model = ParquetModel.open(InputFile.of(path), path.toString());
    }

    @AfterEach
    void tearDown() throws Exception {
        model.close();
    }

    @Test
    void previewRendersAMarkerForEachRefusedValue() {
        ScreenState.DataPreview state = DataPreviewScreen.initialState(model);

        assertThat(state.rows()).containsExactly(
                List.of("1", I8, U8, TIME),
                List.of("2", "5", "7", "00:00:01"));
        assertThat(state.undecodableCells()).containsExactly(Set.of(1, 2, 3), Set.of());

        Buffer buffer = Buffer.empty(AREA);
        DataPreviewScreen.render(buffer, AREA, model, state);

        // A cell is cut at 32 cells, so the start of each marker identifies it on screen
        for (String marker : List.of("<300: ", "<4294967295: ", "<86400000: ")) {
            assertThat(styleOf(buffer, marker).fg()).isEqualTo(Theme.error().fg());
        }
        // The cursor is on the first row: the marker keeps the error colour, `▶` and the row's
        // other cells the cursor's, and the next row the default foreground.
        Style cursor = styleOf(buffer, "▶ 0");
        assertThat(cursor.fg()).isEqualTo(Theme.selection().fg());
        assertThat(cursor.effectiveModifiers()).isEqualTo(Theme.selection().effectiveModifiers());
        assertThat(styleOf(buffer, "▶ 0 1", 4).fg()).isEqualTo(Theme.selection().fg());
        assertThat(styleOf(buffer, "00:00:01").fg()).isEqualTo(Style.EMPTY.fg());
    }

    /// The record modal shows the marker in its expanded form too, styled as an error.
    @Test
    void recordModalRendersTheMarker() {
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model);
        ScreenState.DataPreview state = new ScreenState.DataPreview(initial.firstRow(), initial.pageSize(),
                initial.columnNames(), initial.rows(), initial.expandedRows(), 0, 0, 0, true, Set.of(), 0, 0,
                initial.undecodableCells(), initial.expandedUndecodableCells());

        Buffer buffer = Buffer.empty(AREA);
        DataPreviewScreen.render(buffer, AREA, model, state);

        assertThat(initial.expandedRows().get(0)).containsExactly("1", I8, U8, TIME);
        assertThat(styleOf(buffer, U8).fg()).isEqualTo(Theme.error().fg());
    }

    /// The physical toggle shows stored values as stored, with no marker.
    @Test
    void physicalValuesCarryNoMarker() {
        NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
        stack.push(DataPreviewScreen.initialState(model));

        DataPreviewScreen.handle(new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 't'), model, stack);

        ScreenState.DataPreview physical = (ScreenState.DataPreview) stack.top();
        assertThat(physical.logicalTypes()).isFalse();
        assertThat(physical.rows()).containsExactly(
                List.of("1", "300", "-1", "86400000"),
                List.of("2", "5", "7", "1000"));
        assertThat(physical.undecodableCells()).containsExactly(Set.of(), Set.of());
    }

    /// A shredded Variant whose `INT(8)` `typed_value` lies outside its range shows as a marker in
    /// place of the whole Variant, as does a refused value inside a struct, list or map.
    @Test
    void nestedValuesRenderTheirMarkers() throws Exception {
        Path path = TestResources.path("/undecodable_nested_values.parquet");
        try (ParquetModel nested = ParquetModel.open(InputFile.of(path), path.toString())) {
            ScreenState.DataPreview state = DataPreviewScreen.initialState(nested);

            assertThat(state.rows().get(0)).containsExactly("1", "{ i8 : <300: outside INT_8 range> }",
                    "[1, 2, 3, …+1]",
                    "{ <300: outside INT_8 range> : <86400000: outside a day> }",
                    "<variant: 300 outside INT_8 range>");
            assertThat(state.rows().get(1)).containsExactly("2", "{ i8 : 5 }", "[5]", "{ 5 : 00:00:01 }", "5");
            // The list's refused element is its fourth, which the preview cuts: only its expanded
            // form shows the marker, and only that carries the flag
            assertThat(state.undecodableCells()).containsExactly(Set.of(1, 3, 4), Set.of());
            assertThat(state.expandedUndecodableCells()).containsExactly(Set.of(1, 2, 3, 4), Set.of());

            // The physical toggle reads the Variant's stored fields, which reassemble it as well
            NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
            stack.push(state);
            DataPreviewScreen.handle(new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 't'), nested, stack);
            ScreenState.DataPreview physical = (ScreenState.DataPreview) stack.top();
            assertThat(physical.rows().get(0).get(4)).isEqualTo("<variant: 300 outside INT_8 range>");
            assertThat(physical.undecodableCells()).containsExactly(Set.of(4), Set.of());
        }
    }

    /// The record modal expands a field in full, so the list element the preview cuts shows there
    /// as a marker styled as an error.
    @Test
    void recordModalStylesAMarkerThePreviewCuts() throws Exception {
        Path path = TestResources.path("/undecodable_nested_values.parquet");
        try (ParquetModel nested = ParquetModel.open(InputFile.of(path), path.toString())) {
            ScreenState.DataPreview initial = DataPreviewScreen.initialState(nested);
            ScreenState.DataPreview state = new ScreenState.DataPreview(initial.firstRow(), initial.pageSize(),
                    initial.columnNames(), initial.rows(), initial.expandedRows(), 0, 0, 0, true, Set.of(2), 0, 0,
                    initial.undecodableCells(), initial.expandedUndecodableCells());

            Buffer buffer = Buffer.empty(AREA);
            DataPreviewScreen.render(buffer, AREA, nested, state);

            // The list expands one element per line; the fourth is the marker, and the whole
            // field takes the error colour
            assertThat(styleOf(buffer, "│          <300: ", 11).fg()).isEqualTo(Theme.error().fg());
            assertThat(styleOf(buffer, "│          3,", 11).fg()).isEqualTo(Theme.error().fg());
            assertThat(styleOf(buffer, "│  var : ", 9).fg()).isEqualTo(Theme.error().fg());
        }
    }

    /// Under the modal cursor a refused value keeps the error colour, and its key the cursor's.
    @Test
    void recordModalCursorKeepsTheErrorColour() throws Exception {
        Path path = TestResources.path("/undecodable_nested_values.parquet");
        try (ParquetModel nested = ParquetModel.open(InputFile.of(path), path.toString())) {
            ScreenState.DataPreview initial = DataPreviewScreen.initialState(nested);

            // Collapsed, line 1 is the struct `s`, whose preview holds a marker
            Buffer buffer = renderModal(nested, initial, Set.of(), 1);
            assertThat(styleOf(buffer, "│▶ s   : ", 3).fg()).isEqualTo(Theme.selection().fg());
            assertThat(styleOf(buffer, "│▶ s   : ", 9).fg()).isEqualTo(Theme.error().fg());

            // Expanded, line 2 opens the list `l` and line 6 is its marker element
            buffer = renderModal(nested, initial, Set.of(2), 2);
            assertThat(styleOf(buffer, "│▶ l   : ", 3).fg()).isEqualTo(Theme.selection().fg());
            assertThat(styleOf(buffer, "│▶ l   : ", 9).fg()).isEqualTo(Theme.error().fg());
            buffer = renderModal(nested, initial, Set.of(2), 6);
            assertThat(styleOf(buffer, "│          <300: ", 11).fg()).isEqualTo(Theme.error().fg());
        }
    }

    private static Buffer renderModal(ParquetModel model, ScreenState.DataPreview initial, Set<Integer> expanded,
                                      int cursorLine) {
        ScreenState.DataPreview state = new ScreenState.DataPreview(initial.firstRow(), initial.pageSize(),
                initial.columnNames(), initial.rows(), initial.expandedRows(), 0, 0, 0, true, expanded, cursorLine,
                0, initial.undecodableCells(), initial.expandedUndecodableCells());
        Buffer buffer = Buffer.empty(AREA);
        DataPreviewScreen.render(buffer, AREA, model, state);
        return buffer;
    }

    /// The style of the first cell of the first occurrence of `text` on screen.
    private static Style styleOf(Buffer buffer, String text) {
        return styleOf(buffer, text, 0);
    }

    /// The style of the cell `offset` cells into the first occurrence of `text` on screen.
    private static Style styleOf(Buffer buffer, String text, int offset) {
        Rect area = buffer.area();
        for (int y = area.top(); y < area.bottom(); y++) {
            StringBuilder line = new StringBuilder();
            for (int x = area.left(); x < area.right(); x++) {
                String symbol = buffer.get(x, y).symbol();
                line.append(symbol == null || symbol.isEmpty() ? " " : symbol);
            }
            int at = line.indexOf(text);
            if (at >= 0) {
                return buffer.get(area.left() + at + offset, y).style();
            }
        }
        throw new AssertionError("'" + text + "' is not on screen");
    }
}
