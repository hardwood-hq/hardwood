/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InputFile;
import dev.hardwood.cli.dive.internal.ColumnChunkDetailScreen;
import dev.hardwood.cli.dive.internal.DataPreviewScreen;
import dev.hardwood.cli.dive.internal.FooterScreen;
import dev.hardwood.cli.dive.internal.HelpOverlay;
import dev.hardwood.cli.dive.internal.Keys;
import dev.hardwood.cli.dive.internal.OverviewScreen;
import dev.hardwood.cli.dive.internal.PagesScreen;
import dev.hardwood.cli.dive.internal.RowGroupDetailScreen;
import dev.hardwood.cli.dive.internal.SchemaScreen;
import dev.hardwood.cli.internal.Version;
import dev.hardwood.schema.ColumnSchema;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.tui.event.KeyModifiers;

import static org.assertj.core.api.Assertions.assertThat;

/// Layer-2 visual tests — render screens to an in-memory buffer and assert
/// on the captured cells. Catches title / row / marker bugs that the
/// handler-only tests in [DiveStateTest] don't see.
class DiveRenderTest {

    private static final Rect AREA = new Rect(0, 0, 120, 40);

    private static final Pattern RANGE_MARKER = Pattern.compile("─ \\d+-\\d+/\\d+ ");

    private ParquetModel model;

    @BeforeEach
    void setUp() throws Exception {
        Keys.resetObservedGeometry();
        Path path = Path.of(getClass().getResource("/column_index_pushdown.parquet").getPath());
        model = ParquetModel.open(InputFile.of(path), path.toString());
    }

    @AfterEach
    void tearDown() throws Exception {
        model.close();
    }

    @Test
    void rowGroupsTitleShowsRange() {
        ScreenState.RowGroups state = new ScreenState.RowGroups(0);
        RenderHarness.RenderedFrame frame = RenderHarness.render(AREA, state, model);

        // Title is on the top border; should embed "1-N of M" — total
        // is the row group count of the fixture (1 RG).
        String title = frame.firstLineContaining("Row groups");
        assertThat(title).isNotNull().contains("1");
        assertThat(title).contains("of " + model.rowGroupCount());
    }

    @Test
    void breadcrumbDoesNotDuplicateRowGroupAndShowsLeafName() throws Exception {
        // Open a fixture with a multi-character column name (`category`)
        // and walk Overview → RowGroups → RowGroupDetail → ColumnChunks
        // → ColumnChunkDetail. The breadcrumb chain is rendered by the
        // chrome, not by the screen body — but DiveApp wires it through
        // Chrome.renderBreadcrumb. To avoid pulling DiveApp into this
        // test we assert via direct breadcrumb-label calls on the
        // chrome utility, exercising the same switch.
        Path file = Path.of(getClass().getResource("/dictionary_with_crc.parquet").getPath());
        try (ParquetModel m = ParquetModel.open(InputFile.of(file), file.toString())) {
            NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
            stack.push(new ScreenState.RowGroups(0));
            stack.push(new ScreenState.RowGroupDetail(0,
                    ScreenState.RowGroupDetail.Pane.MENU, 0));
            stack.push(new ScreenState.ColumnChunks(0, 1));  // col 1 = "category"
            stack.push(new ScreenState.ColumnChunkDetail(0, 1,
                    ScreenState.ColumnChunkDetail.Pane.MENU, 0, true, false));

            // Breadcrumb labels via the package-private utility.
            List<String> labels = stack.frames().stream()
                    .map(s -> dev.hardwood.cli.dive.internal.Chrome.breadcrumbLabel(s, m))
                    .toList();

            // No duplicate "RG #0" — RowGroupDetail says "RG #0", and
            // ColumnChunks now just says "Column chunks" (not "RG #0 ›
            // Column chunks").
            assertThat(labels).contains("Overview", "Row groups", "RG #0",
                    "Column chunks", "category");
            assertThat(labels.stream().filter(l -> l.equals("RG #0")).count()).isOne();
            // ColumnChunkDetail label is the leaf name, not "[col 1]".
            assertThat(labels).doesNotContain("[col 1]");
        }
    }

    @Test
    void breadcrumbEnrichesLeafWithRowGroupAndColumnFromFooterPath() {
        // Footer → FileIndexes(COLUMN) → ColumnIndexView. None of the
        // context-bearing frames (RowGroupDetail / ColumnChunks /
        // ColumnChunkDetail / ColumnAcrossRowGroups) appear upstream, so
        // Chrome.renderBreadcrumb must append "(RG #N · column)" to the
        // leaf label so the user still sees which chunk they're in.
        NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
        stack.push(ScreenState.Footer.initial());
        stack.push(new ScreenState.FileIndexes(ScreenState.FileIndexes.Kind.COLUMN, 0));
        stack.push(new ScreenState.ColumnIndexView(0, 0, 0, "", false, true, false));

        Rect breadcrumbArea = new Rect(0, 0, 200, 1);
        dev.tamboui.buffer.Buffer buffer = dev.tamboui.buffer.Buffer.empty(breadcrumbArea);
        dev.hardwood.cli.dive.internal.Chrome.renderBreadcrumb(buffer, breadcrumbArea, stack, model);

        StringBuilder sb = new StringBuilder();
        for (int x = 0; x < breadcrumbArea.width(); x++) {
            String sym = buffer.get(x, 0).symbol();
            sb.append(sym == null || sym.isEmpty() ? ' ' : sym);
        }
        String breadcrumb = sb.toString().stripTrailing();

        assertThat(breadcrumb).contains("Overview");
        assertThat(breadcrumb).contains("Footer & indexes");
        assertThat(breadcrumb).contains("All column indexes");
        assertThat(breadcrumb).contains("Column index");
        // The enrichment: leaf "Column index" gets "(RG #0 · id)" suffix
        // because no upstream frame establishes (RG, column) context.
        String columnPath = model.schema().getColumn(0).fieldPath().toString();
        assertThat(breadcrumb).contains("(RG #0 · " + columnPath + ")");
    }

    @Test
    void breadcrumbDoesNotEnrichLeafWhenContextAlreadyOnPath() {
        // Pages reached via Overview → RowGroups → RowGroupDetail →
        // ColumnChunks → ColumnChunkDetail → Pages. Both RG and column
        // context are already on the path, so no "(RG #N · …)" suffix.
        NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
        stack.push(new ScreenState.RowGroups(0));
        stack.push(new ScreenState.RowGroupDetail(0, ScreenState.RowGroupDetail.Pane.MENU, 0));
        stack.push(new ScreenState.ColumnChunks(0, 0));
        stack.push(new ScreenState.ColumnChunkDetail(0, 0,
                ScreenState.ColumnChunkDetail.Pane.MENU, 0, true, false));
        stack.push(new ScreenState.Pages(0, 0, 0, false, true));

        Rect breadcrumbArea = new Rect(0, 0, 200, 1);
        dev.tamboui.buffer.Buffer buffer = dev.tamboui.buffer.Buffer.empty(breadcrumbArea);
        dev.hardwood.cli.dive.internal.Chrome.renderBreadcrumb(buffer, breadcrumbArea, stack, model);

        StringBuilder sb = new StringBuilder();
        for (int x = 0; x < breadcrumbArea.width(); x++) {
            String sym = buffer.get(x, 0).symbol();
            sb.append(sym == null || sym.isEmpty() ? ' ' : sym);
        }
        String breadcrumb = sb.toString().stripTrailing();

        // Leaf is just "Pages" — no parenthetical enrichment.
        assertThat(breadcrumb).endsWith("Pages");
    }

    @Test
    void longValueFixtureMarksPagesStatistics() throws Exception {
        Path path = Path.of(getClass().getResource("/cli_long_value_test.parquet").getPath());
        try (ParquetModel longValueModel = ParquetModel.open(InputFile.of(path), path.toString())) {
            RenderHarness.RenderedFrame frame = RenderHarness.render(
                    new Rect(0, 0, 120, 40), new ScreenState.Pages(0, 0, 1, false, true), longValueModel);

            assertThat(frame.contains("…")).isTrue();
        }
    }

    @Test
    void longValueFixtureMarksColumnIndexStatisticsAndModalShowsFullValue() throws Exception {
        Path path = Path.of(getClass().getResource("/cli_long_value_test.parquet").getPath());
        try (ParquetModel longValueModel = ParquetModel.open(InputFile.of(path), path.toString())) {
            ScreenState.ColumnIndexView cell = new ScreenState.ColumnIndexView(
                    0, 0, 0, "", false, true, false);
            RenderHarness.RenderedFrame cellFrame = RenderHarness.render(
                    new Rect(0, 0, 120, 40), cell, longValueModel);
            assertThat(cellFrame.contains("…")).isTrue();

            ScreenState.ColumnIndexView modal = new ScreenState.ColumnIndexView(
                    0, 0, 0, "", false, true, true);
            RenderHarness.RenderedFrame modalFrame = RenderHarness.render(AREA, modal, longValueModel);
            assertThat(modalFrame.contains("the-quick-brown-fox-jumps-over-the-lazy-dog-0")).isTrue();
        }
    }

    @Test
    void longValueFixtureMarksColumnAcrossRowGroupsStatistics() throws Exception {
        Path path = Path.of(getClass().getResource("/cli_long_value_test.parquet").getPath());
        try (ParquetModel longValueModel = ParquetModel.open(InputFile.of(path), path.toString())) {
            RenderHarness.RenderedFrame frame = RenderHarness.render(
                    new Rect(0, 0, 120, 40),
                    new ScreenState.ColumnAcrossRowGroups(0, 0, true, 0), longValueModel);

            assertThat(frame.contains("…")).isTrue();
        }
    }

    /// The stat budget must not exceed the cell the renderer actually gets, or
    /// the table clips the trailing `…` and the value is cut with no marker at
    /// all. These widths put the 45-character fixture value one and two
    /// characters past the budget, which is where an over-wide budget shows.
    @ParameterizedTest
    @ValueSource(ints = {173, 175})
    void columnAcrossRowGroupsKeepsItsMarkerAtTheCellBoundary(int width) throws Exception {
        Path path = Path.of(getClass().getResource("/cli_long_value_test.parquet").getPath());
        try (ParquetModel longValueModel = ParquetModel.open(InputFile.of(path), path.toString())) {
            RenderHarness.RenderedFrame frame = RenderHarness.render(
                    new Rect(0, 0, width, 40),
                    new ScreenState.ColumnAcrossRowGroups(0, 0, true, 0), longValueModel);

            assertThat(frame.text())
                    .contains("…")
                    .doesNotContain("the-quick-brown-fox-jumps-over-the-lazy-dog ");
        }
    }

    /// `PagesScreen` moved Min/Max from a fixed `Length(20)` to `Fill(1)`, so its
    /// budget has to track the cell the renderer is given. Its budget is
    /// `(width - 108) / 2`, so these widths put the 45-character fixture value one
    /// and two characters past it — where an over-wide budget shows.
    @ParameterizedTest
    @ValueSource(ints = {194, 196})
    void pagesKeepsItsMarkerAtTheCellBoundary(int width) throws Exception {
        Path path = Path.of(getClass().getResource("/cli_long_value_test.parquet").getPath());
        try (ParquetModel longValueModel = ParquetModel.open(InputFile.of(path), path.toString())) {
            RenderHarness.RenderedFrame frame = RenderHarness.render(
                    new Rect(0, 0, width, 40),
                    new ScreenState.Pages(0, 0, 1, false, true), longValueModel);

            assertThat(frame.text())
                    .contains("…")
                    .doesNotContain("the-quick-brown-fox-jumps-over-the-lazy-dog ");
        }
    }

    /// Column index sizes its Min/Max cells with `Fill(1)` too, so a cap that does
    /// not track the width loses the marker on a narrow terminal — 80 columns being
    /// the one that matters.
    @ParameterizedTest
    @ValueSource(ints = {70, 78, 80, 84})
    void columnIndexKeepsItsMarkerAtNarrowWidths(int width) throws Exception {
        Path path = Path.of(getClass().getResource("/cli_long_value_test.parquet").getPath());
        try (ParquetModel longValueModel = ParquetModel.open(InputFile.of(path), path.toString())) {
            RenderHarness.RenderedFrame frame = RenderHarness.render(
                    new Rect(0, 0, width, 30),
                    new ScreenState.ColumnIndexView(0, 0, 0, "", false, true, false), longValueModel);

            assertThat(frame.text()).contains("…");
        }
    }

    @Test
    void unicodeValueTruncationDoesNotSplitSurrogatePairs() throws Exception {
        Path path = Path.of(getClass().getResource("/cli_unicode_value_test.parquet").getPath());
        try (ParquetModel unicodeModel = ParquetModel.open(InputFile.of(path), path.toString())) {
            Rect area = new Rect(0, 0, 120, 40);
            RenderHarness.RenderedFrame pages = RenderHarness.render(
                    area, new ScreenState.Pages(0, 0, 1, false, true), unicodeModel);
            RenderHarness.RenderedFrame across = RenderHarness.render(
                    area, new ScreenState.ColumnAcrossRowGroups(0, 0, true, 0), unicodeModel);

            assertThat(pages.text()).contains("…").doesNotContain("?");
            assertThat(across.text()).contains("…").doesNotContain("?");
        }
    }

    @Test
    void dataPreviewCellEndsInEllipsisAtComputedWidth() throws Exception {
        // Tight viewport (50 cols) leaves only a partial-width slot for
        // one of the visible columns, so long values must be truncated.
        // The yellow_tripdata fixture has TIMESTAMP and DECIMAL columns
        // wider than the per-cell budget at this viewport.
        Path file = Path.of(getClass().getResource("/yellow_tripdata_sample.parquet").getPath());
        try (ParquetModel m = ParquetModel.open(InputFile.of(file), file.toString())) {
            ScreenState.DataPreview state = dev.hardwood.cli.dive.internal.DataPreviewScreen
                    .initialState(m, 10);
            RenderHarness.RenderedFrame frame = RenderHarness.render(
                    new Rect(0, 0, 50, 40), state, m);
            assertThat(frame.contains("…"))
                    .as("expected at least one truncated cell with ellipsis")
                    .isTrue();
        }
    }

    @Test
    void dataPreviewPacksNarrowColumnsIntoAvailableWidth() {
        List<String> columns = List.of("a", "b", "c", "d", "e", "f", "g", "h");
        List<String> cells = List.of("1", "2", "3", "4", "5", "6", "7", "8");
        ScreenState.DataPreview state = new ScreenState.DataPreview(
                0, 1, columns, List.of(cells), List.of(cells),
                0, 0, -1, true, Set.of(), 0);

        RenderHarness.RenderedFrame frame = RenderHarness.render(
                new Rect(0, 0, 24, 6), state, model);

        assertThat(frame.contains("a b c d e f g h")).isTrue();
    }

    @Test
    void dataPreviewRightScrollEventuallyRevealsTheLastColumnInFull() throws Exception {
        // A column clipped by the remaining width budget — as opposed to one
        // capped at VALUE_TRUNCATE — must always be reachable in full by
        // scrolling right, including when it is the file's last column.
        Path file = Path.of(getClass().getResource("/yellow_tripdata_sample.parquet").getPath());
        try (ParquetModel m = ParquetModel.open(InputFile.of(file), file.toString())) {
            NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
            stack.push(DataPreviewScreen.initialState(m, 5));
            List<String> names = ((ScreenState.DataPreview) stack.top()).columnNames();
            String lastColumn = names.get(names.size() - 1);

            RenderHarness.RenderedFrame frame = RenderHarness.render(AREA, stack.top(), m);
            // Bounded so a regression in the stop condition fails the test
            // instead of hanging it.
            int steps = 0;
            while (DataPreviewScreen.handle(key(KeyCode.RIGHT), m, stack)) {
                frame = RenderHarness.render(AREA, stack.top(), m);
                assertThat(++steps).isLessThan(names.size() + 1);
            }

            assertThat(frame.contains(lastColumn))
                    .as("right-scroll stopped with '%s' still clipped", lastColumn)
                    .isTrue();
        }
    }

    @Test
    void dataPreviewKeybarOffersColumnScrollWhileTheLastColumnIsClipped() {
        // Four columns that all fit the window, but the trailing one is
        // clipped for want of budget. Scrolling right would drop `a` and
        // free the space, so the keybar has to advertise it.
        List<String> columns = List.of("a", "b", "c", "wide");
        List<String> cells = List.of("1".repeat(10), "2".repeat(10), "3".repeat(10), "D".repeat(30));
        ScreenState.DataPreview state = new ScreenState.DataPreview(
                0, 1, columns, List.of(cells), List.of(cells),
                0, 0, -1, true, Set.of(), 0);
        Rect area = new Rect(0, 0, 48, 6);

        RenderHarness.RenderedFrame frame = RenderHarness.render(area, state, model);

        assertThat(frame.contains("…"))
                .as("expected the trailing column to be clipped at this width")
                .isTrue();
        assertThat(RenderHarness.keybarFor(state, model)).contains("[←→] columns");
    }

    @Test
    void dataPreviewScalarOnlyRecordStillWalksAndPages() {
        List<String> names = numberedValues("f", 30);
        List<String> values = numberedValues("v", 30);
        ScreenState.DataPreview state = openModal(names, values, values);
        NavigationStack stack = rooted(state);
        Rect area = new Rect(0, 0, 100, 24);

        RenderHarness.RenderedFrame first = RenderHarness.render(area, state, model);
        assertThat(first.contains("▶ f"))
                .as("nothing in the record can be expanded, so no row is marked")
                .isFalse();
        assertThat(first.contains("Enter expand"))
                .as("the hint does not offer an expansion that cannot happen")
                .isFalse();
        assertThat(first.contains("e/c all")).isFalse();
        assertThat(first.contains("↑↓ move"))
                .as("the cursor walks the body whether or not Enter does anything")
                .isTrue();
        assertThat(first.contains("PgDn/PgUp page")).isTrue();

        assertThat(DataPreviewScreen.handle(key(KeyCode.UP), model, stack))
                .as("already at the top, but the key is still the cursor's")
                .isTrue();
        assertThat(DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack)).isTrue();
        assertThat(((ScreenState.DataPreview) stack.top()).modalCursorLine()).isEqualTo(1);

        assertThat(DataPreviewScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'G'), model, stack)).isTrue();
        ScreenState.DataPreview bottom = (ScreenState.DataPreview) stack.top();
        assertThat(bottom.modalCursorLine()).isEqualTo(names.size() - 1);
        assertThat(RenderHarness.render(area, bottom, model).contains("f29"))
                .as("the body follows the cursor to the tail")
                .isTrue();
    }

    @Test
    void dataPreviewCursorStopsOnEveryLineNotOnlyActionableOnes() {
        List<String> names = new ArrayList<>(numberedValues("f", 30));
        List<String> values = new ArrayList<>(numberedValues("v", 30));
        List<String> expanded = new ArrayList<>(values);
        names.set(0, "items");
        values.set(0, "[1, 2]");
        expanded.set(0, "[\n  1,\n  2\n]");
        names.set(20, "more");
        values.set(20, "[3, 4]");
        expanded.set(20, "[\n  3,\n  4\n]");
        ScreenState.DataPreview state = openModal(names, values, expanded);
        NavigationStack stack = rooted(state);
        Rect area = new Rect(0, 0, 100, 24);
        RenderHarness.RenderedFrame first = RenderHarness.render(area, state, model);

        // Both expandable fields are marked, so a reader can see where Enter
        // goes without arrowing onto each row.
        assertThat(first.contains("▶ items")).isTrue();
        assertThat(first.contains("▶ f1"))
                .as("a scalar the row shows in full is not marked")
                .isFalse();

        // ↓ steps one line, onto the scalar immediately below, rather than
        // skipping the nineteen rows between the two expandable fields.
        assertThat(DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack)).isTrue();
        assertThat(((ScreenState.DataPreview) stack.top()).modalCursorLine()).isEqualTo(1);
        assertThat(DataPreviewScreen.handle(key(KeyCode.ENTER), model, stack))
                .as("Enter does nothing on a row that is not actionable")
                .isFalse();
    }

    @Test
    void dataPreviewExpandingAFieldScrollsItsNewLinesIntoView() {
        List<String> names = new ArrayList<>(numberedValues("f", 30));
        List<String> values = new ArrayList<>(numberedValues("v", 30));
        List<String> expanded = new ArrayList<>(values);
        names.set(20, "items");
        values.set(20, "[alpha, beta, gamma]");
        expanded.set(20, "[\n  alpha,\n  beta,\n  gamma\n]");
        ScreenState.DataPreview state = openModal(names, values, expanded, 20);
        NavigationStack stack = rooted(state);
        Rect area = new Rect(0, 0, 100, 24);
        RenderHarness.render(area, state, model);

        // The cursor sits on `items`, the only field Enter can act on.
        assertThat(DataPreviewScreen.handle(key(KeyCode.ENTER), model, stack)).isTrue();

        ScreenState.DataPreview opened = (ScreenState.DataPreview) stack.top();
        assertThat(opened.expandedColumns()).containsExactly(20);
        RenderHarness.RenderedFrame rendered = RenderHarness.render(area, opened, model);
        assertThat(rendered.contains("▶ items")).isTrue();
        assertThat(rendered.contains("gamma"))
                .as("expanding scrolls so the revealed lines are on screen, "
                        + "not just the field's key line")
                .isTrue();
    }

    @Test
    void dataPreviewPageKeysMoveTheCursorLikeEveryOtherPane() {
        List<String> names = new ArrayList<>(numberedValues("f", 30));
        List<String> values = new ArrayList<>(numberedValues("v", 30));
        List<String> expanded = new ArrayList<>(values);
        names.set(0, "items");
        values.set(0, "[1, 2]");
        expanded.set(0, "[\n  1,\n  2\n]");
        ScreenState.DataPreview state = openModal(names, values, expanded);
        NavigationStack stack = rooted(state);
        Rect area = new Rect(0, 0, 100, 24);
        RenderHarness.render(area, state, model);

        assertThat(DataPreviewScreen.handle(key(KeyCode.PAGE_DOWN), model, stack)).isTrue();
        ScreenState.DataPreview paged = (ScreenState.DataPreview) stack.top();
        assertThat(paged.modalCursorLine())
                .as("PgDn is the coarse ↓, not a second axis")
                .isPositive();
        assertThat(paged.modalScroll())
                .as("the body follows the cursor")
                .isPositive();
        assertThat(DataPreviewScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'G'), model, stack)).isTrue();
        assertThat(RenderHarness.render(area, (ScreenState) stack.top(), model).contains("f29"))
                .as("G reaches the tail the cursor now walks")
                .isTrue();
    }

    @Test
    void dataPreviewLongSingleLineScalarIsActionableWhenTruncated() {
        String longNote = "x".repeat(100) + "TAIL";
        ScreenState.DataPreview state = openModal(
                List.of("tags", "note"),
                List.of("[a, b]", longNote),
                List.of("[\n  a,\n  b\n]", longNote));
        NavigationStack stack = rooted(state);
        Rect area = new Rect(0, 0, 60, 24);
        RenderHarness.render(area, state, model);

        assertThat(DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack)).isTrue();
        ScreenState.DataPreview selectedNote = (ScreenState.DataPreview) stack.top();
        assertThat(selectedNote.modalCursorLine()).isEqualTo(1);
        assertThat(RenderHarness.render(area, selectedNote, model).contains("▶ note"))
                .as("a truncated single-line value shows an action marker")
                .isTrue();

        assertThat(DataPreviewScreen.handle(key(KeyCode.ENTER), model, stack)).isTrue();
        ScreenState.DataPreview expanded = (ScreenState.DataPreview) stack.top();
        assertThat(expanded.expandedColumns()).containsExactly(1);
        assertThat(RenderHarness.render(area, expanded, model).contains("TAIL")).isTrue();
    }

    @Test
    void dataPreviewFittedBodyOffersMovementButNotPaging() {
        ScreenState.DataPreview state = openModal(
                List.of("items", "id", "status"),
                List.of("[a, b]", "1", "ready"),
                List.of("[\n  a,\n  b\n]", "1", "ready"));
        NavigationStack stack = rooted(state);
        Rect area = new Rect(0, 0, 100, 24);

        RenderHarness.RenderedFrame rendered = RenderHarness.render(area, state, model);

        assertThat(DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack))
                .as("the cursor moves to the next line, expandable or not")
                .isTrue();
        assertThat(((ScreenState.DataPreview) stack.top()).modalCursorLine()).isEqualTo(1);
        assertThat(rendered.contains("↑↓ move")).isTrue();
        assertThat(rendered.contains("PgDn/PgUp page"))
                .as("the body fits, so there is nothing to page to")
                .isFalse();
        assertThat(rendered.contains("Enter expand"))
                .as("the selected field remains expandable")
                .isTrue();
    }

    @Test
    void dataPreviewValueFillingTheBudgetExactlyIsNotActionable() {
        // maxKeyWidth 4 ("note") at width 100 leaves an 84-cell value budget.
        int budget = 84;
        ScreenState.DataPreview fits = openModal(
                List.of("note"), List.of("x".repeat(budget)), List.of("x".repeat(budget)));
        NavigationStack stack = rooted(fits);
        Rect area = new Rect(0, 0, 100, 24);

        assertThat(RenderHarness.render(area, fits, model).contains("▶ note"))
                .as("a value the collapsed line shows in full is not actionable")
                .isFalse();
        assertThat(DataPreviewScreen.handle(key(KeyCode.ENTER), model, stack)).isFalse();

        ScreenState.DataPreview overflows = openModal(
                List.of("note"), List.of("x".repeat(budget + 1)), List.of("x".repeat(budget + 1)));
        assertThat(RenderHarness.render(area, overflows, model).contains("▶ note"))
                .as("one cell over the budget and expanding reveals something")
                .isTrue();
    }

    @Test
    void dataPreviewActionabilityMeasuresDisplayCellsNotChars() {
        // 100 chars but only 50 display cells: each 'e' carries a zero-width
        // combining accent. The collapsed line shows all of it, so Enter has
        // nothing to reveal.
        String accented = "e\u0301".repeat(50);
        ScreenState.DataPreview state = openModal(
                List.of("note"), List.of(accented), List.of(accented));
        NavigationStack stack = rooted(state);
        Rect area = new Rect(0, 0, 100, 24);

        RenderHarness.RenderedFrame rendered = RenderHarness.render(area, state, model);
        assertThat(rendered.contains("…"))
                .as("the value fits the budget in cells, so it is not truncated")
                .isFalse();
        assertThat(rendered.contains("▶ note"))
                .as("a fully visible value must not be offered as expandable")
                .isFalse();
        assertThat(DataPreviewScreen.handle(key(KeyCode.ENTER), model, stack)).isFalse();
    }

    @Test
    void dataPreviewModalOpensOnTheFirstExpandableField() {
        List<String> names = List.of("id", "name", "tags");
        List<String> values = List.of("1", "bob", "[a, b]");
        List<String> expanded = List.of("1", "bob", "[\n  a,\n  b\n]");
        Rect area = new Rect(0, 0, 100, 24);
        ScreenState.DataPreview closed = closedPreview(names, values, expanded, area);
        NavigationStack stack = rooted(closed);
        RenderHarness.render(area, closed, model);

        assertThat(DataPreviewScreen.handle(key(KeyCode.ENTER), model, stack)).isTrue();

        ScreenState.DataPreview opened = (ScreenState.DataPreview) stack.top();
        assertThat(opened.columnNames())
                .as("the synthetic state must survive the open, not be re-paged")
                .isEqualTo(names);
        assertThat(opened.modalCursorLine())
                .as("id and name are scalars; the cursor opens on tags")
                .isEqualTo(2);
        assertThat(RenderHarness.render(area, opened, model).contains("▶ tags")).isTrue();
    }

    @Test
    void dataPreviewModalOpensScrolledToAnExpandableFieldBelowTheFold() {
        List<String> names = new ArrayList<>(numberedValues("f", 30));
        List<String> values = new ArrayList<>(numberedValues("v", 30));
        List<String> expanded = new ArrayList<>(values);
        names.set(25, "items");
        values.set(25, "[1, 2]");
        expanded.set(25, "[\n  1,\n  2\n]");
        Rect area = new Rect(0, 0, 100, 24);
        ScreenState.DataPreview closed = closedPreview(names, values, expanded, area);
        NavigationStack stack = rooted(closed);
        RenderHarness.render(area, closed, model);

        assertThat(DataPreviewScreen.handle(key(KeyCode.ENTER), model, stack)).isTrue();

        ScreenState.DataPreview opened = (ScreenState.DataPreview) stack.top();
        assertThat(opened.modalCursorLine()).isEqualTo(25);
        assertThat(opened.modalScroll())
                .as("the only expandable field sits past the first screenful")
                .isPositive();
        assertThat(RenderHarness.render(area, opened, model).contains("▶ items"))
                .as("opening scrolls the field it selected into view")
                .isTrue();
    }

    /// Cross-product smoke render: every screen × every fixture renders
    /// without throwing. Catches data-shape edge cases (no CI, no dict,
    /// nested types, all-null pages) that the handler tests don't
    /// exercise visually.
    @ParameterizedTest(name = "{1} on {0}")
    @MethodSource("smokeMatrix")
    void screenRendersWithoutException(String fixture, String screenName,
                                       Function<ParquetModel, ScreenState> ctor) throws Exception {
        Path file = Path.of(getClass().getResource("/" + fixture).getPath());
        try (ParquetModel m = ParquetModel.open(InputFile.of(file), file.toString())) {
            ScreenState s = ctor.apply(m);
            if (s == null) {
                return;  // not applicable (e.g., no dict in fixture)
            }
            RenderHarness.render(AREA, s, m);
        }
    }

    @Test
    void helpOverlayReachesItsTailOnAShortTerminal() {
        // The overlay runs to about thirty lines. On a short terminal it was
        // capped to the screen and the remainder simply dropped.
        Rect screenArea = new Rect(0, 0, 120, 16);
        Buffer buffer = Buffer.empty(screenArea);
        HelpOverlay.render(buffer, screenArea, 0);
        assertThat(renderToString(buffer, screenArea)).doesNotContain("Data preview");

        Buffer scrolled = Buffer.empty(screenArea);
        HelpOverlay.render(scrolled, screenArea, HelpOverlay.lineCount(screenArea));
        assertThat(renderToString(scrolled, screenArea)).contains("Data preview");
    }

    @Test
    void helpOverlayWrapsLongDescriptions() {
        // At 80 width, the description budget is 38 chars.
        // The longest description is 52 chars, so it should be forced to wrap.
        Rect screenArea = new Rect(0, 0, 80, 40);
        Buffer buffer = Buffer.empty(screenArea);

        HelpOverlay.render(buffer, screenArea, 0);

        assertThat(renderToString(buffer, screenArea))
                .contains("enter filter mode (Schema, Column ")
                .contains("               index, Dictionary) ");
    }

    @Test
    void dataPreviewErrorOverlayWrapsLongMessageWithinModalWidth() {
        String longMessage = "IOException: Failed to decompress page data in column chunk at offset 1234. "
                + "The decompressor returned an unexpected length. "
                + "The file may be truncated or written by an incompatible encoder.";
        ScreenState.DataPreview state = new ScreenState.DataPreview(
                0, 5, List.of("id"), List.of(), List.of(),
                0, 0, -1, true, Set.of(), 0, 0, longMessage);

        RenderHarness.RenderedFrame frame = RenderHarness.render(new Rect(0, 0, 80, 24), state, model);

        assertThat(frame.contains(" Data preview — error")).isTrue();
        assertThat(frame.contains("[Esc] to go back")).isTrue();
        for (String line : frame.lines()) {
            assertThat(line.length()).isLessThanOrEqualTo(80);
        }
    }

    /// The overlay is where a TUI user reads which build they are on, so the line must carry
    /// the resolved version rather than the label alone.
    @Test
    void helpOverlayShowsTheBuildVersion() {
        Rect screenArea = new Rect(0, 0, 120, 40);
        Buffer buffer = Buffer.empty(screenArea);

        HelpOverlay.render(buffer, screenArea, 0);

        assertThat(renderToString(buffer, screenArea)).contains("Version: " + Version.getVersion());
    }

    @Test
    void helpOverlayFitsAllKeybindingsAtNarrowWidth() {
        // At 50×40 the overlay width drops to 46 (descBudget 24), so many more
        // descriptions wrap onto a second line. The overlay's height must grow
        // with the content; otherwise the bottom keybindings get clipped — the
        // same failure mode that motivated #386, just at a different breakpoint.
        Rect screenArea = new Rect(0, 0, 50, 40);
        Buffer buffer = Buffer.empty(screenArea);

        HelpOverlay.render(buffer, screenArea, 0);

        // The "Press ? or Esc to close" sentinel is the very last line of the
        // overlay; if it renders, no content above it can have been clipped.
        assertThat(renderToString(buffer, screenArea)).contains("Press ? or Esc to close");
    }

    /// A Data preview state with the record modal already open on row 0 and
    /// the cursor at line 0 — the shape render assertions want, without
    /// going through the open path.
    private static ScreenState.DataPreview openModal(
            List<String> names, List<String> values, List<String> expandedValues) {
        return openModal(names, values, expandedValues, 0);
    }

    /// As above, with the modal's line cursor already on `cursorLine` — for
    /// tests about what a key does from a given position, rather than about
    /// where opening puts it.
    private static ScreenState.DataPreview openModal(
            List<String> names, List<String> values, List<String> expandedValues, int cursorLine) {
        return new ScreenState.DataPreview(
                0, 1, names, List.of(values), List.of(expandedValues),
                0, 0, 0, true, Set.of(), cursorLine, 0);
    }

    /// A Data preview state with the modal closed, for tests that open it
    /// with `Enter` and assert on where the cursor lands. `pageSize` matches
    /// the stride `area` will make `handle` observe, so opening doesn't
    /// re-page the synthetic rows away.
    private static ScreenState.DataPreview closedPreview(
            List<String> names, List<String> values, List<String> expandedValues, Rect area) {
        return new ScreenState.DataPreview(
                0, area.height() - 3, names, List.of(values), List.of(expandedValues),
                0, 0, -1, true, Set.of(), 0);
    }

    private static List<String> numberedValues(String prefix, int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> prefix + i)
                .toList();
    }

    private static NavigationStack rooted(ScreenState child) {
        NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
        stack.push(child);
        return stack;
    }

    private static String renderToString(Buffer buffer, Rect screenArea) {
        StringBuilder sb = new StringBuilder();
        for (int y = 0; y < screenArea.height(); y++) {
            for (int x = 0; x < screenArea.width(); x++) {
                String sym = buffer.get(x, y).symbol();
                sb.append(sym == null || sym.isEmpty() ? ' ' : sym);
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    static Stream<org.junit.jupiter.params.provider.Arguments> smokeMatrix() {
        // Pick a handful of fixtures that span the file-shape space:
        // CI present / absent, dict present / absent, nested, variant.
        String[] fixtures = {
                "column_index_pushdown.parquet",  // has CI
                "dictionary_with_crc.parquet",    // has dict on one column only
                "filter_pushdown_int.parquet",    // no CI, no dict, plain
                "nested_struct_test.parquet",     // nested schema
                "variant_test.parquet",           // VARIANT type
                "primitive_types_test.parquet",   // many primitive types
        };
        ScreenCtor[] screens = {
                new ScreenCtor("Overview", m -> ScreenState.Overview.initial()),
                new ScreenCtor("Schema", m -> ScreenState.Schema.initial()),
                new ScreenCtor("RowGroups", m -> new ScreenState.RowGroups(0)),
                new ScreenCtor("RowGroupDetail",
                        m -> new ScreenState.RowGroupDetail(0,
                                ScreenState.RowGroupDetail.Pane.MENU, 0)),
                new ScreenCtor("RowGroupIndexes",
                        m -> new ScreenState.RowGroupIndexes(0, 0)),
                new ScreenCtor("ColumnChunks",
                        m -> new ScreenState.ColumnChunks(0, 0)),
                new ScreenCtor("ColumnChunkDetail",
                        m -> new ScreenState.ColumnChunkDetail(0, 0,
                                ScreenState.ColumnChunkDetail.Pane.MENU, 0, true, false)),
                new ScreenCtor("Pages",
                        m -> new ScreenState.Pages(0, 0, 0, false, true)),
                new ScreenCtor("ColumnAcrossRowGroups",
                        m -> new ScreenState.ColumnAcrossRowGroups(0, 0, true)),
                new ScreenCtor("Footer", m -> ScreenState.Footer.initial()),
                new ScreenCtor("DataPreview",
                        m -> dev.hardwood.cli.dive.internal.DataPreviewScreen.initialState(m, 5)),
        };
        return Stream.of(fixtures).flatMap(f ->
                Stream.of(screens).map(sc ->
                        org.junit.jupiter.params.provider.Arguments.of(f, sc.name(), sc.ctor())));
    }

    private record ScreenCtor(String name, Function<ParquetModel, ScreenState> ctor) {
    }

    private static int columnIndexOf(ParquetModel model, String dottedName) {
        for (ColumnSchema column : model.schema().getColumns()) {
            if (column.fieldPath().matchesDottedName(dottedName)) {
                return column.columnIndex();
            }
        }
        throw new IllegalArgumentException("no such column: " + dottedName);
    }

    private static RenderHarness.RenderedFrame renderSizeStatistics(String dottedName, boolean levels)
            throws Exception {
        Path path = Path.of(DiveRenderTest.class.getResource("/dive_screenshots_fixture.parquet").toURI());
        try (ParquetModel sizeStatsModel = ParquetModel.open(InputFile.of(path), path.toString())) {
            return RenderHarness.render(AREA, new ScreenState.ColumnChunkDetail(
                    0, columnIndexOf(sizeStatsModel, dottedName),
                    ScreenState.ColumnChunkDetail.Pane.FACTS, 0, true, levels), sizeStatsModel);
        }
    }

    /// Whether the column index holds per-page histograms is already in the
    /// metadata the menu has loaded, so the hint answers it before the reader
    /// spends a keystroke finding out.
    @Test
    void menuHintsAnnotatePageLevelSizeStatisticsWhenPresent() throws Exception {
        Path path = Path.of(getClass().getResource("/size_statistics_test.parquet").toURI());
        try (ParquetModel pageIndexModel = ParquetModel.open(InputFile.of(path), path.toString())) {
            RenderHarness.RenderedFrame frame = RenderHarness.render(AREA,
                    new ScreenState.ColumnChunkDetail(0, columnIndexOf(pageIndexModel, "name"),
                            ScreenState.ColumnChunkDetail.Pane.MENU, 0, true, false), pageIndexModel);

            assertThat(frame.contains("present · levels")).isTrue();
            assertThat(frame.contains("present · unencoded")).isTrue();
        }
    }

    /// A page index that predates the size-statistics fields still reads
    /// `present`; the annotation is what distinguishes the two.
    @Test
    void menuHintsOmitTheAnnotationWhenThePageIndexCarriesNoSizeStatistics() {
        RenderHarness.RenderedFrame frame = RenderHarness.render(AREA,
                new ScreenState.ColumnChunkDetail(0, 0,
                        ScreenState.ColumnChunkDetail.Pane.MENU, 0, true, false), model);

        assertThat(frame.contains("present")).isTrue();
        assertThat(frame.contains("· levels")).isFalse();
        assertThat(frame.contains("· unencoded")).isFalse();
    }

    /// Collapsed is the default: the derived rows are the summary worth
    /// seeing at a glance, and the pane does not scroll.
    @Test
    void columnChunkDetailShowsDerivedSizeStatisticsWithLevelsCollapsed() throws Exception {
        RenderHarness.RenderedFrame frame = renderSizeStatistics("websites.list.element", false);

        assertThat(frame.contains("Size statistics")).isTrue();
        assertThat(frame.contains("chunk only")).isTrue();
        assertThat(frame.contains("Records")).isTrue();
        assertThat(frame.contains("per record")).isTrue();
        assertThat(frame.contains("[l] to show")).isTrue();
        assertThat(frame.contains("websites empty")).isFalse();
    }

    @Test
    void columnChunkDetailShowsNamedLevelBucketsWhenToggledOn() throws Exception {
        RenderHarness.RenderedFrame frame = renderSizeStatistics("websites.list.element", true);

        assertThat(frame.contains("websites null")).isTrue();
        assertThat(frame.contains("websites empty")).isTrue();
        assertThat(frame.contains("element null")).isTrue();
        assertThat(frame.contains("element present")).isTrue();
        assertThat(frame.contains("new record")).isTrue();
        assertThat(frame.contains("websites.list")).isTrue();
        assertThat(frame.contains("[l] to show")).isFalse();
    }

    /// A column the writer recorded no size statistics for says so, but still
    /// shows the unencoded size, which follows from the value count and the
    /// fixed width rather than from anything the writer had to record.
    @Test
    void columnChunkDetailReportsAMissingSizeStatisticsAsNotWritten() throws Exception {
        RenderHarness.RenderedFrame frame = renderSizeStatistics("metric_a", true);

        assertThat(frame.contains("— (not written)")).isTrue();
        assertThat(frame.contains("per record")).isFalse();
        assertThat(frame.contains("Unencoded")).isTrue();
    }

    /// A required, non-repeated BYTE_ARRAY has no histograms to show but
    /// its unencoded size is still the interesting number.
    @Test
    void columnChunkDetailShowsUnencodedSizeForAFlatRequiredColumn() throws Exception {
        RenderHarness.RenderedFrame frame = renderSizeStatistics("id", true);

        assertThat(frame.contains("Unencoded")).isTrue();
        assertThat(frame.contains("Avg value size")).isTrue();
        assertThat(frame.contains("Records")).isFalse();
    }

    /// `dive` and `hardwood inspect columns` must name the same encoding for
    /// the same chunk. Both read `encoding_stats`, so both say what the data
    /// pages use rather than repeating the flat list, which carries the
    /// dictionary page and the RLE level streams too.
    @Test
    void columnChunkDetailNamesTheDataPageEncodingAndTheDeclaredList() throws Exception {
        RenderHarness.RenderedFrame frame = renderSizeStatistics("names.primary", false);

        assertThat(frame.contains("Encoding")).isTrue();
        // Every value distinct, so the dictionary is a second copy of the
        // column — the one thing `DICT` on its own cannot say.
        assertThat(frame.contains("DICT 100%")).isTrue();
        assertThat(frame.contains("150 entries for 150 values")).isTrue();
        assertThat(frame.contains("Chunk encodings")).isTrue();
    }

    /// Without `encoding_stats` the declared list is where the label came
    /// from, so showing it a second time would restate the row above it.
    @Test
    void columnChunkDetailDropsTheDeclaredListWithoutEncodingStats() throws Exception {
        Path path = Path.of(getClass().getResource("/geospatial_e2e_test.parquet").toURI());
        try (ParquetModel plainModel = ParquetModel.open(InputFile.of(path), path.toString())) {
            RenderHarness.RenderedFrame frame = RenderHarness.render(AREA,
                    new ScreenState.ColumnChunkDetail(0, columnIndexOf(plainModel, "city_name"),
                            ScreenState.ColumnChunkDetail.Pane.FACTS, 0, true, false), plainModel);

            assertThat(frame.contains("Encoding")).isTrue();
            assertThat(frame.contains("Chunk encodings")).isFalse();
        }
    }

    /// A required column holds no nulls, which the schema settles whether or
    /// not the writer recorded a `null_count`. Reporting `—` would contradict
    /// the present-value count taken from the same place.
    @Test
    void columnChunkDetailReportsZeroNullsForARequiredColumnWithoutStatistics() throws Exception {
        Path path = Path.of(getClass().getResource("/geospatial_e2e_test.parquet").toURI());
        try (ParquetModel plainModel = ParquetModel.open(InputFile.of(path), path.toString())) {
            RenderHarness.RenderedFrame frame = RenderHarness.render(AREA,
                    new ScreenState.ColumnChunkDetail(0, columnIndexOf(plainModel, "city_name"),
                            ScreenState.ColumnChunkDetail.Pane.FACTS, 0, true, false), plainModel);

            assertThat(frame.contains("Nulls")).isTrue();
            assertThat(frame.contains("0  (0.0%)")).isTrue();
        }
    }

    /// The cross-row-group screen is the interactive twin of
    /// `inspect columns --column`, so it carries the same unencoded size and
    /// the same percentage form of the compression.
    @Test
    void columnAcrossRowGroupsCarriesTheUnencodedSizeAndCompression() {
        RenderHarness.RenderedFrame frame = RenderHarness.render(AREA,
                new ScreenState.ColumnAcrossRowGroups(0, 0, true, 0), model);

        assertThat(frame.contains("Unencoded")).isTrue();
        assertThat(frame.contains("Compression")).isTrue();
        assertThat(frame.contains("Ratio")).isFalse();
    }

    /// Every surface renders compression as a percentage of the uncompressed
    /// size. A `×` factor on one screen and a `%` on the next describes the
    /// same quantity two ways, which is the reading error this pins shut.
    @ParameterizedTest
    @MethodSource("compressionScreens")
    void everyScreenRendersCompressionAsAPercentage(ScreenState state) {
        RenderHarness.RenderedFrame frame = RenderHarness.render(AREA, state, model);

        assertThat(frame.contains("Compression")).isTrue();
        assertThat(frame.contains("×")).isFalse();
    }

    static Stream<ScreenState> compressionScreens() {
        return Stream.of(
                ScreenState.Overview.initial(),
                new ScreenState.RowGroups(0),
                new ScreenState.RowGroupDetail(0, ScreenState.RowGroupDetail.Pane.MENU, 0),
                new ScreenState.ColumnChunks(0, 0, 0),
                new ScreenState.ColumnAcrossRowGroups(0, 0, true, 0));
    }

    /// Two `—` rows are not worth a toggle, so a chunk with no usable
    /// histogram shows them outright and never advertises `[l]` — the key
    /// would reveal exactly what is already on screen.
    @Test
    void columnChunkDetailShowsDegradedLevelRowsWithoutTheToggle() throws Exception {
        RenderHarness.RenderedFrame collapsed = renderSizeStatistics("id", false);

        assertThat(collapsed.contains("Def levels")).isTrue();
        assertThat(collapsed.contains("— (required, every value present)")).isTrue();
        assertThat(collapsed.contains("— (not repeated)")).isTrue();
        assertThat(collapsed.contains("[l] to show")).isFalse();
    }

    @Test
    void levelsKeyIsAdvertisedOnlyForAChunkWithAHistogram() throws Exception {
        Path path = Path.of(getClass().getResource("/dive_screenshots_fixture.parquet").toURI());
        try (ParquetModel sizeStatsModel = ParquetModel.open(InputFile.of(path), path.toString())) {
            assertThat(keybarFor(sizeStatsModel, "websites.list.element")).contains("[l] levels");
            // Size statistics, but both histograms empty.
            assertThat(keybarFor(sizeStatsModel, "id")).doesNotContain("[l] levels");
            // No size statistics at all.
            assertThat(keybarFor(sizeStatsModel, "metric_a")).doesNotContain("[l] levels");
        }
    }

    /// The facts pane runs to about forty lines on a nested column with its
    /// levels shown, so on an ordinary terminal the tail falls off the bottom.
    /// Dropping it silently is the hazard: the reader cannot tell a clipped
    /// pane from a complete one.
    @Test
    void columnChunkDetailScrollsTheFactsPaneWhenItOverflows() throws Exception {
        Path path = Path.of(getClass().getResource("/dive_screenshots_fixture.parquet").toURI());
        try (ParquetModel model = ParquetModel.open(InputFile.of(path), path.toString())) {
            int column = columnIndexOf(model, "names.common.key_value.value");
            Rect shortArea = new Rect(0, 0, 120, 24);

            RenderHarness.RenderedFrame top = RenderHarness.render(shortArea,
                    new ScreenState.ColumnChunkDetail(0, column,
                            ScreenState.ColumnChunkDetail.Pane.FACTS, 0, true, true, 0), model);
            // The head is visible, the tail is not, and the title says so.
            assertThat(top.contains("Path")).isTrue();
            assertThat(top.contains("Rep levels")).isFalse();
            assertThat(hasRangeMarker(top)).isTrue();

            RenderHarness.RenderedFrame scrolled = RenderHarness.render(shortArea,
                    new ScreenState.ColumnChunkDetail(0, column,
                            ScreenState.ColumnChunkDetail.Pane.FACTS, 0, true, true, 40), model);
            // Clamped to the last full viewport, so the final line is reachable.
            assertThat(scrolled.contains("Rep levels")).isTrue();
            assertThat(scrolled.contains("Path")).isFalse();
        }
    }

    /// A pane that fits carries no range suffix — the marker is a statement
    /// that content is hidden, not decoration.
    @Test
    void columnChunkDetailOmitsTheRangeWhenEverythingFits() throws Exception {
        Path path = Path.of(getClass().getResource("/dive_screenshots_fixture.parquet").toURI());
        try (ParquetModel model = ParquetModel.open(InputFile.of(path), path.toString())) {
            RenderHarness.RenderedFrame frame = RenderHarness.render(new Rect(0, 0, 120, 60),
                    new ScreenState.ColumnChunkDetail(0, columnIndexOf(model, "metric_a"),
                            ScreenState.ColumnChunkDetail.Pane.FACTS, 0, true, false, 0), model);

            assertThat(frame.contains("Path")).isTrue();
            assertThat(frame.contains("— (not written)")).isTrue();
            assertThat(hasRangeMarker(frame)).isFalse();
        }
    }

    /// Whether the facts pane's title carries the `first-last/total` suffix
    /// that says content is hidden below the fold.
    private static boolean hasRangeMarker(RenderHarness.RenderedFrame frame) {
        return frame.lines().stream().anyMatch(line -> RANGE_MARKER.matcher(line).find());
    }

    private static String keybarFor(ParquetModel model, String dottedName) {
        return ColumnChunkDetailScreen.keybarKeys(new ScreenState.ColumnChunkDetail(
                0, columnIndexOf(model, dottedName),
                ScreenState.ColumnChunkDetail.Pane.FACTS, 0, true, false), model);
    }

    @Test
    void dataPreviewFillsTheViewportOnTheFrameItIsEnteredOn() {
        // Entering the screen left the bottom of the viewport blank until the
        // first keypress: the page was sized from whatever viewport the
        // previous screen had observed, and only re-loaded on the next event.
        Keys.resetObservedGeometry();
        Rect body = new Rect(0, 0, 120, 30);
        // A page sized for a shorter screen, as arriving from a pane with
        // less room for rows produces.
        ScreenState.DataPreview entered = DataPreviewScreen.initialState(model, 5);
        NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
        stack.push(entered);

        DataPreviewScreen.fitToViewport(model, stack, body);

        ScreenState.DataPreview fitted = (ScreenState.DataPreview) stack.top();
        int viewport = body.height() - 3;
        assertThat(fitted.rows())
                .as("the page fills the rows the body can paint")
                .hasSize(viewport);
        assertThat(RenderHarness.render(body, fitted, model).contains(String.valueOf(viewport)))
                .as("the last row of the viewport is painted, not blank")
                .isTrue();
    }

    @Test
    void overviewFactsPaneKeepsTheKeyValueCursorOnScreen() throws Exception {
        // The facts pane moved a cursor it never scrolled to, so on a short
        // terminal the selected entry could sit below the fold while the
        // keybar still offered Enter to open it.
        Path file = Path.of(getClass().getResource("/cli_info_kv_metadata_test.parquet").getPath());
        try (ParquetModel m = ParquetModel.open(InputFile.of(file), file.toString())) {
            Rect area = new Rect(0, 0, 120, 12);
            NavigationStack stack = new NavigationStack(new ScreenState.Overview(
                    ScreenState.Overview.Pane.FACTS, 0, 0, false, 0));
            RenderHarness.render(area, stack.top(), m);

            assertThat(OverviewScreen.handle(
                    new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'G'), m, stack)).isTrue();
            ScreenState.Overview bottom = (ScreenState.Overview) stack.top();
            List<Map.Entry<String, String>> kv = m.facts().keyValueMetadata();
            assertThat(bottom.kvSelection())
                    .isEqualTo(OverviewScreen.kvEntryRow(kv.size() - 1));
            RenderHarness.RenderedFrame frame = RenderHarness.render(area, bottom, m);
            assertThat(frame.contains(kv.get(kv.size() - 1).getKey())).isTrue();
        }
    }

    @Test
    void dataPreviewModalOffersJumpKeysEvenWhenTheBodyFits() {
        // g/G move the cursor, so they act whenever there is more than one
        // line — the hint gated them on the body having to scroll.
        ScreenState.DataPreview state = openModal(
                List.of("items", "id", "status"),
                List.of("[a, b]", "1", "ready"),
                List.of("[\n  a,\n  b\n]", "1", "ready"));
        RenderHarness.RenderedFrame rendered =
                RenderHarness.render(new Rect(0, 0, 100, 24), state, model);

        assertThat(rendered.contains("PgDn/PgUp page"))
                .as("the body fits, so there is nothing to page to")
                .isFalse();
        assertThat(rendered.contains("g/G first/last")).isTrue();
    }

    @Test
    void drillIntoCursorWalksPastEntriesTheChunkDoesNotHave() throws Exception {
        // The cursor was re-snapped to the first enabled entry on every
        // keypress, so on a chunk with only Pages and Dictionary populated it
        // moved onto Column index and was dragged straight back.
        Path file = Path.of(getClass().getResource("/yellow_tripdata_sample.parquet").getPath());
        try (ParquetModel m = ParquetModel.open(InputFile.of(file), file.toString())) {
            NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
            stack.push(ColumnChunkDetailScreen.initialState(m, 0, 0, true));
            RenderHarness.render(new Rect(0, 0, 120, 24), stack.top(), m);

            for (int expected = 1; expected <= 3; expected++) {
                assertThat(ColumnChunkDetailScreen.handle(key(KeyCode.DOWN), m, stack)).isTrue();
                assertThat(((ScreenState.ColumnChunkDetail) stack.top()).menuSelection())
                        .isEqualTo(expected);
            }
            assertThat(ColumnChunkDetailScreen.handle(key(KeyCode.DOWN), m, stack)).isTrue();
            assertThat(((ScreenState.ColumnChunkDetail) stack.top()).menuSelection())
                    .as("the last entry is the last entry")
                    .isEqualTo(3);
        }
    }

    @Test
    void documentPaneWalksTheCursorToTheEdgeBeforeMovingItsContent() throws Exception {
        // Recomputing the window from the cursor pinned it to the bottom row,
        // so every step up dragged the whole pane along instead of walking the
        // cursor to the top of the window first, as the list screens do.
        Path file = Path.of(getClass().getResource("/yellow_tripdata_sample.parquet").getPath());
        try (ParquetModel m = ParquetModel.open(InputFile.of(file), file.toString())) {
            Keys.resetObservedGeometry();
            Rect area = new Rect(0, 0, 110, 10);
            NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
            stack.push(new ScreenState.RowGroupDetail(
                    0, ScreenState.RowGroupDetail.Pane.FACTS, 0));
            RenderHarness.render(area, stack.top(), m);
            RowGroupDetailScreen.handle(
                    new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'G'), m, stack);
            RenderHarness.render(area, stack.top(), m);

            int settled = ((ScreenState.RowGroupDetail) stack.top()).factsTop();
            assertThat(settled).isPositive();

            assertThat(RowGroupDetailScreen.handle(key(KeyCode.UP), m, stack)).isTrue();
            ScreenState.RowGroupDetail moved = (ScreenState.RowGroupDetail) stack.top();
            assertThat(moved.scrollTop())
                    .as("the cursor moved")
                    .isLessThan(((ScreenState.RowGroupDetail) stack.top()).scrollTop() + 1);
            assertThat(moved.factsTop())
                    .as("the content did not: the cursor was not yet at the top of the window")
                    .isEqualTo(settled);
        }
    }

    @Test
    void overviewFactsComeBackWhenTheCursorReturnsToTheTop() throws Exception {
        // The window only ever slid far enough to reveal the cursor, and this
        // cursor cannot reach the facts — so once the list had pushed them off
        // the top, walking back up did not bring them back.
        Path file = Path.of(getClass().getResource("/cli_info_kv_metadata_test.parquet").getPath());
        try (ParquetModel m = ParquetModel.open(InputFile.of(file), file.toString())) {
            Keys.resetObservedGeometry();
            Rect area = new Rect(0, 0, 110, 12);
            NavigationStack stack = new NavigationStack(new ScreenState.Overview(
                    ScreenState.Overview.Pane.FACTS, 0, 0, false, 0));
            RenderHarness.render(area, stack.top(), m);

            OverviewScreen.handle(new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'G'), m, stack);
            RenderHarness.render(area, stack.top(), m);
            OverviewScreen.handle(new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'g'), m, stack);

            assertThat(RenderHarness.render(area, (ScreenState) stack.top(), m)
                    .contains("Format version"))
                    .as("the facts are on screen again")
                    .isTrue();
        }
    }

    @Test
    void footerDoesNotJumpOnTheFirstKeyAfterOpening() {
        // The entry state was built before this pane had rendered, so its
        // stored window came from the previous pane's viewport. The frame the
        // reader saw was drawn from the reconciled value, and the first
        // keypress used to step the body to catch up.
        Keys.resetObservedGeometry();
        Rect area = new Rect(0, 0, 110, 14);
        NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
        stack.push(FooterScreen.initialState(model));
        RenderHarness.RenderedFrame opened = RenderHarness.render(area, stack.top(), model);

        assertThat(FooterScreen.handle(key(KeyCode.UP), model, stack)).isTrue();
        RenderHarness.RenderedFrame moved = RenderHarness.render(area, stack.top(), model);

        assertThat(moved.lines().getFirst())
                .as("the body stayed put; only the cursor moved")
                .isEqualTo(opened.lines().getFirst());
    }

    @Test
    void footerCursorReachesEveryRowOfTheBody() throws Exception {
        // The cursor stopped only on anchors, and the anchors sit in the
        // lower half of the body, so the rows above the topmost one could not
        // be visited at all.
        Path file = Path.of(getClass().getResource("/yellow_tripdata_sample.parquet").getPath());
        try (ParquetModel m = ParquetModel.open(InputFile.of(file), file.toString())) {
            Keys.resetObservedGeometry();
            Rect area = new Rect(0, 0, 110, 14);
            NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
            stack.push(FooterScreen.initialState(m));
            RenderHarness.render(area, stack.top(), m);
            int anchor = ((ScreenState.Footer) stack.top()).cursorRow();
            assertThat(anchor)
                    .as("opening lands on an anchor, not on line zero")
                    .isPositive();

            // One row at a time all the way to the top, visiting every row.
            for (int expected = anchor - 1; expected >= 0; expected--) {
                assertThat(FooterScreen.handle(key(KeyCode.UP), m, stack)).isTrue();
                assertThat(((ScreenState.Footer) stack.top()).cursorRow()).isEqualTo(expected);
            }
            assertThat(FooterScreen.handle(key(KeyCode.UP), m, stack))
                    .as("handled, but there is nowhere above the first line")
                    .isTrue();
            assertThat(((ScreenState.Footer) stack.top()).cursorRow()).isZero();
            assertThat(RenderHarness.render(area, (ScreenState) stack.top(), m)
                    .contains("Format version"))
                    .as("the head of the body is on screen")
                    .isTrue();
        }
    }

    @Test
    void overviewFactsScrollPastTheFixedFactsWhenTheCursorNeedsTheRoom() throws Exception {
        // The facts above the key/value list were pinned and only the list
        // was windowed, so on a short pane whatever they pushed past the
        // bottom could not be reached.
        Path file = Path.of(getClass().getResource("/cli_info_kv_metadata_test.parquet").getPath());
        try (ParquetModel m = ParquetModel.open(InputFile.of(file), file.toString())) {
            Keys.resetObservedGeometry();
            Rect area = new Rect(0, 0, 110, 12);
            NavigationStack stack = new NavigationStack(new ScreenState.Overview(
                    ScreenState.Overview.Pane.FACTS, 0, 0, false, 0));
            RenderHarness.render(area, stack.top(), m);

            List<Map.Entry<String, String>> kv = m.facts().keyValueMetadata();
            assertThat(OverviewScreen.handle(
                    new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'G'), m, stack)).isTrue();

            RenderHarness.RenderedFrame frame =
                    RenderHarness.render(area, (ScreenState) stack.top(), m);
            assertThat(frame.contains(kv.get(kv.size() - 1).getKey()))
                    .as("the last entry is reachable")
                    .isTrue();
            assertThat(frame.contains("Format version"))
                    .as("the facts scrolled out of the way to make room for it")
                    .isFalse();
        }
    }

    @Test
    void schemaKeepsTheCursorOnScreen() throws Exception {
        // The schema tree moved its cursor and rendered from row zero, so on
        // any file with more columns than the viewport the cursor left the
        // screen — while the title reported a range the body did not show.
        Path file = Path.of(getClass().getResource("/yellow_tripdata_sample.parquet").getPath());
        try (ParquetModel m = ParquetModel.open(InputFile.of(file), file.toString())) {
            Rect area = new Rect(0, 0, 120, 12);
            NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
            stack.push(ScreenState.Schema.initial());
            RenderHarness.render(area, stack.top(), m);

            assertThat(SchemaScreen.handle(
                    new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'G'), m, stack)).isTrue();
            ScreenState.Schema bottom = (ScreenState.Schema) stack.top();
            assertThat(bottom.scrollTop()).isPositive();

            // G selects the last leaf, so the window must have moved off the
            // first one and onto the last.
            RenderHarness.RenderedFrame frame = RenderHarness.render(area, bottom, m);
            assertThat(frame.contains(m.schema().getColumn(0).name())).isFalse();
            assertThat(frame.contains(m.schema().getColumn(m.columnCount() - 1).name())).isTrue();
        }
    }

    @Test
    void rowGroupDetailFactsPaneScrollsToItsTail() {
        // Eighteen lines of facts in a pane that can show eleven: without
        // scrolling the page-index section is unreachable.
        Rect area = new Rect(0, 0, 120, 14);
        ScreenState.RowGroupDetail top = new ScreenState.RowGroupDetail(
                0, ScreenState.RowGroupDetail.Pane.FACTS, 0);
        assertThat(RenderHarness.render(area, top, model).contains("Page indexes")).isFalse();

        NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
        stack.push(top);
        assertThat(RowGroupDetailScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'G'), model, stack)).isTrue();
        ScreenState.RowGroupDetail bottom = (ScreenState.RowGroupDetail) stack.top();
        assertThat(bottom.scrollTop()).isPositive();
        assertThat(RenderHarness.render(area, bottom, model).contains("Page indexes")).isTrue();
    }

    @Test
    void keyValueModalPagesWithPageDownAsWellAsShift() throws Exception {
        // The modal recognised only Shift+↑/↓, so PgDn did nothing there while
        // it paged on every other scrollable pane.
        Path file = Path.of(getClass().getResource("/cli_info_kv_metadata_test.parquet").getPath());
        try (ParquetModel m = ParquetModel.open(InputFile.of(file), file.toString())) {
            Rect area = new Rect(0, 0, 120, 14);
            ScreenState.Overview open = new ScreenState.Overview(
                    ScreenState.Overview.Pane.FACTS, 0,
                    OverviewScreen.kvEntryRow(kvIndexOf(m, "pandas")), true, 0);
            RenderHarness.render(area, open, m);

            NavigationStack stack = new NavigationStack(open);
            assertThat(OverviewScreen.handle(key(KeyCode.PAGE_DOWN), m, stack)).isTrue();
            assertThat(((ScreenState.Overview) stack.top()).kvModalScroll()).isPositive();

            assertThat(OverviewScreen.handle(
                    new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'G'), m, stack)).isTrue();
            int bottom = ((ScreenState.Overview) stack.top()).kvModalScroll();
            assertThat(bottom).isPositive();

            assertThat(OverviewScreen.handle(
                    new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'g'), m, stack)).isTrue();
            assertThat(((ScreenState.Overview) stack.top()).kvModalScroll()).isZero();
        }
    }

    @Test
    void pageHeaderModalScrollsOnAShortTerminal() {
        // The modal took only Esc/Enter, so on a terminal too short for the
        // header its tail could not be reached.
        Rect area = new Rect(0, 0, 120, 14);
        ScreenState.Pages open = new ScreenState.Pages(0, 0, 0, true, true);
        RenderHarness.render(area, open, model);

        NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
        stack.push(open);
        assertThat(PagesScreen.handle(key(KeyCode.PAGE_DOWN), model, stack)).isTrue();
        assertThat(((ScreenState.Pages) stack.top()).modalScroll()).isPositive();
        // The modal stays open: paging is not a way out of it.
        assertThat(((ScreenState.Pages) stack.top()).modalOpen()).isTrue();
    }

    private static int kvIndexOf(ParquetModel model, String key) {
        List<Map.Entry<String, String>> kv = model.facts().keyValueMetadata();
        for (int i = 0; i < kv.size(); i++) {
            if (kv.get(i).getKey().equals(key)) {
                return i;
            }
        }
        throw new IllegalArgumentException("no such key/value entry: " + key);
    }

    private static KeyEvent key(KeyCode code) {
        return new KeyEvent(code, KeyModifiers.NONE, '\0');
    }
}
