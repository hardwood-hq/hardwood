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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.cli.dive.internal.ColumnAcrossRowGroupsScreen;
import dev.hardwood.cli.dive.internal.ColumnChunkDetailScreen;
import dev.hardwood.cli.dive.internal.ColumnChunksScreen;
import dev.hardwood.cli.dive.internal.ColumnIndexScreen;
import dev.hardwood.cli.dive.internal.DataPreviewScreen;
import dev.hardwood.cli.dive.internal.DictionaryScreen;
import dev.hardwood.cli.dive.internal.FileIndexesScreen;
import dev.hardwood.cli.dive.internal.FooterScreen;
import dev.hardwood.cli.dive.internal.Keys;
import dev.hardwood.cli.dive.internal.OverviewScreen;
import dev.hardwood.cli.dive.internal.PagesScreen;
import dev.hardwood.cli.dive.internal.RowGroupDetailScreen;
import dev.hardwood.cli.dive.internal.RowGroupsScreen;
import dev.hardwood.cli.dive.internal.SchemaScreen;
import dev.tamboui.layout.Rect;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.tui.event.KeyModifiers;

import static org.assertj.core.api.Assertions.assertThat;

/// Layer-1 tests for screen handlers. Handlers are pure functions of `(event,
/// model, stack)`; these tests drive them with synthesised key events against a
/// real fixture and assert on the resulting [NavigationStack].
class DiveStateTest {

    private ParquetModel model;

    @BeforeEach
    void openFixture() throws Exception {
        // Clear any viewport observation a previous render-path test left
        // behind. Data preview's auto-resize keys off Keys.viewportStride
        // so a stale observation would force a re-load and break the
        // explicit page-size assertions in this suite.
        Keys.resetObservedGeometry();
        // 10 000 rows × 2 columns (id, value) in 1 RG / ~10 pages; has a Column Index.
        // Covers pagination, schema navigation, and column-index drills without
        // needing multiple fixtures.
        Path path = Path.of(getClass().getResource("/column_index_pushdown.parquet").getPath());
        model = ParquetModel.open(InputFile.of(path), path.toString());
    }

    @AfterEach
    void closeModel() throws Exception {
        model.close();
    }

    @Test
    void overviewDownMovesMenuSelection() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU, 0, 0, false, 0));

        OverviewScreen.handle(key(KeyCode.DOWN), model, stack);

        assertThat(stack.top()).isEqualTo(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU, 1, 0, false, 0));
    }

    @Test
    void overviewUpAtTopClampsToZero() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU, 0, 0, false, 0));

        OverviewScreen.handle(key(KeyCode.UP), model, stack);

        assertThat(((ScreenState.Overview) stack.top()).menuSelection()).isZero();
    }

    @Test
    void overviewTabSwitchesFocus() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU, 0, 0, false, 0));

        OverviewScreen.handle(key(KeyCode.TAB), model, stack);

        assertThat(((ScreenState.Overview) stack.top()).focus())
                .isEqualTo(ScreenState.Overview.Pane.FACTS);
    }

    @Test
    void overviewEnterOnRowGroupsPushesRowGroupsScreen() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU,
                        OverviewScreen.MenuItem.ROW_GROUPS.ordinal(), 0, false, 0));

        OverviewScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.RowGroups.class);
        assertThat(stack.depth()).isEqualTo(2);
    }

    @Test
    void overviewEnterOnSchemaPushesSchemaScreen() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU,
                        OverviewScreen.MenuItem.SCHEMA.ordinal(), 0, false, 0));

        OverviewScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.Schema.class);
    }

    @Test
    void overviewEnterOnFooterPushesFooterScreen() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU,
                        OverviewScreen.MenuItem.FOOTER.ordinal(), 0, false, 0));

        OverviewScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.Footer.class);
    }

    @Test
    void rowGroupsEnterDrillsIntoRowGroupDetail() {
        NavigationStack stack = rooted(new ScreenState.RowGroups(0));

        RowGroupsScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.RowGroupDetail.class);
        assertThat(((ScreenState.RowGroupDetail) stack.top()).rowGroupIndex()).isZero();
    }

    @Test
    void rowGroupDetailEnterOnColumnChunksDrills() {
        NavigationStack stack = rooted(new ScreenState.RowGroupDetail(
                0, ScreenState.RowGroupDetail.Pane.MENU,
                RowGroupDetailScreen.MenuItem.COLUMN_CHUNKS.ordinal()));

        RowGroupDetailScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.ColumnChunks.class);
    }

    @Test
    void rowGroupDetailEnterOnIndexesDrills() {
        NavigationStack stack = rooted(new ScreenState.RowGroupDetail(
                0, ScreenState.RowGroupDetail.Pane.MENU,
                RowGroupDetailScreen.MenuItem.INDEXES.ordinal()));

        RowGroupDetailScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.RowGroupIndexes.class);
    }

    @Test
    void rowGroupsDownClampsAtLastGroup() {
        int last = model.rowGroupCount() - 1;
        NavigationStack stack = rooted(new ScreenState.RowGroups(last));

        RowGroupsScreen.handle(key(KeyCode.DOWN), model, stack);

        assertThat(((ScreenState.RowGroups) stack.top()).selection()).isEqualTo(last);
    }

    @Test
    void columnChunksEnterDrillsIntoDetail() {
        NavigationStack stack = rooted(new ScreenState.ColumnChunks(0, 0));

        ColumnChunksScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.ColumnChunkDetail.class);
    }

    @Test
    void schemaDownMovesSelection() {
        NavigationStack stack = rooted(ScreenState.Schema.initial());

        SchemaScreen.handle(key(KeyCode.DOWN), model, stack);

        int expected = model.columnCount() == 1 ? 0 : 1;
        assertThat(((ScreenState.Schema) stack.top()).selection()).isEqualTo(expected);
    }

    @Test
    void navigationStackPopReturnsToParent() {
        NavigationStack stack = rooted(new ScreenState.RowGroups(0));

        stack.pop();

        assertThat(stack.depth()).isEqualTo(1);
        assertThat(stack.top()).isInstanceOf(ScreenState.Overview.class);
    }

    @Test
    void navigationStackCannotPopRoot() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU, 0, 0, false, 0));

        stack.pop();
        stack.pop();

        assertThat(stack.depth()).isEqualTo(1);
    }

    @Test
    void navigationStackClearToRootCollapsesPath() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU, 0, 0, false, 0));
        stack.push(new ScreenState.RowGroups(0));
        stack.push(new ScreenState.ColumnChunks(0, 2));
        stack.push(new ScreenState.ColumnChunkDetail(0, 2,
                ScreenState.ColumnChunkDetail.Pane.MENU, 0, true, false));

        stack.clearToRoot();

        assertThat(stack.depth()).isEqualTo(1);
        assertThat(stack.top()).isInstanceOf(ScreenState.Overview.class);
    }

    // --- Phase 2 ---

    @Test
    void schemaEnterDrillsIntoColumnAcrossRowGroups() {
        NavigationStack stack = rooted(ScreenState.Schema.initial());

        SchemaScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.ColumnAcrossRowGroups.class);
        assertThat(((ScreenState.ColumnAcrossRowGroups) stack.top()).columnIndex()).isZero();
    }

    @Test
    void columnAcrossRowGroupsEnterDrillsIntoChunkDetail() {
        NavigationStack stack = rooted(new ScreenState.ColumnAcrossRowGroups(0, 0, true));

        ColumnAcrossRowGroupsScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.ColumnChunkDetail.class);
        ScreenState.ColumnChunkDetail top = (ScreenState.ColumnChunkDetail) stack.top();
        assertThat(top.rowGroupIndex()).isZero();
        assertThat(top.columnIndex()).isZero();
    }

    @Test
    void columnChunkDetailTabSwitchesPaneBetweenFactsAndMenu() {
        NavigationStack stack = rooted(new ScreenState.ColumnChunkDetail(
                0, 0, ScreenState.ColumnChunkDetail.Pane.MENU, 0, true, false));

        ColumnChunkDetailScreen.handle(key(KeyCode.TAB), model, stack);

        assertThat(((ScreenState.ColumnChunkDetail) stack.top()).focus())
                .isEqualTo(ScreenState.ColumnChunkDetail.Pane.FACTS);
    }

    @Test
    void columnChunkDetailEnterOnPagesPushesPagesScreen() {
        NavigationStack stack = rooted(new ScreenState.ColumnChunkDetail(
                0, 0, ScreenState.ColumnChunkDetail.Pane.MENU,
                ColumnChunkDetailScreen.MenuItem.PAGES.ordinal(), true, false));

        ColumnChunkDetailScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.Pages.class);
    }

    @Test
    void dictionaryWithCrcFixtureHasDictOnCategoryColumnOnly() throws Exception {
        Path file = Path.of(getClass().getResource("/dictionary_with_crc.parquet").getPath());
        try (ParquetModel m = ParquetModel.open(InputFile.of(file), file.toString())) {
            // col0 = id (int64): no dictionary
            assertThat(m.chunk(0, 0).metaData().dictionaryPageOffset()).isNull();
            // col1 = category (string): has dictionary
            assertThat(m.chunk(0, 1).metaData().dictionaryPageOffset()).isNotNull();
        }
    }

    @Test
    void columnChunkDetailEnterDoesNothingOnAnEntryTheChunkDoesNotHave() {
        // The fixture chunk has no dictionary. The cursor may rest there —
        // the row carries a fact worth reading — but Enter has nowhere to go,
        // which the missing ▶ says.
        NavigationStack stack = rooted(new ScreenState.ColumnChunkDetail(
                0, 0, ScreenState.ColumnChunkDetail.Pane.MENU,
                ColumnChunkDetailScreen.MenuItem.DICTIONARY.ordinal(), true, false));

        assertThat(ColumnChunkDetailScreen.handle(key(KeyCode.ENTER), model, stack)).isFalse();
        assertThat(stack.top()).isInstanceOf(ScreenState.ColumnChunkDetail.class);
    }

    @Test
    void columnChunkDetailEntryStateLandsOnTheFirstEntryThatGoesSomewhere() {
        ScreenState.ColumnChunkDetail entered =
                ColumnChunkDetailScreen.initialState(model, 0, 0, true);

        assertThat(entered.menuSelection())
                .isEqualTo(ColumnChunkDetailScreen.MenuItem.PAGES.ordinal());
    }

    @Test
    void pagesEnterOpensModalAndCancelCloses() {
        NavigationStack stack = rooted(new ScreenState.Pages(0, 0, 0, false, true));

        PagesScreen.handle(key(KeyCode.ENTER), model, stack);
        assertThat(((ScreenState.Pages) stack.top()).modalOpen()).isTrue();

        PagesScreen.handle(key(KeyCode.ESCAPE), model, stack);
        assertThat(((ScreenState.Pages) stack.top()).modalOpen()).isFalse();
    }

    // --- Phase 3 ---

    @Test
    void overviewEnterOnDataPreviewPushesDataPreview() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU,
                        OverviewScreen.MenuItem.DATA_PREVIEW.ordinal(), 0, false, 0));

        OverviewScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.DataPreview.class);
        ScreenState.DataPreview preview = (ScreenState.DataPreview) stack.top();
        assertThat(preview.firstRow()).isZero();
        // Default startup page size is Keys.PAGE_STRIDE before the screen
        // first renders. Once it does, handle() resizes to the actual
        // viewport.
        assertThat(preview.pageSize()).isPositive();
    }

    @Test
    void dataPreviewPageDownAdvancesAbsoluteSelectionBottomPinned() {
        // Sliding-window navigation: PgDn advances the absolute selection by
        // one viewport (pageSize), and the window slides so the cursor sits
        // on the last visible row — mirroring the dictionary screen.
        int pageSize = 10;
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, pageSize);
        NavigationStack stack = rooted(initial);

        DataPreviewScreen.handle(key(KeyCode.PAGE_DOWN), model, stack);

        ScreenState.DataPreview next = (ScreenState.DataPreview) stack.top();
        assertThat(next.firstRow() + next.selectedRow()).isEqualTo(pageSize);
        assertThat(next.selectedRow()).isEqualTo(pageSize - 1);
        assertThat(next.rows()).hasSize(pageSize);
    }

    @Test
    void dataPreviewLoadsNestedSchemaWithoutIndexOutOfBounds() throws Exception {
        // Regression for the AIOOBE that fired when loadPage iterated leaf-column
        // indices against a RowReader that expects top-level field indices.
        Path nested = Path.of(getClass().getResource("/nested_struct_test.parquet").getPath());
        try (ParquetModel nestedModel = ParquetModel.open(InputFile.of(nested), nested.toString())) {
            ScreenState.DataPreview state = DataPreviewScreen.initialState(nestedModel, 5);

            int topLevelFieldCount = nestedModel.schema().getRootNode().children().size();
            assertThat(state.columnNames()).hasSize(topLevelFieldCount);
            // Rows must also have exactly topLevelFieldCount cells — anything else
            // means the field / leaf confusion is back.
            if (!state.rows().isEmpty()) {
                assertThat(state.rows().get(0)).hasSize(topLevelFieldCount);
            }
        }
    }

    @Test
    void dataPreviewRendersNestedValuesStructurally() throws Exception {
        // Regression: PqList / PqStruct / PqMap / PqVariant fall through to
        // the JVM default toString, producing "dev.hardwood.internal.reader.…".
        // The formatter now renders them as JSON-like text.
        Path nested = Path.of(getClass().getResource("/nested_struct_test.parquet").getPath());
        try (ParquetModel nestedModel = ParquetModel.open(InputFile.of(nested), nested.toString())) {
            ScreenState.DataPreview state = DataPreviewScreen.initialState(nestedModel, 5);
            for (List<String> row : state.rows()) {
                for (String cell : row) {
                    assertThat(cell)
                            .as("cell value should not be a JVM hashcode form")
                            .doesNotStartWith("dev.hardwood.internal")
                            .doesNotStartWith("[B@");
                }
            }
        }
    }

    @Test
    void dataPreviewPageDownLoadsContiguousRows() {
        int pageSize = 10;
        ScreenState.DataPreview first = DataPreviewScreen.initialState(model, pageSize);
        NavigationStack stack = rooted(first);

        DataPreviewScreen.handle(key(KeyCode.PAGE_DOWN), model, stack);

        ScreenState.DataPreview second = (ScreenState.DataPreview) stack.top();
        // `id` column in column_index_pushdown.parquet is sorted 0..9999.
        // Sliding window after one PgDn from row 0: cursor sits on absolute
        // row 10, the window covers [1, 10] so the first visible id is "1"
        // and the cursor (selectedRow = pageSize - 1) lands on id "10".
        assertThat(second.rows().get(0).get(0)).isEqualTo("1");
        assertThat(second.rows().get(second.selectedRow()).get(0)).isEqualTo("10");
    }

    @Test
    void dataPreviewStepDownPastBottomKeepsCursorAtBottom() {
        // Mirrors dictionary's behaviour: walking past the bottom of the
        // viewport slides the window by one row and keeps the cursor on
        // the last visible row. Without this the cursor would jump to the
        // top of the next page.
        int pageSize = 4;
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, pageSize);
        NavigationStack stack = rooted(initial);
        // Move to the bottom of the first viewport.
        for (int i = 0; i < pageSize - 1; i++) {
            DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack);
        }
        ScreenState.DataPreview atBottom = (ScreenState.DataPreview) stack.top();
        assertThat(atBottom.firstRow()).isZero();
        assertThat(atBottom.selectedRow()).isEqualTo(pageSize - 1);

        DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack);

        ScreenState.DataPreview slid = (ScreenState.DataPreview) stack.top();
        assertThat(slid.firstRow()).isEqualTo(1);
        assertThat(slid.selectedRow()).isEqualTo(pageSize - 1);
        assertThat(slid.firstRow() + slid.selectedRow()).isEqualTo(pageSize);
    }

    @Test
    void dataPreviewStepUpPastTopKeepsCursorAtTop() {
        // Symmetric: walking past the top of the viewport slides the window
        // by one row in the other direction and keeps the cursor on row 0.
        int pageSize = 4;
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, pageSize);
        NavigationStack stack = rooted(initial);
        // Slide a few rows down so we have room to step up across the top.
        for (int i = 0; i < pageSize + 2; i++) {
            DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack);
        }
        ScreenState.DataPreview anchored = (ScreenState.DataPreview) stack.top();
        assertThat(anchored.selectedRow()).isEqualTo(pageSize - 1);
        long abs = anchored.firstRow() + anchored.selectedRow();

        // Step up enough times to drop the cursor to row 0 of the viewport.
        for (int i = 0; i < pageSize - 1; i++) {
            DataPreviewScreen.handle(key(KeyCode.UP), model, stack);
        }
        ScreenState.DataPreview atTop = (ScreenState.DataPreview) stack.top();
        assertThat(atTop.selectedRow()).isZero();
        long topFirstRow = atTop.firstRow();

        DataPreviewScreen.handle(key(KeyCode.UP), model, stack);

        ScreenState.DataPreview slid = (ScreenState.DataPreview) stack.top();
        assertThat(slid.firstRow()).isEqualTo(topFirstRow - 1);
        assertThat(slid.selectedRow()).isZero();
        assertThat(slid.firstRow() + slid.selectedRow()).isEqualTo(abs - pageSize);
    }

    @Test
    void dataPreviewPageUpAtStartIsNoop() {
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, 2);
        NavigationStack stack = rooted(initial);

        DataPreviewScreen.handle(key(KeyCode.PAGE_UP), model, stack);

        assertThat(((ScreenState.DataPreview) stack.top()).firstRow()).isZero();
    }

    @Test
    void rowGroupsGJumpsToFirstShiftGToLast() {
        int last = model.rowGroupCount() - 1;
        NavigationStack stack = rooted(new ScreenState.RowGroups(last));

        RowGroupsScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'g'), model, stack);
        assertThat(((ScreenState.RowGroups) stack.top()).selection()).isZero();

        RowGroupsScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'G'), model, stack);
        assertThat(((ScreenState.RowGroups) stack.top()).selection()).isEqualTo(last);
    }

    @Test
    void dictionaryPageUpLandsCursorAtTopOfNewViewport() {
        // PgUp from a bottom-pinned position should reveal the previous
        // viewport with the cursor at row 0, mirroring the data preview's
        // PgUp behaviour. Concretely: with viewportStride=10 and the cursor
        // at index 50, the visible window is [41, 50]; one PgUp moves
        // selection to 40 and the visible window slides to [40, 49] —
        // selection - scrollTop = 0, so the cursor lands at the top.
        dev.hardwood.cli.dive.internal.Keys.observeViewport(10);
        // Find a dictionary column with at least 60 entries so we can scroll.
        for (int rg = 0; rg < model.rowGroupCount(); rg++) {
            for (int c = 0; c < model.rowGroup(rg).columns().size(); c++) {
                if (model.chunk(rg, c).metaData().dictionaryPageOffset() == null) {
                    continue;
                }
                NavigationStack stack = rooted(new ScreenState.DictionaryView(
                        rg, c, 50, false, "", false, true, true, 41));

                DictionaryScreen.handle(key(KeyCode.PAGE_UP), model, stack);

                ScreenState.DictionaryView top = (ScreenState.DictionaryView) stack.top();
                assertThat(top.selection()).isEqualTo(40);
                assertThat(top.scrollTop()).isEqualTo(40);
                return;
            }
        }
    }

    @Test
    void dictionaryStepUpInsideViewportLeavesScrollAlone() {
        // A single up-arrow that stays inside the current viewport should
        // not slide the visible rows: `scrollTop` is preserved.
        dev.hardwood.cli.dive.internal.Keys.observeViewport(10);
        for (int rg = 0; rg < model.rowGroupCount(); rg++) {
            for (int c = 0; c < model.rowGroup(rg).columns().size(); c++) {
                if (model.chunk(rg, c).metaData().dictionaryPageOffset() == null) {
                    continue;
                }
                NavigationStack stack = rooted(new ScreenState.DictionaryView(
                        rg, c, 45, false, "", false, true, true, 41));

                DictionaryScreen.handle(key(KeyCode.UP), model, stack);

                ScreenState.DictionaryView top = (ScreenState.DictionaryView) stack.top();
                assertThat(top.selection()).isEqualTo(44);
                assertThat(top.scrollTop()).isEqualTo(41);
                return;
            }
        }
    }

    @Test
    void dataPreviewGReloadsAtRowZeroShiftGAtEnd() {
        int pageSize = 10;
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, pageSize);
        NavigationStack stack = rooted(initial);

        DataPreviewScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'G'), model, stack);

        long total = model.facts().totalRows();
        long expectedFirst = Math.max(0, total - pageSize);
        ScreenState.DataPreview atEnd = (ScreenState.DataPreview) stack.top();
        assertThat(atEnd.firstRow()).isEqualTo(expectedFirst);
        // Selection must point at the actual last row of the dataset, not the
        // first row of the last page (issue #400).
        assertThat(atEnd.selectedRow()).isEqualTo(atEnd.rows().size() - 1);
        assertThat(atEnd.firstRow() + atEnd.selectedRow()).isEqualTo(total - 1);

        DataPreviewScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'g'), model, stack);

        ScreenState.DataPreview atStart = (ScreenState.DataPreview) stack.top();
        assertThat(atStart.firstRow()).isZero();
        assertThat(atStart.selectedRow()).isZero();
    }

    @Test
    void dataPreviewPageDownOnLastPageMovesSelectionToLastRow() {
        // After enough PgDn presses the cursor reaches the last row of the
        // dataset and stays there: each PgDn advances by one viewport with a
        // bottom-pinned cursor, so the cursor walks all the way to row
        // `total - 1` and subsequent PgDn presses are no-ops (issue #400
        // follow-up comment).
        int pageSize = 10;
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, pageSize);
        NavigationStack stack = rooted(initial);
        long total = model.facts().totalRows();
        long lastPageFirst = Math.max(0, total - pageSize);
        while (((ScreenState.DataPreview) stack.top()).firstRow() < lastPageFirst) {
            DataPreviewScreen.handle(key(KeyCode.PAGE_DOWN), model, stack);
        }

        DataPreviewScreen.handle(key(KeyCode.PAGE_DOWN), model, stack);

        ScreenState.DataPreview atEnd = (ScreenState.DataPreview) stack.top();
        assertThat(atEnd.firstRow()).isEqualTo(lastPageFirst);
        assertThat(atEnd.selectedRow()).isEqualTo(atEnd.rows().size() - 1);
        assertThat(atEnd.firstRow() + atEnd.selectedRow()).isEqualTo(total - 1);
    }

    @Test
    void dataPreviewPageUpOnFirstPageMovesSelectionToFirstRow() {
        // Symmetric: on the first page, PgUp can't load an earlier page, but
        // the selection should snap to row 0 rather than be a no-op.
        int pageSize = 10;
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, pageSize);
        NavigationStack stack = rooted(initial);
        DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack);
        DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack);
        DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack);
        assertThat(((ScreenState.DataPreview) stack.top()).selectedRow()).isEqualTo(3);

        DataPreviewScreen.handle(key(KeyCode.PAGE_UP), model, stack);

        ScreenState.DataPreview atStart = (ScreenState.DataPreview) stack.top();
        assertThat(atStart.firstRow()).isZero();
        assertThat(atStart.selectedRow()).isZero();
    }

    @Test
    void dataPreviewShiftGFromLastPageMovesSelectionToLastRow() {
        // Reach the last viewport via repeated PgDn (sliding window: each
        // PgDn advances `firstRow + selectedRow` by one viewport with the
        // cursor bottom-pinned), then verify that pressing G keeps the
        // cursor on the actual last row of the dataset.
        int pageSize = 10;
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, pageSize);
        NavigationStack stack = rooted(initial);
        long total = model.facts().totalRows();
        long lastPageFirst = Math.max(0, total - pageSize);
        while (((ScreenState.DataPreview) stack.top()).firstRow() < lastPageFirst) {
            DataPreviewScreen.handle(key(KeyCode.PAGE_DOWN), model, stack);
        }

        DataPreviewScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'G'), model, stack);

        ScreenState.DataPreview atEnd = (ScreenState.DataPreview) stack.top();
        assertThat(atEnd.firstRow()).isEqualTo(lastPageFirst);
        assertThat(atEnd.selectedRow()).isEqualTo(atEnd.rows().size() - 1);
        assertThat(atEnd.firstRow() + atEnd.selectedRow()).isEqualTo(total - 1);
    }

    /// `l` is wired ahead of the menu-only early return, so it works with
    /// either pane focused — the histograms it reveals are on the facts pane
    /// but the cursor is usually parked on the drill-into menu.
    @Test
    void columnChunkDetailTogglesLevelsWithLFromEitherPane() {
        NavigationStack stack = rooted(new ScreenState.ColumnChunkDetail(
                0, 0, ScreenState.ColumnChunkDetail.Pane.MENU, 0, true, false));

        ColumnChunkDetailScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'l'), model, stack);
        assertThat(((ScreenState.ColumnChunkDetail) stack.top()).levels()).isTrue();

        ColumnChunkDetailScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'l'), model, stack);
        assertThat(((ScreenState.ColumnChunkDetail) stack.top()).levels()).isFalse();

        NavigationStack onFacts = rooted(new ScreenState.ColumnChunkDetail(
                0, 0, ScreenState.ColumnChunkDetail.Pane.FACTS, 0, true, false));

        ColumnChunkDetailScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'l'), model, onFacts);
        assertThat(((ScreenState.ColumnChunkDetail) onFacts.top()).levels()).isTrue();
    }

    /// ↑/↓ mean nothing on the facts pane until it overflows, so they are
    /// given to the scroll rather than left dead. The menu keeps them when it
    /// has focus, which is why the branch is keyed on focus.
    @Test
    void columnChunkDetailScrollsFactsPaneWithArrowsAndClampsAtBothEnds() {
        Keys.observeViewport(4);
        NavigationStack stack = rooted(new ScreenState.ColumnChunkDetail(
                0, 0, ScreenState.ColumnChunkDetail.Pane.FACTS, 0, true, false));

        ColumnChunkDetailScreen.handle(key(KeyCode.DOWN), model, stack);
        assertThat(((ScreenState.ColumnChunkDetail) stack.top()).scrollTop()).isEqualTo(1);

        ColumnChunkDetailScreen.handle(key(KeyCode.UP), model, stack);
        assertThat(((ScreenState.ColumnChunkDetail) stack.top()).scrollTop()).isZero();

        // Already at the top: clamped, not negative.
        ColumnChunkDetailScreen.handle(key(KeyCode.UP), model, stack);
        assertThat(((ScreenState.ColumnChunkDetail) stack.top()).scrollTop()).isZero();

        // The bottom stops one viewport short of the end, so the last line
        // sits on the last row rather than scrolling into empty space.
        ColumnChunkDetailScreen.handle(new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'G'), model, stack);
        int bottom = ((ScreenState.ColumnChunkDetail) stack.top()).scrollTop();
        assertThat(bottom).isPositive();
        ColumnChunkDetailScreen.handle(key(KeyCode.DOWN), model, stack);
        assertThat(((ScreenState.ColumnChunkDetail) stack.top()).scrollTop()).isEqualTo(bottom);
    }

    /// With the menu focused the same keys still move the selection — the
    /// facts pane does not steal them.
    @Test
    void columnChunkDetailArrowsStillDriveTheMenuWhenItHasFocus() {
        Keys.observeViewport(4);
        NavigationStack stack = rooted(new ScreenState.ColumnChunkDetail(
                0, 0, ScreenState.ColumnChunkDetail.Pane.MENU, 0, true, false));

        ColumnChunkDetailScreen.handle(key(KeyCode.DOWN), model, stack);

        ScreenState.ColumnChunkDetail top = (ScreenState.ColumnChunkDetail) stack.top();
        assertThat(top.scrollTop()).isZero();
        assertThat(top.menuSelection()).isNotZero();
    }

    /// Toggling levels must not disturb the other flag the screen carries.
    @Test
    void columnChunkDetailLevelsAndLogicalTypesToggleIndependently() {
        NavigationStack stack = rooted(new ScreenState.ColumnChunkDetail(
                0, 0, ScreenState.ColumnChunkDetail.Pane.MENU, 0, true, false));

        ColumnChunkDetailScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'l'), model, stack);
        ColumnChunkDetailScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 't'), model, stack);

        ScreenState.ColumnChunkDetail top = (ScreenState.ColumnChunkDetail) stack.top();
        assertThat(top.levels()).isTrue();
        assertThat(top.logicalTypes()).isFalse();
    }

    @Test
    void dataPreviewTogglesLogicalTypeWithT() {
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, 3);
        NavigationStack stack = rooted(initial);
        assertThat(initial.logicalTypes()).isTrue();

        DataPreviewScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 't'), model, stack);

        assertThat(((ScreenState.DataPreview) stack.top()).logicalTypes()).isFalse();

        DataPreviewScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 't'), model, stack);

        assertThat(((ScreenState.DataPreview) stack.top()).logicalTypes()).isTrue();
    }

    @Test
    void dataPreviewEnterOpensRecordModalForSelectedRow() {
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, 5);
        NavigationStack stack = rooted(initial);

        DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack);
        DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack);
        DataPreviewScreen.handle(key(KeyCode.ENTER), model, stack);

        ScreenState.DataPreview opened = (ScreenState.DataPreview) stack.top();
        assertThat(opened.modalRow()).isEqualTo(2);

        // ↑/↓ inside the modal move the line cursor — the modalRow stays put
        // (row stepping is intentionally not available inside the modal; users
        // close it and pick another row from the table).
        assertThat(DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack)).isTrue();
        ScreenState.DataPreview moved = (ScreenState.DataPreview) stack.top();
        assertThat(moved.modalRow()).isEqualTo(2);
        assertThat(moved.modalCursorLine()).isEqualTo(1);

        // Esc closes the modal.
        DataPreviewScreen.handle(key(KeyCode.ESCAPE), model, stack);
        assertThat(((ScreenState.DataPreview) stack.top()).modalRow()).isEqualTo(-1);
    }

    @Test
    void dataPreviewRowModalCursorStopsOnScalarFieldsToo() throws Exception {
        // First row: scalar id, followed by two non-empty expandable lists.
        Path file = Path.of(getClass().getResource("/list_basic_test.parquet").getPath());
        try (ParquetModel listModel = ParquetModel.open(InputFile.of(file), file.toString())) {
            ScreenState.DataPreview initial = DataPreviewScreen.initialState(listModel, 5);
            NavigationStack stack = rooted(initial);
            // Render first, as the runtime loop does: which fields are
            // expandable depends on the width the modal is drawn at, so a
            // handler-only run would answer from a default terminal size
            // rather than the one under test.
            RenderHarness.render(new Rect(0, 0, 100, 24), initial, listModel);

            // Opening still selects the first field Enter can act on, which is
            // tags (line 1) rather than the scalar id above it.
            DataPreviewScreen.handle(key(KeyCode.ENTER), listModel, stack);
            assertThat(((ScreenState.DataPreview) stack.top()).modalCursorLine()).isEqualTo(1);

            // From there the cursor walks every line, so id is reachable.
            assertThat(DataPreviewScreen.handle(key(KeyCode.UP), listModel, stack)).isTrue();
            assertThat(((ScreenState.DataPreview) stack.top()).modalCursorLine()).isZero();
            assertThat(DataPreviewScreen.handle(key(KeyCode.UP), listModel, stack))
                    .as("already at the top, but the key is still the cursor's")
                    .isTrue();
            assertThat(((ScreenState.DataPreview) stack.top()).modalCursorLine()).isZero();

            assertThat(DataPreviewScreen.handle(key(KeyCode.DOWN), listModel, stack)).isTrue();
            assertThat(DataPreviewScreen.handle(key(KeyCode.DOWN), listModel, stack)).isTrue();
            assertThat(((ScreenState.DataPreview) stack.top()).modalCursorLine()).isEqualTo(2);
        }
    }

    @Test
    void dataPreviewRowModalEnterTogglesInlineExpansion() throws Exception {
        // Needs a record with an expandable field: id is scalar, tags and
        // scores are lists whose full value doesn't fit the collapsed line.
        Path file = Path.of(getClass().getResource("/list_basic_test.parquet").getPath());
        try (ParquetModel listModel = ParquetModel.open(InputFile.of(file), file.toString())) {
            ScreenState.DataPreview initial = DataPreviewScreen.initialState(listModel, 5);
            NavigationStack stack = rooted(initial);
            RenderHarness.render(new Rect(0, 0, 100, 24), initial, listModel);

            DataPreviewScreen.handle(key(KeyCode.ENTER), listModel, stack);
            ScreenState.DataPreview opened = (ScreenState.DataPreview) stack.top();
            assertThat(opened.modalRow()).isZero();
            assertThat(opened.modalCursorLine())
                    .as("the cursor opens on tags, the first expandable field")
                    .isEqualTo(1);
            assertThat(opened.expandedColumns()).isEmpty();

            // Enter expands the field at the cursor (column 1).
            DataPreviewScreen.handle(key(KeyCode.ENTER), listModel, stack);
            assertThat(((ScreenState.DataPreview) stack.top()).expandedColumns())
                    .containsExactly(1);

            // Enter again on the same field collapses it.
            DataPreviewScreen.handle(key(KeyCode.ENTER), listModel, stack);
            assertThat(((ScreenState.DataPreview) stack.top()).expandedColumns()).isEmpty();

            // Esc closes the row modal entirely.
            DataPreviewScreen.handle(key(KeyCode.ESCAPE), listModel, stack);
            assertThat(((ScreenState.DataPreview) stack.top()).modalRow()).isEqualTo(-1);
        }
    }

    @Test
    void dataPreviewRowModalExpandAllAndCollapseAll() {
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, 5);
        NavigationStack stack = rooted(initial);

        DataPreviewScreen.handle(key(KeyCode.ENTER), model, stack);
        int columnCount = ((ScreenState.DataPreview) stack.top()).columnNames().size();

        DataPreviewScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'e'), model, stack);
        assertThat(((ScreenState.DataPreview) stack.top()).expandedColumns())
                .hasSize(columnCount);

        DataPreviewScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'c'), model, stack);
        assertThat(((ScreenState.DataPreview) stack.top()).expandedColumns()).isEmpty();
    }

    @Test
    void dataPreviewRightScrollsColumnsOnlyWhenRoom() {
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, 2);
        NavigationStack stack = rooted(initial);

        DataPreviewScreen.handle(key(KeyCode.RIGHT), model, stack);

        // Both narrow columns fit in the default viewport, so scrolling
        // would hide data without revealing anything new.
        assertThat(((ScreenState.DataPreview) stack.top()).columnScroll()).isZero();
    }

    @Test
    void dataPreviewValidLoadHasNoError() {
        ScreenState.DataPreview state = DataPreviewScreen.initialState(model, 5);

        assertThat(state.hasError()).isFalse();
        assertThat(state.errorMessage()).isNull();
        assertThat(state.rows()).isNotEmpty();
    }

    @Test
    void dataPreviewIoErrorSurfacesAsOverlayAndBlocksNavigation() {
        ScreenState.DataPreview errorState = new ScreenState.DataPreview(
                0, 5, List.of("id"), List.of(), List.of(),
                0, 0, -1, true, java.util.Set.of(), 0, 0, "Corrupt page: bad checksum");
        NavigationStack stack = rooted(errorState);

        assertThat(errorState.hasError()).isTrue();
        assertThat(DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack)).isFalse();
        assertThat(DataPreviewScreen.handle(key(KeyCode.PAGE_DOWN), model, stack)).isFalse();
        assertThat(DataPreviewScreen.handle(key(KeyCode.ESCAPE), model, stack)).isFalse();
        assertThat(stack.top()).isSameAs(errorState);
    }

    @Test
    void columnChunkDetailDictionaryEnabledWhenChunkHasDictionary() {
        // Walk chunks until we find one with a dictionary; if none, test is vacuous.
        for (int rg = 0; rg < model.rowGroupCount(); rg++) {
            for (int c = 0; c < model.rowGroup(rg).columns().size(); c++) {
                if (model.chunk(rg, c).metaData().dictionaryPageOffset() != null) {
                    NavigationStack stack = rooted(new ScreenState.ColumnChunkDetail(
                            rg, c, ScreenState.ColumnChunkDetail.Pane.MENU,
                            ColumnChunkDetailScreen.MenuItem.DICTIONARY.ordinal(), true, false));

                    ColumnChunkDetailScreen.handle(key(KeyCode.ENTER), model, stack);

                    assertThat(stack.top()).isInstanceOf(ScreenState.DictionaryView.class);
                    return;
                }
            }
        }
    }

    // --- Phase 4 ---

    @Test
    void schemaInitialIsCollapsed() {
        ScreenState.Schema initial = ScreenState.Schema.initial();

        assertThat(initial.selection()).isZero();
        assertThat(initial.expanded()).isEmpty();
    }

    @Test
    void dictionarySlashEntersSearchMode() {
        NavigationStack stack = rooted(new ScreenState.DictionaryView(0, 0, 0, false, "", false, false, true));

        DictionaryScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, '/'), model, stack);

        ScreenState.DictionaryView top = (ScreenState.DictionaryView) stack.top();
        assertThat(top.searching()).isTrue();
    }

    @Test
    void dictionarySearchAppendsCharToFilter() {
        NavigationStack stack = rooted(new ScreenState.DictionaryView(0, 0, 0, false, "", true, false, true));

        DictionaryScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'a'), model, stack);

        assertThat(((ScreenState.DictionaryView) stack.top()).filter()).isEqualTo("a");
    }

    @Test
    void dictionarySearchEscClearsFilter() {
        NavigationStack stack = rooted(new ScreenState.DictionaryView(0, 0, 0, false, "abc", true, false, true));

        DictionaryScreen.handle(key(KeyCode.ESCAPE), model, stack);

        ScreenState.DictionaryView top = (ScreenState.DictionaryView) stack.top();
        assertThat(top.searching()).isFalse();
        assertThat(top.filter()).isEmpty();
    }

    @Test
    void dictionarySearchEnterKeepsFilter() {
        NavigationStack stack = rooted(new ScreenState.DictionaryView(0, 0, 0, false, "abc", true, false, true));

        DictionaryScreen.handle(key(KeyCode.ENTER), model, stack);

        ScreenState.DictionaryView top = (ScreenState.DictionaryView) stack.top();
        assertThat(top.searching()).isFalse();
        assertThat(top.filter()).isEqualTo("abc");
    }

    @Test
    void dictionaryBackspaceTrimsFilter() {
        NavigationStack stack = rooted(new ScreenState.DictionaryView(0, 0, 0, false, "abc", true, false, true));

        DictionaryScreen.handle(key(KeyCode.BACKSPACE), model, stack);

        assertThat(((ScreenState.DictionaryView) stack.top()).filter()).isEqualTo("ab");
    }

    @Test
    void schemaSlashEntersSearchMode() {
        NavigationStack stack = rooted(ScreenState.Schema.initial());

        SchemaScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, '/'), model, stack);

        assertThat(((ScreenState.Schema) stack.top()).searching()).isTrue();
    }

    @Test
    void schemaSearchAppendsCharsToFilter() {
        NavigationStack stack = rooted(new ScreenState.Schema(0, java.util.Set.of(), "", true));

        SchemaScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'v'), model, stack);
        SchemaScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'a'), model, stack);
        SchemaScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'l'), model, stack);

        assertThat(((ScreenState.Schema) stack.top()).filter()).isEqualTo("val");
        // The filter's effect on visibleRows is covered observably by
        // schemaSearchEnterDrillsIntoColumnAcrossRowGroups below.
    }

    @Test
    void schemaSearchEscClearsFilter() {
        NavigationStack stack = rooted(
                new ScreenState.Schema(0, java.util.Set.of(), "abc", true));

        SchemaScreen.handle(key(KeyCode.ESCAPE), model, stack);

        ScreenState.Schema top = (ScreenState.Schema) stack.top();
        assertThat(top.searching()).isFalse();
        assertThat(top.filter()).isEmpty();
    }

    @Test
    void schemaSearchEnterDrillsIntoColumnAcrossRowGroups() {
        // Filter narrows to "value" (columnIndex 1 in the fixture), then pressing Enter
        // while not in search-edit mode drills into the cross-RG view.
        NavigationStack stack = rooted(
                new ScreenState.Schema(0, java.util.Set.of(), "value", false));

        SchemaScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.ColumnAcrossRowGroups.class);
        assertThat(((ScreenState.ColumnAcrossRowGroups) stack.top()).columnIndex()).isEqualTo(1);
    }

    @Test
    void columnIndexSlashEntersSearchMode() {
        NavigationStack stack = rooted(new ScreenState.ColumnIndexView(0, 0, 0, "", false, true, false));

        ColumnIndexScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, '/'), model, stack);

        assertThat(((ScreenState.ColumnIndexView) stack.top()).searching()).isTrue();
    }

    @Test
    void columnIndexSearchAppendsCharsToFilter() {
        NavigationStack stack = rooted(new ScreenState.ColumnIndexView(0, 0, 0, "", true, true, false));

        ColumnIndexScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, '5'), model, stack);

        assertThat(((ScreenState.ColumnIndexView) stack.top()).filter()).isEqualTo("5");
    }

    @Test
    void columnIndexSearchEscClearsFilter() {
        NavigationStack stack = rooted(new ScreenState.ColumnIndexView(0, 0, 0, "abc", true, true, false));

        ColumnIndexScreen.handle(key(KeyCode.ESCAPE), model, stack);

        ScreenState.ColumnIndexView top = (ScreenState.ColumnIndexView) stack.top();
        assertThat(top.searching()).isFalse();
        assertThat(top.filter()).isEmpty();
    }

    @Test
    void schemaRightOnPrimitiveIsNoop() {
        NavigationStack stack = rooted(ScreenState.Schema.initial());

        SchemaScreen.handle(key(KeyCode.RIGHT), model, stack);

        assertThat(((ScreenState.Schema) stack.top()).expanded()).isEmpty();
    }

    @Test
    void dictionaryEnterOpensModalAndCancelCloses() {
        // Find a chunk with a dictionary to test against.
        for (int rg = 0; rg < model.rowGroupCount(); rg++) {
            for (int c = 0; c < model.rowGroup(rg).columns().size(); c++) {
                if (model.chunk(rg, c).metaData().dictionaryPageOffset() != null) {
                    NavigationStack stack = rooted(new ScreenState.DictionaryView(rg, c, 0, false, "", false, false, true));

                    DictionaryScreen.handle(key(KeyCode.ENTER), model, stack);
                    assertThat(((ScreenState.DictionaryView) stack.top()).modalOpen()).isTrue();

                    DictionaryScreen.handle(key(KeyCode.ESCAPE), model, stack);
                    assertThat(((ScreenState.DictionaryView) stack.top()).modalOpen()).isFalse();
                    return;
                }
            }
        }
    }

    @Test
    void footerEnterOnColumnAnchorDrillsIntoFileIndexes() {
        // Initial cursor is the Column anchor; Enter drills.
        NavigationStack stack = rooted(FooterScreen.initialState(model));
        FooterScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.FileIndexes.class);
        assertThat(((ScreenState.FileIndexes) stack.top()).kind())
                .isEqualTo(ScreenState.FileIndexes.Kind.COLUMN);

        FileIndexesScreen.handle(key(KeyCode.ENTER), model, stack);
        assertThat(stack.top()).isInstanceOf(ScreenState.ColumnIndexView.class);
    }

    @Test
    void pagesTogglesLogicalTypesWithT() {
        NavigationStack stack = rooted(new ScreenState.Pages(0, 0, 0, false, true));

        PagesScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 't'), model, stack);

        assertThat(((ScreenState.Pages) stack.top()).logicalTypes()).isFalse();
    }

    @Test
    void footerDownSkipsTheDisabledDictionaryAnchor() {
        // The fixture has no chunks with a dictionary, so past the offset
        // anchor the only stop left is the end of the body — ↓ does not pause
        // on a section the file does not have.
        NavigationStack stack = rooted(FooterScreen.initialState(model));
        FooterScreen.handle(key(KeyCode.DOWN), model, stack);
        int offsetAnchor = ((ScreenState.Footer) stack.top()).cursorRow();

        assertThat(FooterScreen.handle(key(KeyCode.DOWN), model, stack)).isTrue();
        ScreenState.Footer end = (ScreenState.Footer) stack.top();
        assertThat(end.cursorRow()).isGreaterThan(offsetAnchor);
        assertThat(FooterScreen.handle(key(KeyCode.ENTER), model, stack))
                .as("the end of the body is a stop, not an anchor")
                .isFalse();
    }

    @Test
    void footerDownJumpsToTheOffsetAnchorThenEnterDrills() {
        NavigationStack stack = rooted(FooterScreen.initialState(model));
        FooterScreen.handle(key(KeyCode.DOWN), model, stack);

        FooterScreen.handle(key(KeyCode.ENTER), model, stack);
        assertThat(stack.top()).isInstanceOf(ScreenState.FileIndexes.class);
        assertThat(((ScreenState.FileIndexes) stack.top()).kind())
                .isEqualTo(ScreenState.FileIndexes.Kind.OFFSET);
    }

    @Test
    void footerPagingMovesTheCursorAndIsNotUndoneByTheNextFrame() {
        // The body used to re-derive its offset from the anchor on every
        // render, so anything paged to was discarded on the next repaint and
        // the top of the body could not be reached at all.
        NavigationStack stack = rooted(FooterScreen.initialState(model));
        RenderHarness.render(new Rect(0, 0, 120, 16), stack.top(), model);

        assertThat(FooterScreen.handle(key(KeyCode.PAGE_DOWN), model, stack)).isTrue();
        ScreenState.Footer paged = (ScreenState.Footer) stack.top();
        assertThat(paged.cursorRow()).isPositive();

        RenderHarness.render(new Rect(0, 0, 120, 16), paged, model);
        assertThat(((ScreenState.Footer) stack.top()).cursorRow())
                .as("rendering does not move the reader back to the anchor")
                .isEqualTo(paged.cursorRow());

        assertThat(FooterScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'g'), model, stack)).isTrue();
        assertThat(((ScreenState.Footer) stack.top()).cursorRow()).isZero();
        assertThat(RenderHarness.render(new Rect(0, 0, 120, 16), stack.top(), model)
                .contains("File layout"))
                .as("g reaches the head of the body")
                .isTrue();
    }

    @Test
    void dictionaryConfirmPromptShownWhenChunkExceedsCap() throws Exception {
        // Force the cap below the chunk size so the screen lands on the
        // confirm prompt instead of auto-loading. dictionary_with_crc
        // has an actual dictionary on column 1; the test column doesn't
        // matter for the gating logic — only its compressed-bytes size
        // vs. the cap.
        Path file = Path.of(getClass().getResource("/dictionary_with_crc.parquet").getPath());
        try (ParquetModel m = ParquetModel.open(InputFile.of(file), file.toString())) {
            long chunkBytes = m.dictionaryChunkBytes(0, 1);
            assertThat(chunkBytes).isPositive();
            m.setDictionaryReadCapBytes(1);  // smaller than any real chunk

            NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
            stack.push(new ScreenState.DictionaryView(0, 1, 0, false, "", false, false, true));

            // Before opt-in, ↓ is ignored — the screen is on the prompt,
            // not the table.
            boolean handledDown = DictionaryScreen.handle(key(KeyCode.DOWN), m, stack);
            assertThat(handledDown).isFalse();
            assertThat(((ScreenState.DictionaryView) stack.top()).loadConfirmed()).isFalse();

            // Enter opts in; loadConfirmed flips and subsequent rendering
            // would call dictionaryForced.
            boolean handledEnter = DictionaryScreen.handle(key(KeyCode.ENTER), m, stack);
            assertThat(handledEnter).isTrue();
            assertThat(((ScreenState.DictionaryView) stack.top()).loadConfirmed()).isTrue();
        }
    }

    @Test
    void dictionaryConfirmPromptSkippedWhenChunkUnderCap() throws Exception {
        // Default cap (16 MiB) is well above the fixture chunk; the screen
        // proceeds straight to the table without prompting.
        Path file = Path.of(getClass().getResource("/dictionary_with_crc.parquet").getPath());
        try (ParquetModel m = ParquetModel.open(InputFile.of(file), file.toString())) {
            NavigationStack stack = new NavigationStack(ScreenState.Overview.initial());
            stack.push(new ScreenState.DictionaryView(0, 1, 0, false, "", false, false, true));

            // ↓ is claimed by the table handler (loadConfirmed stays false
            // because the cap was never exceeded — the screen never
            // entered prompt mode in the first place).
            boolean handled = DictionaryScreen.handle(key(KeyCode.DOWN), m, stack);

            assertThat(handled).isTrue();
            assertThat(((ScreenState.DictionaryView) stack.top()).loadConfirmed()).isFalse();
        }
    }

    /// The cell caps a 21-byte WKB Point's hex at the cell budget while the
    /// modal shows it whole, so `Enter` has something to reveal. The keybar
    /// hint, the `▶` marker and the key handler all read the same gate, so a
    /// hint that offers `Enter` is the observable proof they agree.
    @Test
    void columnIndexOffersEnterForAnOpaqueBinaryBound() throws Exception {
        Path path = Path.of(getClass().getResource("/nested_binary_test.parquet").getPath());
        try (ParquetModel binaryModel = ParquetModel.open(InputFile.of(path), path.toString())) {
            int blob = columnIndexOf(binaryModel, "blob");
            ScreenState.ColumnIndexView state =
                    new ScreenState.ColumnIndexView(0, blob, 0, "", false, true, false);

            assertThat(ColumnIndexScreen.keybarKeys(state, binaryModel))
                    .contains("[Enter] view min/max");

            NavigationStack stack = rooted(state);
            ColumnIndexScreen.handle(key(KeyCode.ENTER), binaryModel, stack);
            assertThat(((ScreenState.ColumnIndexView) stack.top()).modalOpen()).isTrue();
        }
    }

    /// The `▶` marker, the keybar hint and the key handler read one gate, so
    /// what the screen advertises is what `Enter` does. Both arms matter: a
    /// 3072-byte payload hexes far past the row and opens, while a 21-byte WKB
    /// Point hexes to 44 characters, fits the row whole, and must not — a modal
    /// there would redraw what the row already shows.
    @Test
    void dictionaryOffersEnterOnlyForAnEntryTheRowHadToTruncate() throws Exception {
        Path path = Path.of(getClass().getResource("/nested_binary_test.parquet").getPath());
        try (ParquetModel binaryModel = ParquetModel.open(InputFile.of(path), path.toString())) {
            assertDictionaryExpandable(binaryModel, "var.value", true);
            assertDictionaryExpandable(binaryModel, "blob", false);
        }
    }

    private void assertDictionaryExpandable(ParquetModel model, String column, boolean expandable) {
        ScreenState.DictionaryView state = new ScreenState.DictionaryView(
                0, columnIndexOf(model, column), 0, false, "", false, true, true);

        assertThat(DictionaryScreen.keybarKeys(state, model))
                .as("keybar hint for %s", column)
                .matches(hints -> hints.contains("[Enter] view full value") == expandable);

        NavigationStack stack = rooted(state);
        DictionaryScreen.handle(key(KeyCode.ENTER), model, stack);
        assertThat(((ScreenState.DictionaryView) stack.top()).modalOpen())
                .as("Enter opens the modal for %s", column)
                .isEqualTo(expandable);
    }

    private static int columnIndexOf(ParquetModel model, String name) {
        for (int i = 0; i < model.schema().getColumns().size(); i++) {
            if (model.schema().getColumn(i).fieldPath().toString().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("no such column: " + name);
    }

    private NavigationStack rooted(ScreenState child) {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU, 0, 0, false, 0));
        stack.push(child);
        return stack;
    }

    private static KeyEvent key(KeyCode code) {
        return new KeyEvent(code, KeyModifiers.NONE, '\0');
    }
}
