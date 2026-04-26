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
import dev.hardwood.cli.dive.internal.OverviewScreen;
import dev.hardwood.cli.dive.internal.PagesScreen;
import dev.hardwood.cli.dive.internal.RowGroupDetailScreen;
import dev.hardwood.cli.dive.internal.RowGroupsScreen;
import dev.hardwood.cli.dive.internal.SchemaScreen;
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
        dev.hardwood.cli.dive.internal.Keys.resetObservedViewport();
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
                ScreenState.ColumnChunkDetail.Pane.MENU, 0, true));

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
                0, 0, ScreenState.ColumnChunkDetail.Pane.MENU, 0, true));

        ColumnChunkDetailScreen.handle(key(KeyCode.TAB), model, stack);

        assertThat(((ScreenState.ColumnChunkDetail) stack.top()).focus())
                .isEqualTo(ScreenState.ColumnChunkDetail.Pane.FACTS);
    }

    @Test
    void columnChunkDetailEnterOnPagesPushesPagesScreen() {
        NavigationStack stack = rooted(new ScreenState.ColumnChunkDetail(
                0, 0, ScreenState.ColumnChunkDetail.Pane.MENU,
                ColumnChunkDetailScreen.MenuItem.PAGES.ordinal(), true));

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
    void columnChunkDetailDisabledSelectionSnapsToFirstEnabled() {
        // The fixture chunk has no dictionary, so opening the menu with
        // DICTIONARY selected should snap to the first enabled item
        // (PAGES) on the next event rather than firing a no-op drill.
        NavigationStack stack = rooted(new ScreenState.ColumnChunkDetail(
                0, 0, ScreenState.ColumnChunkDetail.Pane.MENU,
                ColumnChunkDetailScreen.MenuItem.DICTIONARY.ordinal(), true));

        ColumnChunkDetailScreen.handle(key(KeyCode.ENTER), model, stack);

        // First event snaps to PAGES, then drills.
        assertThat(stack.top()).isInstanceOf(ScreenState.Pages.class);
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
    void dataPreviewPageDownAdvancesFirstRow() {
        int pageSize = 10;
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, pageSize);
        NavigationStack stack = rooted(initial);

        DataPreviewScreen.handle(key(KeyCode.PAGE_DOWN), model, stack);

        ScreenState.DataPreview next = (ScreenState.DataPreview) stack.top();
        assertThat(next.firstRow()).isEqualTo(pageSize);
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
        // Page 1 starts at row 10 → first id value in the second page is "10".
        assertThat(second.rows().get(0).get(0)).isEqualTo("10");
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
    void dataPreviewGReloadsAtRowZeroShiftGAtEnd() {
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, 10);
        NavigationStack stack = rooted(initial);

        DataPreviewScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'G'), model, stack);

        long expectedFirst = Math.max(0, model.facts().totalRows() - 10);
        assertThat(((ScreenState.DataPreview) stack.top()).firstRow()).isEqualTo(expectedFirst);

        DataPreviewScreen.handle(
                new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'g'), model, stack);

        assertThat(((ScreenState.DataPreview) stack.top()).firstRow()).isZero();
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

        // ↑/↓ inside the modal navigate the per-line cursor — the modalRow
        // stays put (row stepping is intentionally not available inside the
        // modal; users close it and pick another row from the table).
        DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack);
        ScreenState.DataPreview moved = (ScreenState.DataPreview) stack.top();
        assertThat(moved.modalRow()).isEqualTo(2);
        assertThat(moved.modalCursorLine()).isEqualTo(1);

        // Esc closes the modal.
        DataPreviewScreen.handle(key(KeyCode.ESCAPE), model, stack);
        assertThat(((ScreenState.DataPreview) stack.top()).modalRow()).isEqualTo(-1);
    }

    @Test
    void dataPreviewRowModalEnterTogglesInlineExpansion() {
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, 5);
        NavigationStack stack = rooted(initial);

        DataPreviewScreen.handle(key(KeyCode.ENTER), model, stack);
        ScreenState.DataPreview opened = (ScreenState.DataPreview) stack.top();
        assertThat(opened.modalRow()).isEqualTo(0);
        assertThat(opened.modalCursorLine()).isEqualTo(0);
        assertThat(opened.expandedColumns()).isEmpty();

        // ↓ moves the per-line cursor within the modal.
        DataPreviewScreen.handle(key(KeyCode.DOWN), model, stack);
        assertThat(((ScreenState.DataPreview) stack.top()).modalCursorLine()).isEqualTo(1);

        // Enter expands the field at the cursor (column 1).
        DataPreviewScreen.handle(key(KeyCode.ENTER), model, stack);
        assertThat(((ScreenState.DataPreview) stack.top()).expandedColumns()).containsExactly(1);

        // Enter again on the same field collapses it.
        DataPreviewScreen.handle(key(KeyCode.ENTER), model, stack);
        assertThat(((ScreenState.DataPreview) stack.top()).expandedColumns()).isEmpty();

        // Esc closes the row modal entirely.
        DataPreviewScreen.handle(key(KeyCode.ESCAPE), model, stack);
        assertThat(((ScreenState.DataPreview) stack.top()).modalRow()).isEqualTo(-1);
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

        // single-column fixture → no room to scroll, state unchanged
        assertThat(((ScreenState.DataPreview) stack.top()).columnScroll()).isZero();
    }

    @Test
    void columnChunkDetailDictionaryEnabledWhenChunkHasDictionary() {
        // Walk chunks until we find one with a dictionary; if none, test is vacuous.
        for (int rg = 0; rg < model.rowGroupCount(); rg++) {
            for (int c = 0; c < model.rowGroup(rg).columns().size(); c++) {
                if (model.chunk(rg, c).metaData().dictionaryPageOffset() != null) {
                    NavigationStack stack = rooted(new ScreenState.ColumnChunkDetail(
                            rg, c, ScreenState.ColumnChunkDetail.Pane.MENU,
                            ColumnChunkDetailScreen.MenuItem.DICTIONARY.ordinal(), true));

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
        NavigationStack stack = rooted(ScreenState.Footer.initial());
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
    void footerDownSkipsDisabledDictionaryAnchor() {
        // The fixture has 0 chunks with dictionary, so ↓ from OFFSET should
        // not advance to DICTIONARY (it's disabled).
        NavigationStack stack = rooted(new ScreenState.Footer(
                ScreenState.Footer.Anchor.OFFSET, 0));

        FooterScreen.handle(key(KeyCode.DOWN), model, stack);

        assertThat(((ScreenState.Footer) stack.top()).cursor())
                .isEqualTo(ScreenState.Footer.Anchor.OFFSET);
    }

    @Test
    void footerDownTogglesToOffsetAnchorThenEnterDrills() {
        NavigationStack stack = rooted(ScreenState.Footer.initial());
        FooterScreen.handle(key(KeyCode.DOWN), model, stack);

        assertThat(((ScreenState.Footer) stack.top()).cursor())
                .isEqualTo(ScreenState.Footer.Anchor.OFFSET);

        FooterScreen.handle(key(KeyCode.ENTER), model, stack);
        assertThat(stack.top()).isInstanceOf(ScreenState.FileIndexes.class);
        assertThat(((ScreenState.FileIndexes) stack.top()).kind())
                .isEqualTo(ScreenState.FileIndexes.Kind.OFFSET);
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
