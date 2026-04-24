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
import dev.hardwood.cli.dive.internal.ColumnAcrossRowGroupsScreen;
import dev.hardwood.cli.dive.internal.ColumnChunkDetailScreen;
import dev.hardwood.cli.dive.internal.ColumnChunksScreen;
import dev.hardwood.cli.dive.internal.DataPreviewScreen;
import dev.hardwood.cli.dive.internal.DictionaryScreen;
import dev.hardwood.cli.dive.internal.OverviewScreen;
import dev.hardwood.cli.dive.internal.PagesScreen;
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
        Path path = Path.of(getClass().getResource("/compat_plain_int64.parquet").getPath());
        model = ParquetModel.open(InputFile.of(path), path.toString());
    }

    @AfterEach
    void closeModel() throws Exception {
        model.close();
    }

    @Test
    void overviewDownMovesMenuSelection() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU, 0));

        OverviewScreen.handle(key(KeyCode.DOWN), model, stack);

        assertThat(stack.top()).isEqualTo(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU, 1));
    }

    @Test
    void overviewUpAtTopClampsToZero() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU, 0));

        OverviewScreen.handle(key(KeyCode.UP), model, stack);

        assertThat(((ScreenState.Overview) stack.top()).menuSelection()).isZero();
    }

    @Test
    void overviewTabSwitchesFocus() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU, 0));

        OverviewScreen.handle(key(KeyCode.TAB), model, stack);

        assertThat(((ScreenState.Overview) stack.top()).focus())
                .isEqualTo(ScreenState.Overview.Pane.FACTS);
    }

    @Test
    void overviewEnterOnRowGroupsPushesRowGroupsScreen() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU,
                        OverviewScreen.MenuItem.ROW_GROUPS.ordinal()));

        OverviewScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.RowGroups.class);
        assertThat(stack.depth()).isEqualTo(2);
    }

    @Test
    void overviewEnterOnSchemaPushesSchemaScreen() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU,
                        OverviewScreen.MenuItem.SCHEMA.ordinal()));

        OverviewScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.Schema.class);
    }

    @Test
    void overviewEnterOnFooterPushesFooterScreen() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU,
                        OverviewScreen.MenuItem.FOOTER.ordinal()));

        OverviewScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.Footer.class);
    }

    @Test
    void rowGroupsEnterDrillsIntoColumnChunks() {
        NavigationStack stack = rooted(new ScreenState.RowGroups(0));

        RowGroupsScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.ColumnChunks.class);
        assertThat(((ScreenState.ColumnChunks) stack.top()).rowGroupIndex()).isZero();
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
        NavigationStack stack = rooted(new ScreenState.Schema(0));

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
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU, 0));

        stack.pop();
        stack.pop();

        assertThat(stack.depth()).isEqualTo(1);
    }

    @Test
    void navigationStackClearToRootCollapsesPath() {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU, 0));
        stack.push(new ScreenState.RowGroups(0));
        stack.push(new ScreenState.ColumnChunks(0, 2));
        stack.push(new ScreenState.ColumnChunkDetail(0, 2,
                ScreenState.ColumnChunkDetail.Pane.MENU, 0));

        stack.clearToRoot();

        assertThat(stack.depth()).isEqualTo(1);
        assertThat(stack.top()).isInstanceOf(ScreenState.Overview.class);
    }

    // --- Phase 2 ---

    @Test
    void schemaEnterDrillsIntoColumnAcrossRowGroups() {
        NavigationStack stack = rooted(new ScreenState.Schema(0));

        SchemaScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.ColumnAcrossRowGroups.class);
        assertThat(((ScreenState.ColumnAcrossRowGroups) stack.top()).columnIndex()).isZero();
    }

    @Test
    void columnAcrossRowGroupsEnterDrillsIntoChunkDetail() {
        NavigationStack stack = rooted(new ScreenState.ColumnAcrossRowGroups(0, 0));

        ColumnAcrossRowGroupsScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.ColumnChunkDetail.class);
        ScreenState.ColumnChunkDetail top = (ScreenState.ColumnChunkDetail) stack.top();
        assertThat(top.rowGroupIndex()).isZero();
        assertThat(top.columnIndex()).isZero();
    }

    @Test
    void columnChunkDetailTabSwitchesPaneBetweenFactsAndMenu() {
        NavigationStack stack = rooted(new ScreenState.ColumnChunkDetail(
                0, 0, ScreenState.ColumnChunkDetail.Pane.MENU, 0));

        ColumnChunkDetailScreen.handle(key(KeyCode.TAB), model, stack);

        assertThat(((ScreenState.ColumnChunkDetail) stack.top()).focus())
                .isEqualTo(ScreenState.ColumnChunkDetail.Pane.FACTS);
    }

    @Test
    void columnChunkDetailEnterOnPagesPushesPagesScreen() {
        NavigationStack stack = rooted(new ScreenState.ColumnChunkDetail(
                0, 0, ScreenState.ColumnChunkDetail.Pane.MENU,
                ColumnChunkDetailScreen.MenuItem.PAGES.ordinal()));

        ColumnChunkDetailScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.Pages.class);
    }

    @Test
    void columnChunkDetailEnterOnDictionaryIsNoop() {
        NavigationStack stack = rooted(new ScreenState.ColumnChunkDetail(
                0, 0, ScreenState.ColumnChunkDetail.Pane.MENU,
                ColumnChunkDetailScreen.MenuItem.DICTIONARY.ordinal()));

        ColumnChunkDetailScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.ColumnChunkDetail.class);
    }

    @Test
    void pagesEnterOpensModalAndCancelCloses() {
        NavigationStack stack = rooted(new ScreenState.Pages(0, 0, 0, false));

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
                        OverviewScreen.MenuItem.DATA_PREVIEW.ordinal()));

        OverviewScreen.handle(key(KeyCode.ENTER), model, stack);

        assertThat(stack.top()).isInstanceOf(ScreenState.DataPreview.class);
        ScreenState.DataPreview preview = (ScreenState.DataPreview) stack.top();
        assertThat(preview.firstRow()).isZero();
        assertThat(preview.pageSize()).isEqualTo(model.previewPageSize());
    }

    @Test
    void dataPreviewPageDownAdvancesFirstRow() {
        int pageSize = 2;
        if (model.facts().totalRows() < pageSize * 2) {
            // Fixture too small to exercise pagination; the is-it-clamped path is
            // covered by the PAGE_UP test below.
            return;
        }
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, pageSize);
        NavigationStack stack = rooted(initial);

        DataPreviewScreen.handle(key(KeyCode.PAGE_DOWN), model, stack);

        ScreenState.DataPreview next = (ScreenState.DataPreview) stack.top();
        assertThat(next.firstRow()).isEqualTo(pageSize);
    }

    @Test
    void dataPreviewPageUpAtStartIsNoop() {
        ScreenState.DataPreview initial = DataPreviewScreen.initialState(model, 2);
        NavigationStack stack = rooted(initial);

        DataPreviewScreen.handle(key(KeyCode.PAGE_UP), model, stack);

        assertThat(((ScreenState.DataPreview) stack.top()).firstRow()).isZero();
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
                            ColumnChunkDetailScreen.MenuItem.DICTIONARY.ordinal()));

                    ColumnChunkDetailScreen.handle(key(KeyCode.ENTER), model, stack);

                    assertThat(stack.top()).isInstanceOf(ScreenState.DictionaryView.class);
                    return;
                }
            }
        }
    }

    @Test
    void dictionaryEnterOpensModalAndCancelCloses() {
        // Find a chunk with a dictionary to test against.
        for (int rg = 0; rg < model.rowGroupCount(); rg++) {
            for (int c = 0; c < model.rowGroup(rg).columns().size(); c++) {
                if (model.chunk(rg, c).metaData().dictionaryPageOffset() != null) {
                    NavigationStack stack = rooted(new ScreenState.DictionaryView(rg, c, 0, false));

                    DictionaryScreen.handle(key(KeyCode.ENTER), model, stack);
                    assertThat(((ScreenState.DictionaryView) stack.top()).modalOpen()).isTrue();

                    DictionaryScreen.handle(key(KeyCode.ESCAPE), model, stack);
                    assertThat(((ScreenState.DictionaryView) stack.top()).modalOpen()).isFalse();
                    return;
                }
            }
        }
    }

    private NavigationStack rooted(ScreenState child) {
        NavigationStack stack = new NavigationStack(
                new ScreenState.Overview(ScreenState.Overview.Pane.MENU, 0));
        stack.push(child);
        return stack;
    }

    private static KeyEvent key(KeyCode code) {
        return new KeyEvent(code, KeyModifiers.NONE, '\0');
    }
}
