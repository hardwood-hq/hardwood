/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive;

/// State of one screen in the `hardwood dive` navigation stack.
///
/// Each variant is an immutable record carrying only the state specific to that screen
/// (cursor position, parent-screen context). Display strings and tamboui widgets are
/// derived from these records and the [ParquetModel], not stored here.
public sealed interface ScreenState {

    /// Landing screen. Two panes: file-facts (left) and drill-into menu (right).
    record Overview(Pane focus, int menuSelection) implements ScreenState {
        public enum Pane { FACTS, MENU }
    }

    /// Flat list of leaf columns. Enter drills into [ColumnAcrossRowGroups] for
    /// the selected column. Tree expansion ships in phase 4.
    record Schema(int selection) implements ScreenState {}

    /// Row groups in the file, one row per group.
    record RowGroups(int selection) implements ScreenState {}

    /// Column chunks within one row group.
    record ColumnChunks(int rowGroupIndex, int selection) implements ScreenState {}

    /// All metadata for one `(rowGroup, column)` chunk. `focus` chooses between
    /// the facts pane and the drill-into menu (phase 2 onwards).
    record ColumnChunkDetail(int rowGroupIndex, int columnIndex, Pane focus, int menuSelection)
            implements ScreenState {
        public enum Pane { FACTS, MENU }
    }

    /// List of pages inside one column chunk; Enter opens a modal page-header view.
    record Pages(int rowGroupIndex, int columnIndex, int selection, boolean modalOpen)
            implements ScreenState {
    }

    /// Per-page statistics view for one column chunk.
    record ColumnIndexView(int rowGroupIndex, int columnIndex, int selection) implements ScreenState {
    }

    /// Page-location view for one column chunk.
    record OffsetIndexView(int rowGroupIndex, int columnIndex, int selection) implements ScreenState {
    }

    /// Raw footer layout: file size, footer offset/length, aggregate index bytes.
    record Footer() implements ScreenState {
    }

    /// Cross-row-group view of one leaf column. `selection` drills into
    /// [ColumnChunkDetail] for the corresponding `(rowGroup, column)`.
    record ColumnAcrossRowGroups(int columnIndex, int selection) implements ScreenState {
    }

    /// Dictionary entries for one column chunk. `selection` is the entry index;
    /// `modalOpen` flags the full-value modal that opens on Enter.
    record DictionaryView(int rowGroupIndex, int columnIndex, int selection, boolean modalOpen)
            implements ScreenState {
    }

    /// Projected rows. `firstRow` is the 0-based absolute index of the first row
    /// currently displayed; `pageSize` controls how many rows fit on a page; the
    /// loaded `rows` are pre-formatted strings per column. `columnNames`
    /// duplicates the projection so renderers don't re-derive it.
    record DataPreview(
            long firstRow,
            int pageSize,
            java.util.List<String> columnNames,
            java.util.List<java.util.List<String>> rows,
            int columnScroll)
            implements ScreenState {
        public DataPreview {
            columnNames = java.util.List.copyOf(columnNames);
            rows = java.util.List.copyOf(rows);
        }
    }
}
