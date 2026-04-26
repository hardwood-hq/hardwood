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
    /// `kvSelection` is the row index within the key/value metadata list in the
    /// facts pane (0 = first KV entry); `kvModalOpen` is true when the
    /// full-value modal is showing for the selected KV entry. `kvModalScroll`
    /// is the line offset into the modal contents — used when the formatted
    /// value (e.g. an Arrow hex dump) overflows the modal viewport.
    record Overview(Pane focus, int menuSelection, int kvSelection,
                    boolean kvModalOpen, int kvModalScroll)
            implements ScreenState {
        public enum Pane { FACTS, MENU }

        /// Default state: menu pane focused, selection at 0, no KV interaction.
        public static Overview initial() {
            return new Overview(Pane.MENU, 0, 0, false, 0);
        }
    }

    /// Expandable tree of schema nodes. `selection` is the visible-row index;
    /// `expanded` tracks which group paths are currently expanded. `filter` is
    /// the live search substring (empty = show the tree); `searching` toggles
    /// inline filter-edit mode via `/`.
    record Schema(
            int selection,
            java.util.Set<String> expanded,
            String filter,
            boolean searching) implements ScreenState {
        public Schema {
            expanded = java.util.Set.copyOf(expanded);
        }

        public static Schema initial() {
            return new Schema(0, java.util.Set.of(), "", false);
        }
    }

    /// Row groups in the file, one row per group.
    record RowGroups(int selection) implements ScreenState {}

    /// Two-pane overview of one row group: facts (left) + drill menu (right)
    /// leading to Column chunks and Indexes-for-this-RG.
    record RowGroupDetail(int rowGroupIndex, Pane focus, int menuSelection)
            implements ScreenState {
        public enum Pane { FACTS, MENU }
    }

    /// Per-chunk index location table for one row group: Column |
    /// CI offset | CI bytes | OI offset | OI bytes.
    record RowGroupIndexes(int rowGroupIndex, int selection) implements ScreenState {}

    /// Column chunks within one row group.
    record ColumnChunks(int rowGroupIndex, int selection) implements ScreenState {}

    /// All metadata for one `(rowGroup, column)` chunk. `focus` chooses between
    /// the facts pane and the drill-into menu. `logicalTypes` controls whether
    /// the chunk-level Min / Max in the facts pane render via the column's
    /// logical type (default) or the raw physical-type form — toggled with `t`.
    record ColumnChunkDetail(int rowGroupIndex, int columnIndex, Pane focus, int menuSelection,
                              boolean logicalTypes)
            implements ScreenState {
        public enum Pane { FACTS, MENU }
    }

    /// List of pages inside one column chunk; Enter opens a modal page-header view.
    /// `logicalTypes` controls whether stats columns render via logical type
    /// (default) or raw physical-type form — toggled with `t`.
    record Pages(int rowGroupIndex, int columnIndex, int selection, boolean modalOpen,
                 boolean logicalTypes)
            implements ScreenState {
    }

    /// Per-page statistics view for one column chunk. `filter` is the live
    /// search substring matched against each page's formatted min / max;
    /// `searching` toggles inline filter-edit mode via `/`. `logicalTypes`
    /// controls whether Min / Max render via logical type (default) or raw
    /// physical-type form — toggled with `t`. `modalOpen` is the
    /// full-Min/Max modal that opens on Enter when the selected row's
    /// rendering was truncated.
    record ColumnIndexView(
            int rowGroupIndex,
            int columnIndex,
            int selection,
            String filter,
            boolean searching,
            boolean logicalTypes,
            boolean modalOpen) implements ScreenState {
    }

    /// Page-location view for one column chunk.
    record OffsetIndexView(int rowGroupIndex, int columnIndex, int selection) implements ScreenState {
    }

    /// Raw footer layout: file size, footer offset/length, aggregate index bytes.
    /// The cursor only lands on the drillable anchors — `Anchor#COLUMN`,
    /// `Anchor#OFFSET`, `Anchor#DICTIONARY` — so ↑/↓ cycle between them
    /// and Enter always drills. Body content is scrolled separately with
    /// `PgDn` / `PgUp`; `scroll` is the line offset into the body content.
    record Footer(Anchor cursor, int scroll) implements ScreenState {
        public enum Anchor { COLUMN, OFFSET, DICTIONARY }

        public static Footer initial() {
            return new Footer(Anchor.COLUMN, 0);
        }
    }

    /// File-wide list of every column chunk that carries the selected kind
    /// of side data — Column index, Offset index, or Dictionary. Chunks
    /// without the relevant data are filtered out. Enter drills into the
    /// matching per-chunk screen.
    record FileIndexes(Kind kind, int selection) implements ScreenState {
        public enum Kind { COLUMN, OFFSET, DICTIONARY }
    }

    /// Cross-row-group view of one leaf column. `selection` drills into
    /// [ColumnChunkDetail] for the corresponding `(rowGroup, column)`.
    /// `logicalTypes` controls whether Min / Max render via logical type
    /// (default) or raw physical-type form — toggled with `t`.
    record ColumnAcrossRowGroups(int columnIndex, int selection, boolean logicalTypes)
            implements ScreenState {
    }

    /// Dictionary entries for one column chunk. `selection` is the position
    /// within the currently-filtered view; `modalOpen` is the full-value modal
    /// that opens on Enter; `filter` is the live search substring (empty = no
    /// filter); `searching` is the inline search-edit mode toggled with `/`;
    /// `loadConfirmed` flips to true after the user opts into reading a
    /// chunk whose size exceeds `ParquetModel.dictionaryReadCapBytes()`.
    record DictionaryView(
            int rowGroupIndex,
            int columnIndex,
            int selection,
            boolean modalOpen,
            String filter,
            boolean searching,
            boolean loadConfirmed,
            boolean logicalTypes) implements ScreenState {
    }

    /// Projected rows. `firstRow` is the 0-based absolute index of the first row
    /// currently displayed; `pageSize` controls how many rows fit on a page; the
    /// loaded `rows` are pre-formatted strings per column. `columnNames`
    /// duplicates the projection so renderers don't re-derive it.
    /// `selectedRow` is the page-relative cursor (0-based) used for the
    /// full-record modal; `modalRow` is the page-relative index whose record
    /// modal is currently open (-1 when closed). `logicalTypes` controls
    /// whether values render via their logical type (default) or the raw
    /// physical-type form — toggled with `t`. Inside the record modal,
    /// `modalCursorLine` is the per-line cursor (collapsed fields = 1 line,
    /// expanded fields = N lines); `expandedColumns` is the set of fields
    /// whose pretty-printed value is rendered inline (multi-line, no
    /// element caps). `expandedRows` is parallel to `rows` and holds the
    /// multi-line pretty-printed form of each cell, populated by
    /// `RowValueFormatter.formatExpanded`.
    record DataPreview(
            long firstRow,
            int pageSize,
            java.util.List<String> columnNames,
            java.util.List<java.util.List<String>> rows,
            java.util.List<java.util.List<String>> expandedRows,
            int columnScroll,
            int selectedRow,
            int modalRow,
            boolean logicalTypes,
            java.util.Set<Integer> expandedColumns,
            int modalCursorLine)
            implements ScreenState {
        public DataPreview {
            columnNames = java.util.List.copyOf(columnNames);
            rows = java.util.List.copyOf(rows);
            expandedRows = java.util.List.copyOf(expandedRows);
            expandedColumns = java.util.Set.copyOf(expandedColumns);
        }
    }
}
