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

    /// Flat list of leaf columns. Selecting one drills into a cross-row-group view
    /// (phase 2); in phase 1 [Enter] is a no-op and the screen is explorational only.
    record Schema(int selection) implements ScreenState {}

    /// Row groups in the file, one row per group.
    record RowGroups(int selection) implements ScreenState {}

    /// Column chunks within one row group.
    record ColumnChunks(int rowGroupIndex, int selection) implements ScreenState {}

    /// All metadata for one `(rowGroup, column)` chunk.
    record ColumnChunkDetail(int rowGroupIndex, int columnIndex) implements ScreenState {}
}
