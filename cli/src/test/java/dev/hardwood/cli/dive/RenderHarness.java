/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive;

import java.util.ArrayList;
import java.util.List;

import dev.hardwood.cli.dive.internal.ColumnAcrossRowGroupsScreen;
import dev.hardwood.cli.dive.internal.ColumnChunkDetailScreen;
import dev.hardwood.cli.dive.internal.ColumnChunksScreen;
import dev.hardwood.cli.dive.internal.ColumnIndexScreen;
import dev.hardwood.cli.dive.internal.DataPreviewScreen;
import dev.hardwood.cli.dive.internal.DictionaryScreen;
import dev.hardwood.cli.dive.internal.FileIndexesScreen;
import dev.hardwood.cli.dive.internal.FooterScreen;
import dev.hardwood.cli.dive.internal.OffsetIndexScreen;
import dev.hardwood.cli.dive.internal.OverviewScreen;
import dev.hardwood.cli.dive.internal.PagesScreen;
import dev.hardwood.cli.dive.internal.RowGroupDetailScreen;
import dev.hardwood.cli.dive.internal.RowGroupIndexesScreen;
import dev.hardwood.cli.dive.internal.RowGroupsScreen;
import dev.hardwood.cli.dive.internal.SchemaScreen;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.buffer.Cell;
import dev.tamboui.layout.Rect;

/// Test harness for dive screen render assertions. Renders any
/// [ScreenState] to an in-memory [Buffer] and exposes the resulting
/// terminal cells as text. Lets tests assert on title text, row
/// contents, presence of marker characters (▶ / …), etc. without
/// depending on tamboui-internal layout.
///
/// Tests render through this harness instead of calling each screen's
/// `render` method directly so the dispatch matches `DiveApp.renderBody`.
final class RenderHarness {

    private RenderHarness() {
    }

    /// Renders the given screen state into a fresh buffer of `area`'s
    /// dimensions and returns the captured frame. Mirrors
    /// `DiveApp.renderBody`'s switch.
    static RenderedFrame render(Rect area, ScreenState state, ParquetModel model) {
        Buffer buffer = Buffer.empty(area);
        switch (state) {
            case ScreenState.Overview s -> OverviewScreen.render(buffer, area, model, s);
            case ScreenState.Schema s -> SchemaScreen.render(buffer, area, model, s);
            case ScreenState.RowGroups s -> RowGroupsScreen.render(buffer, area, model, s);
            case ScreenState.RowGroupDetail s -> RowGroupDetailScreen.render(buffer, area, model, s);
            case ScreenState.RowGroupIndexes s -> RowGroupIndexesScreen.render(buffer, area, model, s);
            case ScreenState.ColumnChunks s -> ColumnChunksScreen.render(buffer, area, model, s);
            case ScreenState.ColumnChunkDetail s -> ColumnChunkDetailScreen.render(buffer, area, model, s);
            case ScreenState.Pages s -> PagesScreen.render(buffer, area, model, s);
            case ScreenState.ColumnIndexView s -> ColumnIndexScreen.render(buffer, area, model, s);
            case ScreenState.OffsetIndexView s -> OffsetIndexScreen.render(buffer, area, model, s);
            case ScreenState.Footer s -> FooterScreen.render(buffer, area, model, s);
            case ScreenState.ColumnAcrossRowGroups s -> ColumnAcrossRowGroupsScreen.render(buffer, area, model, s);
            case ScreenState.DictionaryView s -> DictionaryScreen.render(buffer, area, model, s);
            case ScreenState.DataPreview s -> DataPreviewScreen.render(buffer, area, model, s);
            case ScreenState.FileIndexes s -> FileIndexesScreen.render(buffer, area, model, s);
        }
        return new RenderedFrame(extractLines(buffer, area));
    }

    /// Returns the screen-state-specific keybar text, mirroring
    /// `DiveApp.keybarForActive`.
    static String keybarFor(ScreenState state, ParquetModel model) {
        return switch (state) {
            case ScreenState.Overview s -> OverviewScreen.keybarKeys(s, model);
            case ScreenState.Schema s -> SchemaScreen.keybarKeys(s, model);
            case ScreenState.RowGroups s -> RowGroupsScreen.keybarKeys(s, model);
            case ScreenState.RowGroupDetail s -> RowGroupDetailScreen.keybarKeys(s);
            case ScreenState.RowGroupIndexes s -> RowGroupIndexesScreen.keybarKeys(s, model);
            case ScreenState.ColumnChunks s -> ColumnChunksScreen.keybarKeys(s, model);
            case ScreenState.ColumnChunkDetail s -> ColumnChunkDetailScreen.keybarKeys(s, model);
            case ScreenState.Pages s -> PagesScreen.keybarKeys(s, model);
            case ScreenState.ColumnIndexView s -> ColumnIndexScreen.keybarKeys(s, model);
            case ScreenState.OffsetIndexView s -> OffsetIndexScreen.keybarKeys(s, model);
            case ScreenState.Footer s -> FooterScreen.keybarKeys(s, model);
            case ScreenState.ColumnAcrossRowGroups s -> ColumnAcrossRowGroupsScreen.keybarKeys(s, model);
            case ScreenState.DictionaryView s -> DictionaryScreen.keybarKeys(s, model);
            case ScreenState.DataPreview s -> DataPreviewScreen.keybarKeys(s, model);
            case ScreenState.FileIndexes s -> FileIndexesScreen.keybarKeys(s, model);
        };
    }

    private static List<String> extractLines(Buffer buffer, Rect area) {
        List<String> lines = new ArrayList<>(area.height());
        for (int y = area.top(); y < area.top() + area.height(); y++) {
            StringBuilder sb = new StringBuilder(area.width());
            for (int x = area.left(); x < area.left() + area.width(); x++) {
                Cell cell = buffer.get(x, y);
                String sym = cell.symbol();
                if (sym == null || sym.isEmpty()) {
                    sb.append(' ');
                }
                else {
                    sb.append(sym);
                }
            }
            // Trim trailing spaces — they're padding, not content.
            int end = sb.length();
            while (end > 0 && sb.charAt(end - 1) == ' ') {
                end--;
            }
            lines.add(sb.substring(0, end));
        }
        return List.copyOf(lines);
    }

    /// A captured rendered frame — terminal cells as a list of trimmed
    /// strings, one per row. Provides convenience predicates for
    /// content assertions.
    record RenderedFrame(List<String> lines) {

        /// All lines joined with `\n` — useful for substring checks
        /// that don't care about row boundaries.
        String text() {
            return String.join("\n", lines);
        }

        /// Whether any line contains the given substring.
        boolean contains(String s) {
            for (String line : lines) {
                if (line.contains(s)) {
                    return true;
                }
            }
            return false;
        }

        /// First line containing `marker` (typically the title between
        /// the box-drawing border characters), or null if none.
        String firstLineContaining(String marker) {
            for (String line : lines) {
                if (line.contains(marker)) {
                    return line;
                }
            }
            return null;
        }
    }
}
