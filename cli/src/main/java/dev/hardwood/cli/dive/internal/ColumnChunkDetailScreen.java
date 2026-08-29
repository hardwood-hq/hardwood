/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import dev.hardwood.cli.dive.NavigationStack;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.cli.internal.Encodings;
import dev.hardwood.cli.internal.Fmt;
import dev.hardwood.cli.internal.LevelSummary;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.cli.internal.Strings;
import dev.hardwood.cli.internal.ValueFormatter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.schema.ColumnSchema;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Layout;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Style;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.text.Text;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.paragraph.Paragraph;

/// Two-pane detail for one `(rowGroup, column)` chunk: facts on the left, drill
/// menu on the right leading into Pages, Column index, Offset index, and
/// Dictionary (dictionary deferred to phase 3).
public final class ColumnChunkDetailScreen {

    public enum MenuItem {
        PAGES("Pages"),
        COLUMN_INDEX("Column index"),
        OFFSET_INDEX("Offset index"),
        DICTIONARY("Dictionary");

        final String label;

        MenuItem(String label) {
            this.label = label;
        }
    }

    private ColumnChunkDetailScreen() {
    }

    /// The state to push when drilling into a chunk: the menu focused, with
    /// the cursor on the first entry `Enter` can act on.
    ///
    /// Chosen here rather than corrected on each keypress — a screen that
    /// re-snapped the cursor every event would drag it straight back off any
    /// disabled entry the reader moved onto.
    public static ScreenState.ColumnChunkDetail initialState(ParquetModel model, int rowGroupIndex,
                                                             int columnIndex, boolean logicalTypes) {
        ScreenState.ColumnChunkDetail entry = new ScreenState.ColumnChunkDetail(
                rowGroupIndex, columnIndex, ScreenState.ColumnChunkDetail.Pane.MENU, 0,
                logicalTypes, false);
        int first = firstEnabledIndex(MenuItem.values(), model, entry);
        return first <= 0 ? entry : state(entry, first);
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.ColumnChunkDetail state = (ScreenState.ColumnChunkDetail) stack.top();
        if (event.isFocusNext() || event.isFocusPrevious()) {
            ScreenState.ColumnChunkDetail.Pane next = state.focus() == ScreenState.ColumnChunkDetail.Pane.FACTS
                    ? ScreenState.ColumnChunkDetail.Pane.MENU
                    : ScreenState.ColumnChunkDetail.Pane.FACTS;
            stack.replaceTop(new ScreenState.ColumnChunkDetail(
                    state.rowGroupIndex(), state.columnIndex(), next, state.menuSelection(),
                    state.logicalTypes(), state.levels(), state.scrollTop()));
            return true;
        }
        // `t` toggles logical-type rendering and `l` the level histograms, both
        // on the facts pane and regardless of which pane has focus. Wire before
        // the MENU-only check.
        if (isPlainChar(event, 't')) {
            stack.replaceTop(new ScreenState.ColumnChunkDetail(
                    state.rowGroupIndex(), state.columnIndex(), state.focus(),
                    state.menuSelection(), !state.logicalTypes(), state.levels(), state.scrollTop()));
            return true;
        }
        if (isPlainChar(event, 'l')) {
            stack.replaceTop(new ScreenState.ColumnChunkDetail(
                    state.rowGroupIndex(), state.columnIndex(), state.focus(),
                    state.menuSelection(), state.logicalTypes(), !state.levels(), state.scrollTop()));
            return true;
        }
        if (state.focus() != ScreenState.ColumnChunkDetail.Pane.MENU) {
            return scrollFacts(event, model, stack, state);
        }
        MenuItem[] items = MenuItem.values();
        int selected = CursorPane.select(event, state.menuSelection(), items.length);
        if (selected != CursorPane.UNHANDLED) {
            stack.replaceTop(state(state, selected));
            return true;
        }
        if (event.isConfirm()) {
            MenuItem item = items[state.menuSelection()];
            if (!itemEnabled(item, model, state)) {
                return false;
            }
            switch (item) {
                case PAGES -> stack.push(new ScreenState.Pages(
                        state.rowGroupIndex(), state.columnIndex(), 0, false, true));
                case COLUMN_INDEX -> stack.push(new ScreenState.ColumnIndexView(
                        state.rowGroupIndex(), state.columnIndex(), 0, "", false, true, false));
                case OFFSET_INDEX -> stack.push(new ScreenState.OffsetIndexView(
                        state.rowGroupIndex(), state.columnIndex(), 0));
                case DICTIONARY -> stack.push(new ScreenState.DictionaryView(
                        state.rowGroupIndex(), state.columnIndex(), 0, false, "", false, false, true));
            }
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.ColumnChunkDetail state) {
        List<Rect> cols = Layout.horizontal()
                .constraints(new Constraint.Percentage(60), new Constraint.Percentage(40))
                .split(area);
        renderFactsPane(buffer, cols.get(0), model, state);
        renderMenuPane(buffer, cols.get(1), model, state);
    }

    public static String keybarKeys(ScreenState.ColumnChunkDetail state, ParquetModel model) {
        boolean onMenu = state.focus() == ScreenState.ColumnChunkDetail.Pane.MENU;
        MenuItem[] items = MenuItem.values();
        boolean currentEnabled = state.menuSelection() < items.length
                && itemEnabled(items[state.menuSelection()], model, state);
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        boolean hasLogical = col.logicalType() != null;
        ColumnMetaData cmd = model.chunk(state.rowGroupIndex(), state.columnIndex()).metaData();
        // Offered only when there is a histogram behind it. A chunk whose
        // levels degrade to the two `—` rows has nothing to collapse, so those
        // rows are shown outright and the key would toggle nothing.
        LevelSummary summary = LevelSummary.of(model.schema(), col, cmd);
        boolean hasLevels = hasHistogram(summary);
        // The scroll fragment is empty unless the facts pane overflows, so it
        // contributes nothing while the menu has focus or the content fits.
        boolean canScroll = !onMenu;
        return new Keys.Hints()
                .add(true, "[Tab] pane")
                .add(onMenu, CursorPane.hints(items.length))
                .add(canScroll, factsLines(model, state, LevelSummary.MINIMUM_WIDTH)
                        .hints(Keys.viewportStride()))
                .add(onMenu && currentEnabled, "[Enter] open")
                .add(hasLevels, "[l] levels")
                .add(hasLogical, "[t] logical types")
                .add(true, "[Esc] back")
                .build();
    }

    private static ScreenState.ColumnChunkDetail state(ScreenState.ColumnChunkDetail state, int selection) {
        return new ScreenState.ColumnChunkDetail(
                state.rowGroupIndex(), state.columnIndex(), state.focus(), selection,
                state.logicalTypes(), state.levels(), state.scrollTop());
    }

    /// Repaints the cursor line in the selection colour. Nothing in a facts
    /// pane is actionable, so the cursor carries no marker — the colour alone
    /// is the reader's place in the pane.
    private static List<Line> withCursor(List<Line> window, int offset, boolean focused) {
        if (!focused || offset < 0 || offset >= window.size()) {
            return window;
        }
        List<Line> painted = new ArrayList<>(window);
        StringBuilder text = new StringBuilder();
        for (Span span : window.get(offset).spans()) {
            text.append(span.content());
        }
        painted.set(offset, Line.from(new Span(text.toString(), Theme.selection())));
        return painted;
    }

    private static boolean isPlainChar(KeyEvent event, char character) {
        return event.code() == KeyCode.CHAR && event.character() == character
                && !event.hasCtrl() && !event.hasAlt();
    }

    /// Moves the facts cursor, which is what the navigation keys mean while
    /// the pane has focus — the menu is not being navigated. The pane runs to
    /// about forty lines on a nested column with its levels shown.
    ///
    /// Nothing here is actionable, so the cursor carries no marker; it is the
    /// reader's place in the pane, and it moves on the same keys as every
    /// other focusable pane in dive.
    private static boolean scrollFacts(KeyEvent event, ParquetModel model, NavigationStack stack,
                                       ScreenState.ColumnChunkDetail state) {
        int next = factsLines(model, state, LevelSummary.MINIMUM_WIDTH)
                .select(event, state.scrollTop(), Keys.viewportStride());
        if (next == CursorPane.UNHANDLED) {
            return false;
        }
        if (next != state.scrollTop()) {
            stack.replaceTop(new ScreenState.ColumnChunkDetail(
                    state.rowGroupIndex(), state.columnIndex(), state.focus(), state.menuSelection(),
                    state.logicalTypes(), state.levels(), next,
                    factsLines(model, state, LevelSummary.MINIMUM_WIDTH)
                            .windowTopAfterMove(state.factsTop(), state.scrollTop(), next,
                                    Keys.viewportStride())));
        }
        return true;
    }

    private static int firstEnabledIndex(MenuItem[] items, ParquetModel model,
                                          ScreenState.ColumnChunkDetail state) {
        for (int i = 0; i < items.length; i++) {
            if (itemEnabled(items[i], model, state)) {
                return i;
            }
        }
        return -1;
    }


    private static boolean itemEnabled(MenuItem item, ParquetModel model, ScreenState.ColumnChunkDetail state) {
        ColumnChunk chunk = model.chunk(state.rowGroupIndex(), state.columnIndex());
        return switch (item) {
            case PAGES -> true;
            case COLUMN_INDEX -> chunk.columnIndexOffset() != null && chunk.columnIndexLength() != null;
            case OFFSET_INDEX -> chunk.offsetIndexOffset() != null && chunk.offsetIndexLength() != null;
            case DICTIONARY -> chunk.metaData().dictionaryPageOffset() != null;
        };
    }

    /// Number of lines the facts pane holds. Every row is one line and the
    /// `Path` row splits on a fixed budget rather than the pane's width, so
    /// this does not depend on the width the lines are later built at — which
    /// is what lets key handling size a scroll against content it has not
    /// rendered.
    private static int factsRowCount(ParquetModel model, ScreenState.ColumnChunkDetail state) {
        return factsLines(model, state, LevelSummary.MINIMUM_WIDTH).rowCount();
    }

    private static Document factsLines(ParquetModel model, ScreenState.ColumnChunkDetail state,
                                         int innerWidth) {
        ColumnChunk chunk = model.chunk(state.rowGroupIndex(), state.columnIndex());
        ColumnMetaData cmd = chunk.metaData();
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        Statistics stats = cmd.statistics();

        LevelSummary summary = LevelSummary.of(model.schema(), col, cmd);
        Document.Builder lines = Document.builder();

        lines.decoration(group("Identity"));
        for (Line pathLine : pathLines(Sizes.columnPath(cmd))) {
            lines.row(pathLine);
        }
        lines.row(fact("Column idx", String.valueOf(col.columnIndex())));
        lines.row(fact("Physical", cmd.type().name()));
        lines.row(fact("Logical", col.logicalType() != null ? col.logicalType().toString() : Strings.ABSENT_VALUE));

        lines.blank();
        lines.decoration(group("Storage"));
        lines.row(fact("Compressed", Sizes.format(cmd.totalCompressedSize())
                + compressionQualifier(cmd)));
        lines.row(fact("Uncompressed", Sizes.format(cmd.totalUncompressedSize())));
        lines.row(fact("Codec", cmd.codec().name()));
        // What the data pages actually use, which is the same figure
        // `hardwood inspect columns` prints. The chunk's declared list follows
        // only where `encoding_stats` made the two say different things: it
        // carries the dictionary page and the RLE level streams as well, so on
        // its own it cannot show a dictionary abandoned partway through.
        long dictionaryEntries = model.dictionaryEntries(state.rowGroupIndex(), state.columnIndex());
        long dictionaryValues = summary.hasPresentValues() ? summary.presentValues() : cmd.numValues();
        lines.row(fact("Encoding",
                Encodings.label(Encodings.dataPages(cmd), dictionaryEntries, dictionaryValues)
                        + dictionaryQualifier(dictionaryEntries, dictionaryValues)));
        if (Encodings.hasEncodingStats(cmd)) {
            lines.row(fact("Chunk encodings", cmd.encodings().stream()
                    .map(Enum::name)
                    .collect(Collectors.joining(", "))));
        }
        appendStorageStatistics(lines, summary, cmd, model, state);

        lines.blank();
        lines.decoration(group("Content"));
        appendContent(lines, cmd, stats, col, summary, state);

        lines.blank();
        appendLevels(lines, summary, innerWidth, state);

        lines.blank();
        lines.decoration(group("Layout"));
        lines.row(fact("Data offset", Fmt.fmt("%,d", cmd.dataPageOffset())));
        lines.row(fact("Dict offset", cmd.dictionaryPageOffset() != null
                ? Fmt.fmt("%,d", cmd.dictionaryPageOffset())
                : Strings.ABSENT_VALUE));
        lines.row(fact("Column index offset", chunk.columnIndexOffset() != null
                ? Fmt.fmt("%,d", chunk.columnIndexOffset())
                : Strings.ABSENT_VALUE));
        lines.row(fact("Offset index offset", chunk.offsetIndexOffset() != null
                ? Fmt.fmt("%,d", chunk.offsetIndexOffset())
                : Strings.ABSENT_VALUE));
        return lines.build();
    }

    private static void renderFactsPane(Buffer buffer, Rect area, ParquetModel model, ScreenState.ColumnChunkDetail state) {
        boolean focused = state.focus() == ScreenState.ColumnChunkDetail.Pane.FACTS;
        // Two border columns and the one-space inset every row is rendered with.
        Document facts = factsLines(model, state, Math.max(0, area.width() - 3));

        int viewport = Math.max(1, area.height() - 2);
        Keys.observeViewport(viewport);
        int cursorLine = Math.max(0, facts.lineOfRow(state.scrollTop()));
        RowWindow window = RowWindow.from(
                facts.windowTop(state.factsTop(), state.scrollTop(), viewport),
                cursorLine, facts.lineCount(), viewport);
        int scroll = window.start();
        int end = window.end();

        Block block = paneBlock(paneTitle(model, state, scroll, end, facts.lineCount()), focused);
        Paragraph.builder()
                .block(block)
                .text(Text.from(withCursor(facts.lines().subList(scroll, end),
                        cursorLine - scroll, focused)))
                .left().build().render(area, buffer);
    }

    /// The pane title, with a line range appended when the content does not
    /// fit. Without it a clipped pane is indistinguishable from a complete
    /// one, which is the whole hazard of a pane that drops its tail.
    private static String paneTitle(ParquetModel model, ScreenState.ColumnChunkDetail state,
                                    int scroll, int end, int total) {
        ColumnMetaData cmd = model.chunk(state.rowGroupIndex(), state.columnIndex()).metaData();
        String title = " " + truncateLeft(Sizes.columnPath(cmd), 40) + " (RG #" + state.rowGroupIndex() + ") ";
        if (end - scroll >= total) {
            return title;
        }
        return title + "─ " + (scroll + 1) + "-" + end + "/" + total + " ";
    }

    /// The size-statistics rows that describe how the bytes are stored. Rows
    /// whose input the file does not record are dropped rather than shown as
    /// `—`: scaffolding a flat column with rows that can never be filled costs
    /// more lines than it explains.
    private static void appendStorageStatistics(Document.Builder lines, LevelSummary summary, ColumnMetaData cmd,
                                                ParquetModel model, ScreenState.ColumnChunkDetail state) {
        if (!summary.hasSizeStatistics()) {
            lines.row(advisory("Size statistics", Strings.ABSENT_VALUE + " (not written)"));
        }
        else {
            lines.row(fact("Size statistics", coverage(model, state)));
        }
        if (summary.hasUnencoded()) {
            // The length-prefix parenthetical needs the present-value count; a
            // chunk that records a size but no definition histogram has none,
            // and counting its nulls as values would overstate the prefixes.
            StringBuilder note = new StringBuilder();
            if (cmd.totalCompressedSize() > 0) {
                note.append(Fmt.fmt("%.1f× compressed",
                        (double) summary.unencodedBytes() / cmd.totalCompressedSize()));
            }
            if (summary.hasPresentValues() && summary.lengthPrefixBytes() > 0) {
                if (!note.isEmpty()) {
                    note.append(", ");
                }
                note.append('+').append(Sizes.format(summary.lengthPrefixBytes())).append(" lengths");
            }
            lines.row(fact("Unencoded", Sizes.format(summary.unencodedBytes())
                    + qualifier(note.toString())));
        }
        if (summary.hasAvgValueSize()) {
            lines.row(fact("Avg value size", Sizes.format(Math.round(summary.avgValueSize()))));
        }
    }

    /// The rows that describe what the chunk holds rather than how it is
    /// stored. `Records`, `Present` and the fan-out are dropped for a column
    /// that cannot repeat or cannot be null even though their values are
    /// known, since they would restate the `Values` row.
    ///
    /// Fan-out and the present-value share ride as qualifiers on the rows they
    /// describe rather than standing as rows of their own: each is a property
    /// of the count beside it, and a bare `2.64` on its own line reads as an
    /// independent fact with no scale.
    private static void appendContent(Document.Builder lines, ColumnMetaData cmd, Statistics stats, ColumnSchema col,
                                      LevelSummary summary, ScreenState.ColumnChunkDetail state) {
        boolean repeated = summary.maxRepetitionLevel() > 0;
        if (repeated && summary.hasRecords()) {
            lines.row(fact("Records", Fmt.fmt("%,d", summary.records())));
        }
        lines.row(fact("Values", Fmt.fmt("%,d", cmd.numValues())
                + (repeated && summary.hasAvgFanOut()
                        ? qualifier(Fmt.fmt("%.2f per record", summary.avgFanOut()))
                        : "")));
        if (summary.maxDefinitionLevel() > 0 && summary.hasPresentValues()) {
            lines.row(fact("Present", Fmt.fmt("%,d", summary.presentValues())
                    + share(summary.presentValues(), cmd.numValues())));
        }
        long nulls = summary.nullCount(stats);
        lines.row(fact("Nulls", nulls >= 0
                ? Fmt.fmt("%,d", nulls) + share(nulls, cmd.numValues())
                : Strings.ABSENT_VALUE));
        if (summary.hasAvgListLength()) {
            lines.row(fact("Avg list length", Fmt.fmt("%.2f", summary.avgListLength())
                    + qualifier("non-empty")));
        }
        lines.row(fact("Min", stats != null ? formatStatValue(stats.minValue(), col, state.logicalTypes()) : Strings.ABSENT_VALUE));
        lines.row(fact("Max", stats != null ? formatStatValue(stats.maxValue(), col, state.logicalTypes()) : Strings.ABSENT_VALUE));
        if (summary.mismatch() != null) {
            lines.row(Line.from(
                    new Span(" ⚠ " + padRight("Declared vs actual", 20), Theme.error()),
                    new Span(summary.mismatch(), Theme.error())));
        }
    }

    /// The toggle exists because a real histogram runs to a dozen lines. When
    /// both degrade to one `—` row each there is nothing to collapse, so they
    /// are shown outright rather than hidden behind a key that reveals two
    /// lines of explanation.
    private static void appendLevels(Document.Builder lines, LevelSummary summary, int innerWidth,
                                     ScreenState.ColumnChunkDetail state) {
        if (!hasHistogram(summary)) {
            lines.row(advisory("Def levels", definitionLevelsAbsent(summary)));
            lines.row(advisory("Rep levels", repetitionLevelsAbsent(summary)));
            return;
        }
        if (!state.levels()) {
            lines.row(advisory("Levels", "[l] to show"));
            return;
        }
        appendLevelBlock(lines, "Def levels", summary.definitionLevels(),
                summary.maxDefinitionLevel(), definitionLevelsAbsent(summary), innerWidth);
        lines.blank();
        appendLevelBlock(lines, "Rep levels", summary.repetitionLevels(),
                summary.maxRepetitionLevel(), repetitionLevelsAbsent(summary), innerWidth);
    }

    /// A parenthetical that qualifies the value beside it. Empty in, empty out,
    /// so a caller can build one conditionally without branching on the row.
    private static String qualifier(String text) {
        return text.isEmpty() ? "" : "  (" + text + ")";
    }

    /// `count` as a share of `total`, for a count whose magnitude means
    /// nothing without one.
    private static String share(long count, long total) {
        return total > 0 ? qualifier(Fmt.fmt("%.1f%%", 100.0 * count / total)) : "";
    }

    /// What the codec achieved, stated on the row it applies to so the reader
    /// does not have to infer it from two adjacent sizes.
    private static String compressionQualifier(ColumnMetaData cmd) {
        return cmd.totalUncompressedSize() > 0
                ? qualifier(Sizes.compression(cmd.totalCompressedSize(), cmd.totalUncompressedSize())
                        + " of uncompressed")
                : "";
    }

    /// The counts behind the percentage the `Encoding` label carries. `dive`
    /// has the width for them and the tables do not, and a reader who has
    /// drilled this far is the one who wants to see the arithmetic rather than
    /// take the share on trust.
    private static String dictionaryQualifier(long entries, long values) {
        if (entries < 0 || values <= 0) {
            return "";
        }
        return qualifier(Fmt.fmt("%,d entries for %,d values", entries, values));
    }

    /// A section caption. Tier 2 of the [DIVE_THEME](../../../../../../../_designs/DIVE_THEME.md)
    /// hierarchy: structural, so the eye can find the group holding the fact
    /// it wants instead of reading twenty-five uniform rows.
    private static Line group(String name) {
        return Line.from(new Span(" " + name, Theme.accent().bold()));
    }

    private static boolean hasHistogram(LevelSummary summary) {
        return summary.hasDefinitionHistogram() || summary.hasRepetitionHistogram();
    }

    private static String definitionLevelsAbsent(LevelSummary summary) {
        return summary.maxDefinitionLevel() == 0
                ? Strings.ABSENT_VALUE + " (required, every value present)"
                : Strings.ABSENT_VALUE + " (not written)";
    }

    private static String repetitionLevelsAbsent(LevelSummary summary) {
        return summary.maxRepetitionLevel() == 0
                ? Strings.ABSENT_VALUE + " (not repeated)"
                : Strings.ABSENT_VALUE + " (not written)";
    }

    private static void appendLevelBlock(Document.Builder lines, String label, List<LevelSummary.LevelRow> rows,
                                         int maxLevel, String absent, int innerWidth) {
        if (rows.isEmpty()) {
            lines.row(advisory(label, absent));
            return;
        }
        // A populated block is a section, not a fact: its caption sits at the
        // group indent so the level rows below read as its children.
        lines.decoration(group(label + " (max " + maxLevel + ")"));
        for (String row : LevelSummary.renderLevels(rows, innerWidth)) {
            lines.row(Line.from(new Span(" " + row, Style.EMPTY)));
        }
    }

    /// How much of the chunk the file describes: the chunk-level statistics
    /// always and the per-page copies when the page index carries them. Says
    /// which before the reader drills in to find out.
    ///
    /// Both indexes count. The histograms live in the column index and the
    /// unencoded sizes in the offset index, so a required `BYTE_ARRAY` column —
    /// which has no histogram to write — still describes its pages.
    private static String coverage(ParquetModel model, ScreenState.ColumnChunkDetail state) {
        ColumnIndex columnIndex = model.columnIndex(state.rowGroupIndex(), state.columnIndex());
        OffsetIndex offsetIndex = model.offsetIndex(state.rowGroupIndex(), state.columnIndex());
        if (!LevelSummary.hasPageLevelHistograms(columnIndex)
                && !LevelSummary.hasPageUnencodedSizes(offsetIndex)) {
            return "chunk only";
        }
        return offsetIndex != null
                ? "chunk + " + Plurals.format(offsetIndex.pageLocations().size(), "page", "pages")
                : "chunk + pages";
    }

    private static void renderMenuPane(Buffer buffer, Rect area, ParquetModel model, ScreenState.ColumnChunkDetail state) {
        boolean focused = state.focus() == ScreenState.ColumnChunkDetail.Pane.MENU;
        Block block = paneBlock(" Drill into ", focused);
        List<Line> lines = new ArrayList<>();
        MenuItem[] items = MenuItem.values();
        for (int i = 0; i < items.length; i++) {
            MenuItem item = items[i];
            boolean enabled = itemEnabled(item, model, state);
            boolean selected = focused && i == state.menuSelection();
            // Menu entries a chunk does not have — a column index it was
            // written without, say — stay on screen and keep their fact, but
            // lose the marker that says Enter goes somewhere.
            String cursor = CursorPane.marker(enabled, selected, true);
            Style labelStyle = selected
                    ? Theme.selection()
                    : Theme.primary();
            // The hint carries a fact and, where the page index holds size
            // statistics, an annotation: fact at default fg, annotation dim.
            lines.add(Line.from(
                    new Span(cursor, labelStyle),
                    new Span(padRight(item.label, 16), labelStyle),
                    new Span(menuHint(item, model, state), Style.EMPTY),
                    new Span(menuAnnotation(item, model, state), Theme.dim())));
        }
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    /// What the reader gets for a keystroke on this item. `Pages` is a count,
    /// so a chunk with no offset index has none to show and the hint is the
    /// absent-value marker. The other three answer whether the structure is in
    /// the file at all, which is a yes or a no rather than a missing value —
    /// a word on both sides, so the pane does not read one half as a quantity.
    private static String menuHint(MenuItem item, ParquetModel model, ScreenState.ColumnChunkDetail state) {
        ColumnChunk chunk = model.chunk(state.rowGroupIndex(), state.columnIndex());
        return switch (item) {
            case PAGES -> {
                OffsetIndex oi = model.offsetIndex(state.rowGroupIndex(), state.columnIndex());
                yield oi != null ? Plurals.format(oi.pageLocations().size(), "page", "pages") : Strings.ABSENT_VALUE;
            }
            case COLUMN_INDEX -> presence(chunk.columnIndexOffset() != null);
            case OFFSET_INDEX -> presence(chunk.offsetIndexOffset() != null);
            case DICTIONARY -> presence(chunk.metaData().dictionaryPageOffset() != null);
        };
    }

    private static String presence(boolean present) {
        return present ? "present" : "absent";
    }

    /// Says whether an index carries the per-page size statistics, so the
    /// reader knows what drilling in will show before going. Empty when it
    /// does not, or when the item has nothing to annotate.
    private static String menuAnnotation(MenuItem item, ParquetModel model, ScreenState.ColumnChunkDetail state) {
        return switch (item) {
            case COLUMN_INDEX -> LevelSummary.hasPageLevelHistograms(
                    model.columnIndex(state.rowGroupIndex(), state.columnIndex())) ? " · levels" : "";
            case OFFSET_INDEX -> LevelSummary.hasPageUnencodedSizes(
                    model.offsetIndex(state.rowGroupIndex(), state.columnIndex())) ? " · unencoded" : "";
            case PAGES, DICTIONARY -> "";
        };
    }

    private static Block paneBlock(String title, boolean focused) {
        Block.Builder b = Block.builder()
                .title(title)
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED);
        if (!focused) {
            b.borderStyle(Theme.dim());
        }
        return b.build();
    }

    private static Line fact(String key, String value) {
        return Line.from(
                new Span("   " + padRight(key, 20), Theme.primary()),
                new Span(value, Style.EMPTY));
    }

    /// A labelled row whose value says why there is nothing to show. The
    /// value is chrome rather than content, so it reads back a tier fainter
    /// than the facts around it.
    private static Line advisory(String key, String value) {
        return Line.from(
                new Span("   " + padRight(key, 20), Theme.primary()),
                new Span(value, Theme.dim()));
    }

    /// Special-case the Path row: when the path is short, a single "Path  value"
    /// line is fine; for a deeply-nested path the value would overflow the pane,
    /// so split it over two lines — key on its own line, path value indented below.
    private static List<Line> pathLines(String path) {
        // 22 is the key-padding width used by `fact`, plus 1 leading space.
        int inlineBudget = 50 - 23;
        if (path.length() <= inlineBudget) {
            return List.of(fact("Path", path));
        }
        return List.of(
                Line.from(new Span("   " + padRight("Path", 20), Theme.primary())),
                Line.from(new Span("     " + path, Style.EMPTY)));
    }

    private static String formatStatValue(byte[] bytes, ColumnSchema col, boolean useLogicalType) {
        if (bytes == null) {
            return Strings.ABSENT_VALUE;
        }
        // Facts pane has plenty of horizontal room — render the full value
        // rather than passing a budget.
        return ValueFormatter.formatBytes(bytes, col, useLogicalType);
    }

    private static String padRight(String s, int width) {
        return Strings.padRight(s, width);
    }

    private static String truncateLeft(String s, int maxWidth) {
        return Strings.truncateLeft(s, maxWidth);
    }
}
