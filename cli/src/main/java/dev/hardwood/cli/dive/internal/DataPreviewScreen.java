/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.schema.SchemaNode;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Style;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.text.Text;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.widgets.Clear;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.paragraph.Paragraph;
import dev.tamboui.widgets.table.Row;
import dev.tamboui.widgets.table.Table;
import dev.tamboui.widgets.table.TableState;

/// Projected-row preview. `firstRow` / `pageSize` define which rows are currently
/// loaded; `←/→` scrolls the visible column window for wide schemas; `PgDn`/`PgUp`
/// (or `Shift+↓/↑`) flip pages. [ParquetModel#readPreviewPage] maintains a
/// forward-only cursor across calls, so stepping forward never re-iterates from
/// row 0 — only backward moves (`PgUp`, `g` jump-to-top) recreate the reader.
public final class DataPreviewScreen {

    private static final int VISIBLE_COLUMNS = 5;
    private static final int VALUE_TRUNCATE = 32;

    /// A sliding ±10×viewport row window of pre-formatted Data preview
    /// rows. Within-window navigation (PgUp/PgDn that stays inside the
    /// horizon) is served from the buffer with no I/O. See [PreviewWindow].
    private static final PreviewWindow WINDOW = new PreviewWindow();

    private DataPreviewScreen() {
    }

    /// Loads the first page; the page size starts at the most recently
    /// observed viewport stride (or `Keys.PAGE_STRIDE` as a pre-render
    /// fallback) and gets re-loaded to the actual viewport on the first
    /// event after the screen renders.
    public static ScreenState.DataPreview initialState(ParquetModel model) {
        return initialState(model, Keys.viewportStride());
    }

    /// Test entry point — caller picks an explicit page size for
    /// deterministic page-boundary assertions. Production code uses the
    /// viewport-derived overload.
    public static ScreenState.DataPreview initialState(ParquetModel model, int pageSize) {
        return loadPage(model, 0, pageSize, 0, true);
    }

    public static boolean handle(KeyEvent event, ParquetModel model, dev.hardwood.cli.dive.NavigationStack stack) {
        ScreenState.DataPreview state = (ScreenState.DataPreview) stack.top();
        // Auto-resize the page to match the current viewport. The first
        // render after initialState observes the available rows; on the
        // first subsequent event we re-load to fill the actual viewport.
        // Skip while a modal is open — the modal owns the screen.
        if (state.modalRow() < 0 && Keys.hasObservedViewport()
                && state.pageSize() != Keys.viewportStride()) {
            state = loadPage(model, state.firstRow(), Keys.viewportStride(),
                    state.columnScroll(), state.logicalTypes());
            stack.replaceTop(state);
        }
        long total = model.facts().totalRows();
        // columnNames already carries the top-level-field count (see loadPage —
        // the reader indexes into fields, not leaves, so leaf count would overshoot).
        int columnCount = state.columnNames().size();
        if (state.modalRow() >= 0) {
            return handleModal(event, state, stack, model);
        }
        // Plain ↑/↓ moves the selected-row cursor inside the current page; Shift+↑/↓
        // pages (handled by the PgDn/PgUp branches below). Enter opens the
        // full-record modal at the cursor. When the cursor would move past
        // the loaded slice, auto-page so the cursor walks through the whole
        // dataset like any other list screen.
        if (Keys.isStepUp(event)) {
            int rowsLoaded = state.rows().size();
            if (rowsLoaded == 0) {
                return false;
            }
            if (state.selectedRow() == 0) {
                if (state.firstRow() == 0) {
                    return false;
                }
                long prevFirst = Math.max(0, state.firstRow() - state.pageSize());
                ScreenState.DataPreview loaded = loadPage(model, prevFirst, state.pageSize(),
                        state.columnScroll(), state.logicalTypes());
                int lastRow = Math.max(0, loaded.rows().size() - 1);
                stack.replaceTop(withSelectedRow(loaded, lastRow));
                return true;
            }
            stack.replaceTop(withSelectedRow(state, state.selectedRow() - 1));
            return true;
        }
        if (Keys.isStepDown(event)) {
            int rowsLoaded = state.rows().size();
            if (rowsLoaded == 0) {
                return false;
            }
            if (state.selectedRow() >= rowsLoaded - 1) {
                long nextFirst = state.firstRow() + state.pageSize();
                if (nextFirst >= total) {
                    return false;
                }
                ScreenState.DataPreview loaded = loadPage(model, nextFirst, state.pageSize(),
                        state.columnScroll(), state.logicalTypes());
                stack.replaceTop(withSelectedRow(loaded, 0));
                return true;
            }
            stack.replaceTop(withSelectedRow(state, state.selectedRow() + 1));
            return true;
        }
        if (event.isConfirm() && !state.rows().isEmpty()) {
            stack.replaceTop(withModalRow(state, state.selectedRow()));
            return true;
        }
        if (Keys.isPageDown(event)) {
            long nextFirst = Math.min(total, state.firstRow() + state.pageSize());
            if (nextFirst >= total) {
                return false;
            }
            stack.replaceTop(loadPage(model, nextFirst, state.pageSize(), state.columnScroll(),
                    state.logicalTypes()));
            return true;
        }
        if (Keys.isPageUp(event)) {
            long prevFirst = Math.max(0, state.firstRow() - state.pageSize());
            if (prevFirst == state.firstRow()) {
                return false;
            }
            stack.replaceTop(loadPage(model, prevFirst, state.pageSize(), state.columnScroll(),
                    state.logicalTypes()));
            return true;
        }
        if (event.isLeft()) {
            if (state.columnScroll() == 0) {
                return false;
            }
            stack.replaceTop(withColumnScroll(state, Math.max(0, state.columnScroll() - 1)));
            return true;
        }
        if (event.isRight()) {
            int maxScroll = Math.max(0, columnCount - VISIBLE_COLUMNS);
            if (state.columnScroll() >= maxScroll) {
                return false;
            }
            stack.replaceTop(withColumnScroll(state, state.columnScroll() + 1));
            return true;
        }
        if (Keys.isJumpTop(event)) {
            if (state.firstRow() == 0) {
                return false;
            }
            stack.replaceTop(loadPage(model, 0, state.pageSize(), state.columnScroll(),
                    state.logicalTypes()));
            return true;
        }
        if (Keys.isJumpBottom(event)) {
            long lastPageFirst = Math.max(0, total - state.pageSize());
            if (state.firstRow() == lastPageFirst) {
                return false;
            }
            stack.replaceTop(loadPage(model, lastPageFirst, state.pageSize(), state.columnScroll(),
                    state.logicalTypes()));
            return true;
        }
        // Toggle logical-type rendering. Modifier-free `t` (avoid clobbering
        // typed text in any future search-mode here).
        if (event.code() == KeyCode.CHAR && event.character() == 't'
                && !event.hasCtrl() && !event.hasAlt()) {
            stack.replaceTop(loadPage(model, state.firstRow(), state.pageSize(),
                    state.columnScroll(), !state.logicalTypes()));
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.DataPreview state) {
        // Block borders (top + bottom) + header row = 3 cells of chrome
        // around the data rows.
        Keys.observeViewport(area.height() - 3);
        int columnCount = state.columnNames().size();
        int windowEnd = Math.min(columnCount, state.columnScroll() + VISIBLE_COLUMNS);
        List<String> visible = state.columnNames().subList(state.columnScroll(), windowEnd);

        // Compute the per-column width tamboui will end up giving each
        // Fill(1) cell so our `…` truncation indicator stays visible
        // instead of being clipped past. Account for borders (2),
        // highlight symbol "▶ " (2), and column-spacing of 2 between
        // each visible column. Capped at VALUE_TRUNCATE so very wide
        // terminals don't render unbounded values.
        int gutter = 2 + 2 + Math.max(0, (visible.size() - 1) * 2);
        int perColWidth = visible.isEmpty()
                ? VALUE_TRUNCATE
                : Math.max(8, Math.min(VALUE_TRUNCATE,
                        (area.width() - gutter) / visible.size()));

        List<Row> rows = new ArrayList<>();
        for (List<String> row : state.rows()) {
            List<String> sliced = row.subList(state.columnScroll(), windowEnd);
            String[] truncated = new String[sliced.size()];
            for (int i = 0; i < sliced.size(); i++) {
                truncated[i] = truncate(sliced.get(i), perColWidth);
            }
            rows.add(Row.from(truncated));
        }
        Row header = Row.from(visible.toArray(new String[0])).style(Style.EMPTY.bold());

        long total = model.facts().totalRows();
        long lastRow = state.firstRow() + state.rows().size();
        String typeMode = state.logicalTypes() ? "" : " · physical";
        String title = String.format(" Data preview (rows %,d–%,d of %,d · cols %d–%d of %d%s) ",
                state.firstRow() + 1, lastRow, total,
                state.columnScroll() + 1, windowEnd, columnCount, typeMode);

        Block block = Block.builder()
                .title(title)
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        List<Constraint> widths = new ArrayList<>();
        for (int i = 0; i < visible.size(); i++) {
            widths.add(new Constraint.Fill(1));
        }
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(widths)
                .columnSpacing(2)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Style.EMPTY.bold())
                .build();
        TableState tableState = new TableState();
        if (!state.rows().isEmpty()) {
            tableState.select(Math.min(state.selectedRow(), state.rows().size() - 1));
        }
        table.render(area, buffer, tableState);
        if (state.modalRow() >= 0 && state.modalRow() < state.rows().size()) {
            renderRecordModal(buffer, area, model, state);
        }
    }

    private static void renderRecordModal(Buffer buffer, Rect screenArea, ParquetModel model,
                                          ScreenState.DataPreview state) {
        List<String> values = state.rows().get(state.modalRow());
        List<String> expanded = state.expandedRows().get(state.modalRow());
        List<String> names = state.columnNames();
        int width = Math.max(40, screenArea.width() - 4);
        int height = Math.max(8, screenArea.height() - 2);
        int x = screenArea.left() + (screenArea.width() - width) / 2;
        int y = screenArea.top() + (screenArea.height() - height) / 2;
        Rect area = new Rect(x, y, width, height);
        Clear.INSTANCE.render(area, buffer);

        int maxKeyWidth = 0;
        for (String name : names) {
            maxKeyWidth = Math.max(maxKeyWidth, name.length());
        }
        int valueBudget = Math.max(8, width - 2 - 1 - maxKeyWidth - 3 - 1);
        String continuationIndent = " ".repeat(1 + maxKeyWidth + 3);

        // Build the full body as a flat line list. ownership[i] = the line
        // index where field i's key line starts; continuation lines for an
        // expanded field belong to that same field.
        int[] ownership = new int[names.size()];
        List<Line> all = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            String pad = " ".repeat(maxKeyWidth - name.length());
            boolean isExpanded = state.expandedColumns().contains(i);
            String value = i < values.size() ? values.get(i) : "";
            ownership[i] = all.size();
            if (isExpanded) {
                String fullValue = i < expanded.size() ? expanded.get(i) : value;
                List<String> wrapped = wrapValue(fullValue, valueBudget);
                if (wrapped.isEmpty()) {
                    wrapped.add("");
                }
                all.add(Line.from(
                        new Span(" " + name + pad + " : ", Style.EMPTY.bold()),
                        Span.raw(wrapped.get(0))));
                for (int k = 1; k < wrapped.size(); k++) {
                    all.add(Line.from(Span.raw(continuationIndent + wrapped.get(k))));
                }
            }
            else {
                all.add(Line.from(
                        new Span(" " + name + pad + " : ", Style.EMPTY.bold()),
                        Span.raw(truncate(value, valueBudget))));
            }
        }

        int totalLines = all.size();
        int cursorLine = Math.max(0, Math.min(state.modalCursorLine(), totalLines - 1));
        // Cursor is purely decorative when there's nothing to do with it:
        // no field can expand AND content fits the viewport. In that case
        // the modal becomes a static info display.
        boolean canExpandAny = false;
        for (String e : expanded) {
            if (e.indexOf('\n') >= 0) {
                canExpandAny = true;
                break;
            }
        }
        int viewportForCursor = Math.max(1, height - 4);
        boolean overflows = totalLines > viewportForCursor;
        boolean showCursor = canExpandAny || overflows;
        if (showCursor && cursorLine < all.size()) {
            int fieldIdx = fieldForLine(state, cursorLine);
            int fieldFirstLine = ownership[fieldIdx];
            String name = names.get(fieldIdx);
            String pad = " ".repeat(maxKeyWidth - name.length());
            boolean isExpanded = state.expandedColumns().contains(fieldIdx);
            String value = fieldIdx < values.size() ? values.get(fieldIdx) : "";
            Style accent = Style.EMPTY.bold().fg(Theme.ACCENT);
            if (cursorLine == fieldFirstLine) {
                String shown;
                if (isExpanded) {
                    String fullValue = fieldIdx < expanded.size() ? expanded.get(fieldIdx) : value;
                    List<String> wrapped = wrapValue(fullValue, valueBudget);
                    shown = wrapped.isEmpty() ? "" : wrapped.get(0);
                }
                else {
                    shown = truncate(value, valueBudget);
                }
                all.set(cursorLine, Line.from(
                        new Span("▶" + name + pad + " : ", accent),
                        new Span(shown, accent)));
            }
            else if (isExpanded) {
                String fullValue = fieldIdx < expanded.size() ? expanded.get(fieldIdx) : value;
                List<String> wrapped = wrapValue(fullValue, valueBudget);
                int contIdx = cursorLine - fieldFirstLine;
                String text = contIdx < wrapped.size() ? wrapped.get(contIdx) : "";
                all.set(cursorLine, Line.from(new Span(continuationIndent + text, accent)));
            }
        }

        int viewport = Math.max(1, height - 4);
        int scroll = Math.max(0, Math.min(totalLines - viewport,
                Math.max(0, cursorLine - viewport / 2)));
        int end = Math.min(totalLines, scroll + viewport);

        List<Line> lines = new ArrayList<>(all.subList(scroll, end));
        lines.add(Line.empty());
        // Hint is tiered: drop "↑↓ navigate" when navigation has no effect
        // (cursor is hidden because content fits AND nothing is expandable,
        // or content fits with only one line); drop "Enter expand" +
        // "e/c all" when no field has a multi-line expanded form;
        // include "t logical types" only when at least one column has a
        // logical type.
        boolean canNavigate = showCursor && totalLines > 1;
        boolean canExpand = canExpandAny;
        boolean anyLogical = false;
        for (SchemaNode child : model.schema().getRootNode().children()) {
            if (child instanceof SchemaNode.PrimitiveNode p && p.logicalType() != null) {
                anyLogical = true;
                break;
            }
        }
        List<String> segments = new ArrayList<>();
        if (scroll + viewport < totalLines) {
            segments.add(" ↓ " + (totalLines - end) + " more lines");
        }
        else if (scroll > 0) {
            segments.add(" ↑ " + scroll + " lines above");
        }
        if (canNavigate) {
            segments.add("↑↓ navigate");
        }
        if (canExpand) {
            segments.add("Enter expand");
            segments.add("e/c all");
        }
        if (anyLogical) {
            segments.add("t logical types");
        }
        segments.add("Esc close");
        String hint = String.join(" · ", segments);
        if (!hint.startsWith(" ")) {
            hint = " " + hint;
        }
        lines.add(Line.from(new Span(hint, Style.EMPTY.fg(Theme.DIM))));

        long absRow = state.firstRow() + state.modalRow();
        Block block = Block.builder()
                .title(String.format(" Row %,d ", absRow + 1))
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Paragraph.builder()
                .block(block)
                .text(Text.from(lines))
                .left()
                .build()
                .render(area, buffer);
    }

    /// Splits a possibly multi-line value into display lines that each fit
    /// within `width` cells. Hard line breaks in the source are preserved;
    /// each segment is then chunked at `width` if it's longer.
    private static List<String> wrapValue(String value, int width) {
        List<String> out = new ArrayList<>();
        if (width <= 0) {
            out.add(value);
            return out;
        }
        for (String line : value.split("\n", -1)) {
            if (line.isEmpty()) {
                out.add("");
                continue;
            }
            int i = 0;
            while (i < line.length()) {
                int end = Math.min(line.length(), i + width);
                out.add(line.substring(i, end));
                i = end;
            }
        }
        return out;
    }

    public static String keybarKeys(ScreenState.DataPreview state, ParquetModel model) {
        if (state.modalRow() >= 0) {
            return "";
        }
        long total = model.facts().totalRows();
        int loaded = state.rows().size();
        int columnCount = state.columnNames().size();
        boolean canPage = total > state.pageSize();
        boolean canColumnScroll = columnCount > VISIBLE_COLUMNS;
        boolean anyLogical = false;
        for (SchemaNode child : model.schema().getRootNode().children()) {
            if (child instanceof SchemaNode.PrimitiveNode p && p.logicalType() != null) {
                anyLogical = true;
                break;
            }
        }
        return new Keys.Hints()
                .add(loaded > 1, "[↑↓] row")
                .add(loaded > 0, "[Enter] view record")
                .add(canColumnScroll, "[←→] columns")
                .add(canPage, "[PgDn/PgUp or Shift+↓↑] page")
                .add(canPage, "[g/G] start/end")
                .add(anyLogical, "[t] logical types")
                .add(true, "[Esc] back")
                .build();
    }

    private static boolean handleModal(KeyEvent event, ScreenState.DataPreview state,
                                       dev.hardwood.cli.dive.NavigationStack stack,
                                       ParquetModel model) {
        // Inside the modal, ↑/↓ navigate the modal's content one line at a
        // time (collapsed field = 1 line, expanded field = N lines), so a
        // long expansion can be scrolled and the next field below it
        // reached without closing. Enter toggles expansion for the field
        // owning the current line; e / c expand / collapse all fields. Esc
        // closes the modal. Row stepping is intentionally absent — the
        // user picks another row from the table after closing.
        if (event.isCancel()) {
            stack.replaceTop(withModalRow(state, -1));
            return true;
        }
        int totalLines = totalModalLines(state);
        if (event.isConfirm()) {
            int field = fieldForLine(state, state.modalCursorLine());
            Set<Integer> next = new HashSet<>(state.expandedColumns());
            if (!next.remove(field)) {
                next.add(field);
            }
            // Keep the cursor on the same field after toggling so the user
            // doesn't lose their place.
            int newCursor = firstLineForField(state, next, field);
            stack.replaceTop(withExpansion(state, next, newCursor));
            return true;
        }
        if (event.code() == KeyCode.CHAR && event.character() == 'e'
                && !event.hasCtrl() && !event.hasAlt()) {
            int field = fieldForLine(state, state.modalCursorLine());
            Set<Integer> all = new HashSet<>();
            for (int i = 0; i < state.columnNames().size(); i++) {
                all.add(i);
            }
            int newCursor = firstLineForField(state, all, field);
            stack.replaceTop(withExpansion(state, all, newCursor));
            return true;
        }
        if (event.code() == KeyCode.CHAR && event.character() == 'c'
                && !event.hasCtrl() && !event.hasAlt()) {
            int field = fieldForLine(state, state.modalCursorLine());
            int newCursor = firstLineForField(state, Set.of(), field);
            stack.replaceTop(withExpansion(state, Set.of(), newCursor));
            return true;
        }
        // `t` toggles logical-type rendering. Re-loads the current page
        // with the new flag and preserves the modal-state fields
        // (selectedRow, modalRow, expandedColumns, cursorLine) so the
        // user stays put.
        if (event.code() == KeyCode.CHAR && event.character() == 't'
                && !event.hasCtrl() && !event.hasAlt()) {
            boolean nextLogical = !state.logicalTypes();
            ScreenState.DataPreview reloaded = loadPage(model, state.firstRow(),
                    state.pageSize(), state.columnScroll(), nextLogical);
            stack.replaceTop(new ScreenState.DataPreview(
                    reloaded.firstRow(), reloaded.pageSize(), reloaded.columnNames(),
                    reloaded.rows(), reloaded.expandedRows(), reloaded.columnScroll(),
                    state.selectedRow(), state.modalRow(), nextLogical,
                    state.expandedColumns(), state.modalCursorLine()));
            return true;
        }
        if (event.isUp()) {
            if (state.modalCursorLine() == 0) {
                return false;
            }
            stack.replaceTop(withCursorLine(state, state.modalCursorLine() - 1));
            return true;
        }
        if (event.isDown()) {
            if (state.modalCursorLine() >= totalLines - 1) {
                return false;
            }
            stack.replaceTop(withCursorLine(state, state.modalCursorLine() + 1));
            return true;
        }
        return false;
    }

    /// Total displayable lines in the modal body — one per field for
    /// collapsed fields, plus extra continuation lines for each expanded
    /// field's pretty-printed value.
    private static int totalModalLines(ScreenState.DataPreview state) {
        int total = state.columnNames().size();
        List<String> expanded = state.expandedRows().get(state.modalRow());
        for (int i : state.expandedColumns()) {
            if (i < 0 || i >= expanded.size()) {
                continue;
            }
            int continuationLines = expanded.get(i).split("\n", -1).length;
            total += Math.max(0, continuationLines - 1);
        }
        return total;
    }

    /// Field index that owns the given cursor line in the flattened modal
    /// body. Continuation lines of an expanded field map to that field.
    private static int fieldForLine(ScreenState.DataPreview state, int line) {
        int names = state.columnNames().size();
        if (names == 0) {
            return 0;
        }
        List<String> expanded = state.expandedRows().get(state.modalRow());
        int cursor = 0;
        for (int field = 0; field < names; field++) {
            int linesForField = 1;
            if (state.expandedColumns().contains(field) && field < expanded.size()) {
                linesForField = expanded.get(field).split("\n", -1).length;
            }
            if (line < cursor + linesForField) {
                return field;
            }
            cursor += linesForField;
        }
        return names - 1;
    }

    /// Line index of the key line for `field` given the new expanded set.
    private static int firstLineForField(ScreenState.DataPreview state,
                                          Set<Integer> expandedColumns, int field) {
        List<String> expanded = state.expandedRows().get(state.modalRow());
        int line = 0;
        for (int i = 0; i < field; i++) {
            int linesForField = 1;
            if (expandedColumns.contains(i) && i < expanded.size()) {
                linesForField = expanded.get(i).split("\n", -1).length;
            }
            line += linesForField;
        }
        return line;
    }

    private static ScreenState.DataPreview withSelectedRow(ScreenState.DataPreview s, int sel) {
        return new ScreenState.DataPreview(s.firstRow(), s.pageSize(), s.columnNames(), s.rows(),
                s.expandedRows(), s.columnScroll(), sel, s.modalRow(), s.logicalTypes(),
                s.expandedColumns(), s.modalCursorLine());
    }

    private static ScreenState.DataPreview withModalRow(ScreenState.DataPreview s, int modalRow) {
        return new ScreenState.DataPreview(s.firstRow(), s.pageSize(), s.columnNames(), s.rows(),
                s.expandedRows(), s.columnScroll(), s.selectedRow(), modalRow, s.logicalTypes(),
                modalRow < 0 ? Set.of() : s.expandedColumns(),
                modalRow < 0 ? 0 : s.modalCursorLine());
    }

    private static ScreenState.DataPreview withColumnScroll(ScreenState.DataPreview s, int scroll) {
        return new ScreenState.DataPreview(s.firstRow(), s.pageSize(), s.columnNames(), s.rows(),
                s.expandedRows(), scroll, s.selectedRow(), s.modalRow(), s.logicalTypes(),
                s.expandedColumns(), s.modalCursorLine());
    }

    private static ScreenState.DataPreview withCursorLine(ScreenState.DataPreview s, int line) {
        return new ScreenState.DataPreview(s.firstRow(), s.pageSize(), s.columnNames(), s.rows(),
                s.expandedRows(), s.columnScroll(), s.selectedRow(), s.modalRow(), s.logicalTypes(),
                s.expandedColumns(), line);
    }

    private static ScreenState.DataPreview withExpansion(ScreenState.DataPreview s,
                                                          Set<Integer> expanded, int cursorLine) {
        return new ScreenState.DataPreview(s.firstRow(), s.pageSize(), s.columnNames(), s.rows(),
                s.expandedRows(), s.columnScroll(), s.selectedRow(), s.modalRow(), s.logicalTypes(),
                expanded, cursorLine);
    }

    private static ScreenState.DataPreview loadPage(ParquetModel model, long firstRow, int pageSize,
                                                    int columnScroll, boolean logicalTypes) {
        PreviewWindow.Slice slice = WINDOW.slice(model, firstRow, pageSize, logicalTypes);
        return new ScreenState.DataPreview(firstRow, pageSize, slice.columnNames(),
                slice.rows(), slice.expandedRows(), columnScroll, 0, -1,
                logicalTypes, Set.of(), 0);
    }

    private static String truncate(String s, int max) {
        if (s.length() <= max) {
            return s;
        }
        return s.substring(0, max - 1) + "…";
    }
}
