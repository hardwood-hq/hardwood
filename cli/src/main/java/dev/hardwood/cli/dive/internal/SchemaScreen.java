/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import dev.hardwood.cli.dive.NavigationStack;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Layout;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Color;
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

/// Expandable tree of the Parquet schema. Groups (structs, lists, maps) can be
/// expanded with `→` / `Enter` and collapsed with `←`. `Enter` on a leaf column
/// drills into the [ColumnAcrossRowGroupsScreen] for that column.
///
/// `/` enters inline search: typed chars extend the filter, Backspace trims,
/// Enter commits, Esc clears. When the filter is non-empty the tree collapses
/// to a flat list of matching leaf columns.
public final class SchemaScreen {

    /// One row in the rendered view.
    record Row(int depth, SchemaNode node, String path, boolean isGroup, int columnIndex) {
    }

    private SchemaScreen() {
    }

    /// Used by [DiveApp] to decide whether the screen should receive printable
    /// chars instead of the global keymap.
    public static boolean isInInputMode(ScreenState.Schema state) {
        return state.searching();
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.Schema state = (ScreenState.Schema) stack.top();
        if (state.searching()) {
            return handleSearching(event, state, stack);
        }
        List<Row> rows = visibleRows(model.schema(), state.expanded(), state.filter());
        if (event.code() == KeyCode.CHAR && event.character() == '/') {
            stack.replaceTop(with(state, 0, state.expanded(), state.filter(), true));
            return true;
        }
        if (rows.isEmpty()) {
            return false;
        }
        if (event.isUp()) {
            stack.replaceTop(with(state,
                    Math.max(0, state.selection() - 1), state.expanded(), state.filter(), false));
            return true;
        }
        if (event.isDown()) {
            stack.replaceTop(with(state,
                    Math.min(rows.size() - 1, state.selection() + 1), state.expanded(), state.filter(), false));
            return true;
        }
        Row current = rows.get(Math.min(state.selection(), rows.size() - 1));
        if (event.isRight()) {
            if (current.isGroup() && !state.expanded().contains(current.path())) {
                Set<String> next = new HashSet<>(state.expanded());
                next.add(current.path());
                stack.replaceTop(with(state, state.selection(), next, state.filter(), false));
                return true;
            }
            return false;
        }
        if (event.isLeft()) {
            if (current.isGroup() && state.expanded().contains(current.path())) {
                Set<String> next = new HashSet<>(state.expanded());
                next.remove(current.path());
                stack.replaceTop(with(state, state.selection(), next, state.filter(), false));
                return true;
            }
            return false;
        }
        if (event.isConfirm()) {
            if (current.isGroup()) {
                Set<String> next = new HashSet<>(state.expanded());
                if (!next.remove(current.path())) {
                    next.add(current.path());
                }
                stack.replaceTop(with(state, state.selection(), next, state.filter(), false));
                return true;
            }
            stack.push(new ScreenState.ColumnAcrossRowGroups(current.columnIndex(), 0));
            return true;
        }
        return false;
    }

    private static boolean handleSearching(KeyEvent event, ScreenState.Schema state, NavigationStack stack) {
        if (event.isCancel()) {
            stack.replaceTop(with(state, 0, state.expanded(), "", false));
            return true;
        }
        if (event.isConfirm()) {
            stack.replaceTop(with(state, 0, state.expanded(), state.filter(), false));
            return true;
        }
        if (event.isDeleteBackward()) {
            String f = state.filter();
            String next = f.isEmpty() ? f : f.substring(0, f.length() - 1);
            stack.replaceTop(with(state, 0, state.expanded(), next, true));
            return true;
        }
        if (event.code() == KeyCode.CHAR) {
            char c = event.character();
            if (c >= ' ' && c != 127) {
                stack.replaceTop(with(state, 0, state.expanded(), state.filter() + c, true));
                return true;
            }
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.Schema state) {
        List<Row> rows = visibleRows(model.schema(), state.expanded(), state.filter());
        List<Rect> split = Layout.vertical()
                .constraints(new Constraint.Length(1), new Constraint.Fill(1))
                .split(area);

        renderSearchBar(buffer, split.get(0), state, model.columnCount(), rows.size());

        List<Line> lines = new ArrayList<>();
        boolean filtering = !state.filter().isEmpty();
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            boolean selected = i == state.selection();
            String cursor = selected ? "▸ " : "  ";
            Style nameStyle = selected ? Style.EMPTY.bold() : Style.EMPTY;
            if (filtering) {
                String typeInfo = typeOf(row.node());
                String colSuffix = "  [col " + row.columnIndex() + "]";
                lines.add(Line.from(
                        Span.raw(cursor),
                        new Span(row.path(), nameStyle),
                        new Span("  " + typeInfo, Style.EMPTY.fg(Color.GRAY)),
                        new Span(colSuffix, Style.EMPTY.fg(Color.GRAY))));
                continue;
            }
            String indent = "  ".repeat(row.depth());
            String marker;
            if (row.isGroup()) {
                marker = state.expanded().contains(row.path()) ? "▼ " : "▶ ";
            }
            else {
                marker = "  ";
            }
            String typeInfo = typeOf(row.node());
            String colSuffix = !row.isGroup() ? "  [col " + row.columnIndex() + "]" : "";
            lines.add(Line.from(
                    Span.raw(cursor),
                    Span.raw(indent),
                    new Span(marker, Style.EMPTY.fg(Color.CYAN)),
                    new Span(row.node().name(), nameStyle),
                    new Span("  " + typeInfo, Style.EMPTY.fg(Color.GRAY)),
                    new Span(colSuffix, Style.EMPTY.fg(Color.GRAY))));
        }
        Block block = Block.builder()
                .title(" Schema (" + model.columnCount() + " leaf columns"
                        + (filtering ? "; " + rows.size() + " matching" : "") + ") ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.CYAN)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(split.get(1), buffer);
    }

    public static String keybarKeys() {
        return "[↑↓] move  [→/Enter] expand · drill  [←] collapse  [/] search  [Esc] back";
    }

    private static void renderSearchBar(Buffer buffer, Rect area, ScreenState.Schema state,
                                        int totalColumns, int matchCount) {
        if (!state.searching() && state.filter().isEmpty()) {
            Paragraph.builder()
                    .text(Text.from(Line.from(new Span(
                            " " + totalColumns + " leaf columns. Press / to filter by path.",
                            Style.EMPTY.fg(Color.GRAY)))))
                    .left()
                    .build()
                    .render(area, buffer);
            return;
        }
        String cursor = state.searching() ? "█" : "";
        Line line = Line.from(
                new Span(" / ", Style.EMPTY.fg(Color.CYAN).bold()),
                new Span(state.filter() + cursor, Style.EMPTY.bold()),
                new Span("  (" + matchCount + " / " + totalColumns + " leaves)", Style.EMPTY.fg(Color.GRAY)));
        Paragraph.builder().text(Text.from(line)).left().build().render(area, buffer);
    }

    static List<Row> visibleRows(FileSchema schema, Set<String> expanded, String filter) {
        if (!filter.isEmpty()) {
            return matchingLeaves(schema, filter);
        }
        List<Row> out = new ArrayList<>();
        SchemaNode.GroupNode root = schema.getRootNode();
        for (SchemaNode child : root.children()) {
            collect(child, "", 0, expanded, out);
        }
        return out;
    }

    private static void collect(SchemaNode node, String parentPath, int depth,
                                Set<String> expanded, List<Row> out) {
        String path = parentPath.isEmpty() ? node.name() : parentPath + "." + node.name();
        if (node instanceof SchemaNode.PrimitiveNode p) {
            out.add(new Row(depth, node, path, false, p.columnIndex()));
            return;
        }
        SchemaNode.GroupNode group = (SchemaNode.GroupNode) node;
        out.add(new Row(depth, node, path, true, -1));
        if (expanded.contains(path)) {
            for (SchemaNode child : group.children()) {
                collect(child, path, depth + 1, expanded, out);
            }
        }
    }

    private static List<Row> matchingLeaves(FileSchema schema, String filter) {
        String needle = filter.toLowerCase();
        List<Row> all = new ArrayList<>();
        SchemaNode.GroupNode root = schema.getRootNode();
        for (SchemaNode child : root.children()) {
            collectAllLeaves(child, "", all);
        }
        List<Row> matched = new ArrayList<>();
        for (Row r : all) {
            if (r.path().toLowerCase().contains(needle)) {
                matched.add(r);
            }
        }
        return Collections.unmodifiableList(matched);
    }

    private static void collectAllLeaves(SchemaNode node, String parentPath, List<Row> out) {
        String path = parentPath.isEmpty() ? node.name() : parentPath + "." + node.name();
        if (node instanceof SchemaNode.PrimitiveNode p) {
            out.add(new Row(0, node, path, false, p.columnIndex()));
            return;
        }
        SchemaNode.GroupNode group = (SchemaNode.GroupNode) node;
        for (SchemaNode child : group.children()) {
            collectAllLeaves(child, path, out);
        }
    }

    private static ScreenState.Schema with(ScreenState.Schema state,
                                            int selection, Set<String> expanded,
                                            String filter, boolean searching) {
        return new ScreenState.Schema(selection, expanded, filter, searching);
    }

    private static String typeOf(SchemaNode node) {
        if (node instanceof SchemaNode.PrimitiveNode p) {
            String logical = p.logicalType() != null ? " " + p.logicalType().toString() : "";
            return p.type().name() + logical + "  " + p.repetitionType().name();
        }
        SchemaNode.GroupNode g = (SchemaNode.GroupNode) node;
        String tag;
        if (g.isList()) {
            tag = "(LIST)";
        }
        else if (g.isMap()) {
            tag = "(MAP)";
        }
        else if (g.isVariant()) {
            tag = "(VARIANT)";
        }
        else {
            tag = "(group)";
        }
        return tag + "  " + g.repetitionType().name();
    }
}
