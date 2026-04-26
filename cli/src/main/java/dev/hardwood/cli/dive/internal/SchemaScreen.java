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

import dev.hardwood.cli.dive.NavigationStack;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Color;
import dev.tamboui.style.Style;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.text.Text;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.paragraph.Paragraph;

/// Expandable tree of the Parquet schema. Groups (structs, lists, maps) can be
/// expanded with `→` / `Enter` and collapsed with `←`. `Enter` on a leaf column
/// drills into the [ColumnAcrossRowGroupsScreen] for that column.
public final class SchemaScreen {

    /// One row in the flattened tree view.
    record Row(int depth, SchemaNode node, String path, boolean isGroup, int columnIndex) {
    }

    private SchemaScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.Schema state = (ScreenState.Schema) stack.top();
        List<Row> rows = visibleRows(model.schema(), state.expanded());
        if (rows.isEmpty()) {
            return false;
        }
        if (event.isUp()) {
            stack.replaceTop(new ScreenState.Schema(
                    Math.max(0, state.selection() - 1), state.expanded()));
            return true;
        }
        if (event.isDown()) {
            stack.replaceTop(new ScreenState.Schema(
                    Math.min(rows.size() - 1, state.selection() + 1), state.expanded()));
            return true;
        }
        Row current = rows.get(Math.min(state.selection(), rows.size() - 1));
        if (event.isRight()) {
            if (current.isGroup() && !state.expanded().contains(current.path())) {
                Set<String> next = new HashSet<>(state.expanded());
                next.add(current.path());
                stack.replaceTop(new ScreenState.Schema(state.selection(), next));
                return true;
            }
            return false;
        }
        if (event.isLeft()) {
            if (current.isGroup() && state.expanded().contains(current.path())) {
                Set<String> next = new HashSet<>(state.expanded());
                next.remove(current.path());
                stack.replaceTop(new ScreenState.Schema(state.selection(), next));
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
                stack.replaceTop(new ScreenState.Schema(state.selection(), next));
                return true;
            }
            stack.push(new ScreenState.ColumnAcrossRowGroups(current.columnIndex(), 0));
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.Schema state) {
        List<Row> rows = visibleRows(model.schema(), state.expanded());
        List<Line> lines = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            boolean selected = i == state.selection();
            String indent = "  ".repeat(row.depth());
            String marker;
            if (row.isGroup()) {
                marker = state.expanded().contains(row.path()) ? "▼ " : "▶ ";
            }
            else {
                marker = "  ";
            }
            String cursor = selected ? "▸ " : "  ";
            String name = row.node().name();
            Style nameStyle = selected ? Style.EMPTY.bold() : Style.EMPTY;
            String typeInfo = typeOf(row.node());
            String colSuffix = !row.isGroup() ? "  [col " + row.columnIndex() + "]" : "";
            lines.add(Line.from(
                    Span.raw(cursor),
                    Span.raw(indent),
                    new Span(marker, Style.EMPTY.fg(Color.CYAN)),
                    new Span(name, nameStyle),
                    new Span("  " + typeInfo, Style.EMPTY.fg(Color.GRAY)),
                    new Span(colSuffix, Style.EMPTY.fg(Color.GRAY))));
        }
        Block block = Block.builder()
                .title(" Schema (" + model.columnCount() + " leaf columns) ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.CYAN)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    public static String keybarKeys() {
        return "[↑↓] move  [→/Enter] expand · drill  [←] collapse  [Esc] back";
    }

    static List<Row> visibleRows(FileSchema schema, Set<String> expanded) {
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
