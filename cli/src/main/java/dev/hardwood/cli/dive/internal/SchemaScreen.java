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

import dev.hardwood.cli.dive.NavigationStack;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.schema.ColumnSchema;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Color;
import dev.tamboui.style.Style;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.table.Row;
import dev.tamboui.widgets.table.Table;
import dev.tamboui.widgets.table.TableState;

/// Flat list of leaf columns in the file. Phase 1 shows columns in a table; a true
/// expandable tree (with groups, lists, maps) ships in phase 2.
public final class SchemaScreen {

    private SchemaScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.Schema state = (ScreenState.Schema) stack.top();
        int count = model.columnCount();
        if (event.isUp()) {
            stack.replaceTop(new ScreenState.Schema(Math.max(0, state.selection() - 1)));
            return true;
        }
        if (event.isDown()) {
            stack.replaceTop(new ScreenState.Schema(Math.min(count - 1, state.selection() + 1)));
            return true;
        }
        if (event.isConfirm() && count > 0) {
            stack.push(new ScreenState.ColumnAcrossRowGroups(state.selection(), 0));
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.Schema state) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < model.columnCount(); i++) {
            ColumnSchema col = model.schema().getColumn(i);
            rows.add(Row.from(
                    "[" + i + "]",
                    col.fieldPath().toString(),
                    col.type().name(),
                    col.logicalType() != null ? col.logicalType().toString() : "—"));
        }
        Row header = Row.from("#", "Path", "Physical", "Logical").style(Style.EMPTY.bold());
        Block block = Block.builder()
                .title(" Schema (" + model.columnCount() + " columns) ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.CYAN)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(new Constraint.Length(5),
                        new Constraint.Fill(3),
                        new Constraint.Length(16),
                        new Constraint.Length(20))
                .columnSpacing(2)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Style.EMPTY.bold())
                .build();
        TableState tableState = new TableState();
        tableState.select(state.selection());
        table.render(area, buffer, tableState);
    }

    public static String keybarKeys() {
        return "[↑↓] move  [Enter] column across RGs  [Esc] back";
    }
}
