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
import dev.hardwood.cli.internal.Sizes;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Layout;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Color;
import dev.tamboui.style.Style;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.widgets.paragraph.Paragraph;

/// Persistent chrome drawn around every screen: top bar, breadcrumb, keybar.
///
/// [#split] carves the frame into four vertical bands — the middle one is the body
/// rect that a screen renders into. [#renderTopBar], [#renderBreadcrumb], and
/// [#renderKeybar] render the surrounding bands.
public final class Chrome {

    /// Width a char takes up in the keybar separator.
    private static final String KEYBAR_SEP = "   ";

    private Chrome() {
    }

    public record Regions(Rect topBar, Rect breadcrumb, Rect body, Rect keybar) {
    }

    public static Regions split(Rect area) {
        List<Rect> rows = Layout.vertical()
                .constraints(
                        new Constraint.Length(1),
                        new Constraint.Length(1),
                        new Constraint.Fill(1),
                        new Constraint.Length(1))
                .split(area);
        return new Regions(rows.get(0), rows.get(1), rows.get(2), rows.get(3));
    }

    public static void renderTopBar(Buffer buffer, Rect area, ParquetModel model) {
        Style bold = Style.EMPTY.bold();
        Style dim = Style.EMPTY.fg(Color.GRAY);
        ParquetModel.Facts f = model.facts();
        Line line = Line.from(
                new Span(" hardwood dive ", bold.fg(Color.CYAN)),
                new Span("│ ", dim),
                Span.raw(model.displayPath()),
                new Span(" │ ", dim),
                Span.raw(Sizes.format(model.fileSizeBytes())),
                new Span(" │ ", dim),
                Span.raw(f.rowGroupCount() + " RGs"),
                new Span(" │ ", dim),
                Span.raw(formatRowCount(f.totalRows()) + " rows"));
        Paragraph.builder().text(convert(line)).left().build().render(area, buffer);
    }

    public static void renderBreadcrumb(Buffer buffer, Rect area, NavigationStack stack, ParquetModel model) {
        List<Span> spans = new ArrayList<>();
        spans.add(Span.raw(" "));
        List<ScreenState> frames = stack.frames();
        for (int i = 0; i < frames.size(); i++) {
            if (i > 0) {
                spans.add(new Span(" › ", Style.EMPTY.fg(Color.GRAY)));
            }
            boolean last = i == frames.size() - 1;
            Style style = last ? Style.EMPTY.bold() : Style.EMPTY.fg(Color.GRAY);
            spans.add(new Span(breadcrumbLabel(frames.get(i), model), style));
        }
        Paragraph.builder().text(convert(Line.from(spans))).left().build().render(area, buffer);
    }

    public static void renderKeybar(Buffer buffer, Rect area, String screenKeys, String globalKeys) {
        Style dim = Style.EMPTY.fg(Color.GRAY);
        Line line = Line.from(
                Span.raw(" "),
                new Span(screenKeys, dim),
                new Span(KEYBAR_SEP, dim),
                new Span(globalKeys, dim));
        Paragraph.builder().text(convert(line)).left().build().render(area, buffer);
    }

    private static String breadcrumbLabel(ScreenState state, ParquetModel model) {
        return switch (state) {
            case ScreenState.Overview ignored -> "Overview";
            case ScreenState.Schema ignored -> "Schema";
            case ScreenState.RowGroups ignored -> "Row groups";
            case ScreenState.ColumnChunks cc -> "RG #" + cc.rowGroupIndex() + " › Column chunks";
            case ScreenState.ColumnChunkDetail d ->
                    model.schema().getColumn(d.columnIndex()).fieldPath().toString();
            case ScreenState.Pages ignored -> "Pages";
            case ScreenState.ColumnIndexView ignored -> "Column index";
            case ScreenState.OffsetIndexView ignored -> "Offset index";
            case ScreenState.Footer ignored -> "Footer & indexes";
            case ScreenState.ColumnAcrossRowGroups c ->
                    model.schema().getColumn(c.columnIndex()).fieldPath() + " across RGs";
            case ScreenState.DictionaryView ignored -> "Dictionary";
            case ScreenState.DataPreview ignored -> "Data preview";
        };
    }

    private static String formatRowCount(long rows) {
        if (rows < 1_000_000) {
            return rows + "";
        }
        return String.format("%.1f M", rows / 1_000_000.0);
    }

    private static dev.tamboui.text.Text convert(Line line) {
        return dev.tamboui.text.Text.from(line);
    }
}
