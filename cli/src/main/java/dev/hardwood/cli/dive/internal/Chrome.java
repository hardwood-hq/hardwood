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
import dev.hardwood.cli.internal.Fmt;
import dev.hardwood.cli.internal.Sizes;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Layout;
import dev.tamboui.layout.Rect;
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

    public static Regions split(Rect area, int keybarHeight) {
        int kb = Math.max(1, keybarHeight);
        List<Rect> rows = Layout.vertical()
                .constraints(
                        new Constraint.Length(1),
                        new Constraint.Length(1),
                        new Constraint.Fill(1),
                        new Constraint.Length(kb))
                .split(area);
        return new Regions(rows.get(0), rows.get(1), rows.get(2), rows.get(3));
    }

    /// Number of rows the keybar text needs at the given viewport width.
    /// Matches Paragraph's `WRAP_WORD` behavior so the body never gets a
    /// row that the keybar then steals back.
    public static int keybarHeight(String screenKeys, String globalKeys, int width) {
        if (width <= 0) {
            return 1;
        }
        // " " + screenKeys + KEYBAR_SEP + globalKeys
        int total = 1 + (screenKeys != null ? screenKeys.length() : 0)
                + KEYBAR_SEP.length()
                + (globalKeys != null ? globalKeys.length() : 0);
        return Math.max(1, (total + width - 1) / width);
    }

    public static void renderTopBar(Buffer buffer, Rect area, ParquetModel model) {
        Style brand = Theme.accent().bold();
        Style dim = Theme.dim();
        ParquetModel.Facts f = model.facts();
        List<Span> spans = new ArrayList<>();
        spans.add(new Span(" hardwood dive ", brand));
        spans.add(new Span("│ ", dim));
        spans.add(Span.raw(basename(model.displayPath())));
        spans.add(new Span(" │ ", dim));
        spans.add(Span.raw(Sizes.format(model.fileSizeBytes())));
        spans.add(new Span(" │ ", dim));
        spans.add(Span.raw(Plurals.format(f.rowGroupCount(), "RG", "RGs")));
        spans.add(new Span(" │ ", dim));
        spans.add(Span.raw(formatRowCount(f.totalRows()) + " rows"));
        ParquetModel.NetStats net = model.netStats();
        if (net != null) {
            spans.add(new Span(" │ ", dim));
            spans.add(Span.raw(Plurals.format(net.requestCount(), "req", "reqs")
                    + " · " + Sizes.format(net.bytesFetched())));
        }
        Paragraph.builder().text(convert(Line.from(spans))).left().build().render(area, buffer);
    }

    public static void renderBreadcrumb(Buffer buffer, Rect area, NavigationStack stack, ParquetModel model) {
        List<Span> spans = new ArrayList<>();
        spans.add(Span.raw(" "));
        List<ScreenState> frames = stack.frames();
        // Context-bearing frames upstream tell us whether the leaf
        // already has (RG, col) context to inherit from the path. If
        // not (e.g., reached via Footer → FileIndexes), we enrich the
        // leaf's label so the breadcrumb still says which chunk the
        // current screen belongs to.
        boolean haveRgInPath = false;
        boolean haveColInPath = false;
        for (int i = 0; i < frames.size() - 1; i++) {
            ScreenState f = frames.get(i);
            if (f instanceof ScreenState.RowGroupDetail
                    || f instanceof ScreenState.RowGroupIndexes
                    || f instanceof ScreenState.ColumnChunks) {
                haveRgInPath = true;
            }
            if (f instanceof ScreenState.ColumnChunkDetail
                    || f instanceof ScreenState.ColumnAcrossRowGroups) {
                haveColInPath = true;
                haveRgInPath = true;
            }
        }
        Style dim = Theme.dim();
        for (int i = 0; i < frames.size(); i++) {
            if (i > 0) {
                spans.add(new Span(" › ", dim));
            }
            boolean last = i == frames.size() - 1;
            String label = breadcrumbLabel(frames.get(i), model);
            if (last) {
                label += leafContextSuffix(frames.get(i), model, haveRgInPath, haveColInPath);
                spans.add(new Span(label, Theme.primary()));
            }
            else {
                spans.add(new Span(label, dim));
            }
        }
        Paragraph.builder().text(convert(Line.from(spans))).left().build().render(area, buffer);
    }

    /// For per-chunk leaf screens (Pages, ColumnIndex, OffsetIndex,
    /// Dictionary), append "(RG #N · col)" when the earlier frames
    /// don't already establish that context — typically the
    /// Footer → FileIndexes drill path.
    private static String leafContextSuffix(ScreenState state, ParquetModel model,
                                            boolean haveRg, boolean haveCol) {
        int rg;
        int col;
        switch (state) {
            case ScreenState.Pages s -> { rg = s.rowGroupIndex(); col = s.columnIndex(); }
            case ScreenState.ColumnIndexView s -> { rg = s.rowGroupIndex(); col = s.columnIndex(); }
            case ScreenState.OffsetIndexView s -> { rg = s.rowGroupIndex(); col = s.columnIndex(); }
            case ScreenState.DictionaryView s -> { rg = s.rowGroupIndex(); col = s.columnIndex(); }
            default -> { return ""; }
        }
        if (haveRg && haveCol) {
            return "";
        }
        StringBuilder sb = new StringBuilder(" (");
        boolean first = true;
        if (!haveRg) {
            sb.append("RG #").append(rg);
            first = false;
        }
        if (!haveCol) {
            if (!first) {
                sb.append(" · ");
            }
            sb.append(model.schema().getColumn(col).fieldPath());
        }
        sb.append(")");
        return sb.toString();
    }

    public static void renderKeybar(Buffer buffer, Rect area, String screenKeys, String globalKeys) {
        Style style = Theme.dim();
        Line line = Line.from(
                Span.raw(" "),
                new Span(screenKeys, style),
                new Span(KEYBAR_SEP, style),
                new Span(globalKeys, style));
        Paragraph.builder()
                .text(convert(line))
                .left()
                .overflow(dev.tamboui.style.Overflow.WRAP_WORD)
                .build()
                .render(area, buffer);
    }

    /// Package-private for tests so they can assert breadcrumb labels
    /// per state without driving the full chrome render path.
    public static String breadcrumbLabel(ScreenState state, ParquetModel model) {
        return switch (state) {
            case ScreenState.Overview ignored -> "Overview";
            case ScreenState.Schema ignored -> "Schema";
            case ScreenState.RowGroups ignored -> "Row groups";
            case ScreenState.RowGroupDetail d -> "RG #" + d.rowGroupIndex();
            // The previous frame (RowGroupDetail) already shows "RG #N";
            // the indexes / column-chunks labels just say what the screen is.
            case ScreenState.RowGroupIndexes ignored -> "Indexes";
            case ScreenState.ColumnChunks ignored -> "Column chunks";
            // Show the column's leaf name (last path segment); the body of
            // ColumnChunkDetail / ColumnAcrossRowGroups also shows the full
            // path, so the chrome label gives the friendly name and the
            // body retains the disambiguation for nested fields.
            case ScreenState.ColumnChunkDetail d -> columnLeafName(model, d.columnIndex());
            case ScreenState.Pages ignored -> "Pages";
            case ScreenState.ColumnIndexView ignored -> "Column index";
            case ScreenState.OffsetIndexView ignored -> "Offset index";
            case ScreenState.Footer ignored -> "Footer & indexes";
            case ScreenState.ColumnAcrossRowGroups c -> columnLeafName(model, c.columnIndex()) + " across RGs";
            case ScreenState.DictionaryView ignored -> "Dictionary";
            case ScreenState.DataPreview ignored -> "Data preview";
            case ScreenState.FileIndexes f -> switch (f.kind()) {
                case COLUMN -> "All column indexes";
                case OFFSET -> "All offset indexes";
                case DICTIONARY -> "All dictionaries";
            };
        };
    }

    /// Leaf name of the column at the given leaf index — last segment of
    /// the column path (e.g. `id`, `address.street.number → "number"`).
    /// Long names are truncated from the left so the suffix stays
    /// distinctive.
    private static String columnLeafName(ParquetModel model, int columnIndex) {
        String path = model.schema().getColumn(columnIndex).fieldPath().toString();
        int dot = path.lastIndexOf('.');
        String leaf = dot >= 0 ? path.substring(dot + 1) : path;
        if (leaf.length() > 24) {
            return "…" + leaf.substring(leaf.length() - 23);
        }
        return leaf;
    }

    /// Last path segment — for the top bar we want just the file name,
    /// not the full path (cwd / workspace prefixes are noise). Works for
    /// both `/` and `\` separators (paths come from user-supplied
    /// filesystem strings or S3 keys; both use `/`, but local Windows
    /// paths use `\`). Falls back to the input on degenerate cases.
    private static String basename(String path) {
        if (path == null || path.isEmpty()) {
            return path == null ? "" : path;
        }
        int lastSlash = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
        if (lastSlash < 0 || lastSlash == path.length() - 1) {
            return path;
        }
        return path.substring(lastSlash + 1);
    }

    private static String formatRowCount(long rows) {
        if (rows < 1_000_000) {
            return rows + "";
        }
        return Fmt.fmt("%.1f M", rows / 1_000_000.0);
    }

    private static dev.tamboui.text.Text convert(Line line) {
        return dev.tamboui.text.Text.from(line);
    }
}
