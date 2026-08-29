/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal.table;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

public final class RowTable {

    private RowTable() {
    }

    static String[] topLevelFieldNames(FileSchema schema) {
        return topLevelFieldNames(schema, ColumnProjection.all());
    }

    public static String[] topLevelFieldNames(FileSchema schema, ColumnProjection projection) {
        List<SchemaNode> children = schema.getRootNode().children();
        if (projection.projectsAll()) {
            String[] names = new String[children.size()];
            for (int i = 0; i < children.size(); i++) {
                names[i] = children.get(i).name();
            }
            return names;
        }
        Set<String> projectedNames = projection.getProjectedColumnNames();
        return children.stream()
                .map(SchemaNode::name)
                .filter(name -> projectedNames.stream()
                        .anyMatch(p -> p.equals(name) || p.startsWith(name + ".")))
                .toArray(String[]::new);
    }

    public static String renderTable(String[] headers, List<String[]> rows) {
        return renderTable(headers, rows, Collections.emptyList(), Collections.emptyList(), false);
    }

    public static String renderTransposedTable(String[] headers, List<String[]> rows) {
        List<Integer> separatorsBefore = IntStream.range(1, rows.size()).boxed().toList();
        return renderTable(headers, rows, separatorsBefore, Collections.emptyList(), true);
    }

    /// Renders a table like [renderTable(String[], List)], but inserts a horizontal
    /// border line before each row whose index appears in `separatorsBefore`. Indices
    /// refer to positions within `rows` (0 = first data row). Rows listed in
    /// `heavySeparatorsBefore` get a heavier separator (`=` instead of `-`) to visually
    /// distinguish summary sections such as totals.
    ///
    /// Column widths are computed from terminal display width so that East Asian
    /// wide characters (CJK, Hangul, Kana, Fullwidth forms) contribute 2 cells each,
    /// keeping alignment correct across rows that mix Latin and wide-character text.
    public static String renderTable(String[] headers, List<String[]> rows,
                                     List<Integer> separatorsBefore,
                                     List<Integer> heavySeparatorsBefore) {
        return renderTable(headers, rows, separatorsBefore, heavySeparatorsBefore, false);
    }

    private static String renderTable(String[] headers, List<String[]> rows,
                                      List<Integer> separatorsBefore,
                                      List<Integer> heavySeparatorsBefore,
                                      boolean rightAlignHeaders) {
        int cols = headers.length;
        int[] widths = new int[cols];
        for (int i = 0; i < cols; i++) {
            widths[i] = displayWidth(headers[i]);
        }
        for (String[] row : rows) {
            for (int i = 0; i < cols; i++) {
                widths[i] = Math.max(widths[i], displayWidth(row[i]));
            }
        }

        String lightBorder = buildBorder(widths, '-');
        String heavyBorder = buildBorder(widths, '=');
        Set<Integer> lightSet = new HashSet<>(separatorsBefore);
        Set<Integer> heavySet = new HashSet<>(heavySeparatorsBefore);

        StringBuilder sb = new StringBuilder();
        sb.append(lightBorder).append('\n');
        sb.append(renderCells(headers, widths, rightAlignHeaders)).append('\n');
        sb.append(lightBorder).append('\n');
        for (int r = 0; r < rows.size(); r++) {
            if (heavySet.contains(r)) {
                sb.append(heavyBorder).append('\n');
            }
            else if (lightSet.contains(r)) {
                sb.append(lightBorder).append('\n');
            }
            sb.append(renderCells(rows.get(r), widths, true)).append('\n');
        }
        sb.append(lightBorder);
        return sb.toString();
    }

    private static String buildBorder(int[] widths, char fill) {
        StringBuilder sb = new StringBuilder();
        sb.append('+');
        for (int w : widths) {
            for (int i = 0; i < w + 2; i++) {
                sb.append(fill);
            }
            sb.append('+');
        }
        return sb.toString();
    }

    private static String renderCells(String[] cells, int[] widths, boolean rightAlign) {
        StringBuilder sb = new StringBuilder();
        sb.append('|');
        for (int i = 0; i < cells.length; i++) {
            String cell = cells[i];
            int padding = widths[i] - displayWidth(cell);
            sb.append(' ');
            if (rightAlign) {
                appendSpaces(sb, padding);
                sb.append(cell);
            }
            else {
                sb.append(cell);
                appendSpaces(sb, padding);
            }
            sb.append(' ');
            sb.append('|');
        }
        return sb.toString();
    }

    private static void appendSpaces(StringBuilder sb, int count) {
        for (int i = 0; i < count; i++) {
            sb.append(' ');
        }
    }

    /// Returns the number of terminal cells the string occupies. East Asian wide
    /// characters (CJK ideographs, Hangul, Kana, Fullwidth forms) count as 2; other
    /// characters count as 1. Surrogate pairs are counted once per code point.
    static int displayWidth(String s) {
        int width = 0;
        int i = 0;
        int len = s.length();
        while (i < len) {
            int cp = s.codePointAt(i);
            width += charWidth(cp);
            i += Character.charCount(cp);
        }
        return width;
    }

    /// Returns the min-content width of the string: the number of terminal cells taken
    /// by its widest single code point, i.e. the narrowest column the string can be
    /// wrapped into without a glyph overflowing. Empty strings have a width of 1.
    static int widestGlyph(String s) {
        int widest = 1;
        int i = 0;
        int len = s.length();
        while (i < len) {
            int cp = s.codePointAt(i);
            widest = Math.max(widest, charWidth(cp));
            i += Character.charCount(cp);
        }
        return widest;
    }

    /// Returns the number of terminal cells taken by the string's first code point,
    /// i.e. the narrowest column that can render any of the string at all. Empty
    /// strings have a width of 1.
    static int firstGlyph(String s) {
        return s.isEmpty() ? 1 : charWidth(s.codePointAt(0));
    }

    /// Returns the number of terminal cells a single code point occupies: 2 for East
    /// Asian wide characters (CJK ideographs, Hangul, Kana, Fullwidth forms), 1 otherwise.
    static int charWidth(int cp) {
        return isWideCodePoint(cp) ? 2 : 1;
    }

    private static boolean isWideCodePoint(int cp) {
        return (cp >= 0x1100 && cp <= 0x115F)     // Hangul Jamo
                || (cp >= 0x2E80 && cp <= 0x303E) // CJK Radicals, Kangxi, CJK Symbols & Punctuation
                || (cp >= 0x3041 && cp <= 0x33FF) // Hiragana, Katakana, Bopomofo, Hangul Compat, CJK Strokes
                || (cp >= 0x3400 && cp <= 0x4DBF) // CJK Unified Ideographs Extension A
                || (cp >= 0x4E00 && cp <= 0x9FFF) // CJK Unified Ideographs
                || (cp >= 0xA000 && cp <= 0xA4CF) // Yi Syllables & Radicals
                || (cp >= 0xAC00 && cp <= 0xD7A3) // Hangul Syllables
                || (cp >= 0xF900 && cp <= 0xFAFF) // CJK Compatibility Ideographs
                || (cp >= 0xFE30 && cp <= 0xFE4F) // CJK Compatibility Forms
                || (cp >= 0xFF00 && cp <= 0xFF60) // Fullwidth Forms
                || (cp >= 0xFFE0 && cp <= 0xFFE6) // Fullwidth Signs
                || (cp >= 0x20000 && cp <= 0x2FFFD) // CJK Extensions B–F
                || (cp >= 0x30000 && cp <= 0x3FFFD); // CJK Extension G
    }
}
