/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal.table;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import dev.hardwood.cli.internal.Strings;

// note: align text left since it is how people do read in english
public class StreamedTable {

    public void print(PrintWriter out, String[] headers,
               Iterator<IntFunction<String>> iterator,
               int sampleSize, int maxWidth, boolean truncate, boolean rowDelimiter) {
        int n = headers.length;

        // sample a bit the rows so we can better adjust the widths
        List<String[]> sampleRows = new ArrayList<>();
        int count = 0;
        while (count++ < sampleSize && iterator.hasNext()) {
            IntFunction<String> next = iterator.next();
            sampleRows.add(IntStream.range(0, headers.length)
                    .mapToObj(next)
                    .toArray(String[]::new));
        }

        // compute column widths based on headers + sample rows
        int[] widths = new int[n];
        int[] minWidths = new int[n];
        for (int i = 0; i < n; i++) {
            widths[i] = Strings.width(headers[i]);
            minWidths[i] = mandatoryGlyph(headers[i], truncate);
        }
        for (String[] rowFunc : sampleRows) {
            for (int i = 0; i < n; i++) {
                String cell = rowFunc[i];
                if (cell != null) {
                    widths[i] = Math.max(widths[i], Strings.width(cell));
                    minWidths[i] = Math.max(minWidths[i], mandatoryGlyph(cell, truncate));
                }
            }
        }

        for (int i = 0; i < n; i++) {
            widths[i] = Math.max(Math.min(widths[i], maxWidth),
                    minColumnWidth(minWidths[i], widths[i], truncate));
        }

        String sep = makeSeparator(widths);

        out.println(sep);
        printRow(out, i -> headers[i], widths, truncate);
        out.println(sep);

        boolean emittedDataRow = false;

        // catch up the sampled rows
        for (String[] rowFunc : sampleRows) {
            printRow(out, i -> rowFunc[i], widths, truncate);
            emittedDataRow = true;
            if (rowDelimiter) {
                out.println(sep);
            }
        }

        // finish the dataset content
        while (iterator.hasNext()) {
            IntFunction<String> rowFunc = iterator.next();
            printRow(out, rowFunc, widths, truncate);
            emittedDataRow = true;
            if (rowDelimiter) {
                out.println(sep);
            }
        }

        if (!rowDelimiter && emittedDataRow) {
            out.println(sep);
        }

        out.flush();
    }

    /// The glyph of `value` the column is obliged to render, in cells.
    ///
    /// Wrapping owes every glyph a line, so it is the widest one anywhere in the value.
    /// Truncating owes only the first: everything after it is a candidate for being cut,
    /// and sizing the column to a glyph that the ellipsis may well replace pads it out
    /// with space no render can reach.
    private static int mandatoryGlyph(String value, boolean truncate) {
        return truncate ? Strings.firstGlyph(value) : Strings.widestGlyph(value);
    }

    /// The narrowest a column can be and still render its content faithfully.
    ///
    /// Wrapping needs room for the glyph itself, since nothing splits a glyph across
    /// lines. Truncating needs one cell more, for the ellipsis that sits next to it —
    /// without that cell the ellipsis crowds out the character entirely and the column
    /// shows a marker and no content. A column whose values all fit is never truncated,
    /// so it keeps the plain glyph floor.
    private static int minColumnWidth(int mandatoryGlyph, int naturalWidth, boolean truncate) {
        if (!truncate || naturalWidth <= mandatoryGlyph) {
            return mandatoryGlyph;
        }
        return mandatoryGlyph + 1;
    }

    private String makeSeparator(int[] widths) {
        StringBuilder sb = new StringBuilder("+");
        for (int w : widths) {
            sb.repeat("-", w + 2).append("+");
        }
        return sb.toString();
    }

    private void printRow(PrintWriter out, IntFunction<String> rowFunc, int[] widths, boolean truncate) {
        int n = widths.length;
        if (truncate) {
            out.print("|");
            for (int i = 0; i < n; i++) {
                String cell = rowFunc.apply(i);
                if (cell == null) {
                    cell = "";
                }
                cell = Strings.truncateRight(cell, widths[i]);
                printCell(out, cell, widths[i]);
            }
            out.println();
            return;
        }

        List<String[]> wrappedCells = new ArrayList<>();
        int maxLines = 0;

        for (int i = 0; i < n; i++) {
            String cell = rowFunc.apply(i);
            if (cell == null) {
                cell = "";
            }
            List<String> lines = new ArrayList<>();
            if (cell.isEmpty()) {
                lines.add("");
            }
            for (int start = 0; start < cell.length();) {
                String chunk = Strings.prefixByWidth(cell, start, widths[i]);
                lines.add(chunk);
                start += chunk.length();
            }
            maxLines = Math.max(maxLines, lines.size());
            wrappedCells.add(lines.toArray(new String[0]));
        }

        for (int line = 0; line < maxLines; line++) {
            out.print("|");
            for (int i = 0; i < n; i++) {
                String[] lines = wrappedCells.get(i);
                String content = (line < lines.length) ? lines[line] : "";
                printCell(out, content, widths[i]);
            }
            out.println();
        }
    }

    private void printCell(PrintWriter out, String content, int width) {
        out.print(" ");
        out.print(content);
        out.print(" ".repeat(Math.max(0, width - Strings.width(content))));
        out.print(" |");
    }

}
