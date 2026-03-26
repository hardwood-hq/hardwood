/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.table;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

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
            var next = iterator.next();
            sampleRows.add(IntStream.range(0, headers.length)
                    .mapToObj(next)
                    .toArray(String[]::new));
        }

        // compute column widths based on headers + sample rows
        int[] widths = new int[n];
        for (int i = 0; i < n; i++) {
            widths[i] = headers[i].length();
        }
        for (String[] rowFunc : sampleRows) {
            for (int i = 0; i < n; i++) {
                String cell = rowFunc[i];
                if (cell != null) {
                    widths[i] = Math.max(widths[i], cell.length());
                }
            }
        }

        for (int i = 0; i < n; i++) {
            widths[i] = Math.min(widths[i], maxWidth);
        }

        String sep = makeSeparator(widths);

        out.println(sep);
        printRow(out, i -> headers[i], widths, truncate);
        out.println(sep);

        // catch up the sampled rows
        for (String[] rowFunc : sampleRows) {
            printRow(out, i -> rowFunc[i], widths, truncate);
            if (rowDelimiter) {
                out.println(sep);
            }
        }

        // finish the dataset content
        while (iterator.hasNext()) {
            IntFunction<String> rowFunc = iterator.next();
            printRow(out, rowFunc, widths, truncate);
            if (rowDelimiter) {
                out.println(sep);
            }
        }

        if (!rowDelimiter && !sampleRows.isEmpty()) {
            out.println(sep);
        }

        out.flush();
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
                if (cell.length() > widths[i]) {
                    cell = cell.substring(0, widths[i] - 1) + "…";
                }
                out.printf(" %-" + widths[i] + "s |", cell);
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
            for (int start = 0; start < cell.length(); start += widths[i]) {
                int end = Math.min(start + widths[i], cell.length());
                lines.add(cell.substring(start, end));
            }
            maxLines = Math.max(maxLines, lines.size());
            wrappedCells.add(lines.toArray(new String[0]));
        }

        for (int line = 0; line < maxLines; line++) {
            out.print("|");
            for (int i = 0; i < n; i++) {
                String[] lines = wrappedCells.get(i);
                String content = (line < lines.length) ? lines[line] : "";
                out.printf(" %-" + widths[i] + "s |", content);
            }
            out.println();
        }
    }
}
