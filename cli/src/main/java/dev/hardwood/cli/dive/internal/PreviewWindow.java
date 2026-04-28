/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.internal.RowValueFormatter;
import dev.hardwood.schema.SchemaNode;

/// A buffered window of pre-formatted Data preview rows around the
/// current viewport.
///
/// **Refill-on-miss semantics.** The buffer covers a range
/// `[start, end)` of absolute row indices, sized at `20 × pageSize`
/// (clamped at file boundaries). On each
/// `slice(model, firstRow, pageSize, logicalTypes)`:
///
/// - **Page fully inside the buffer:** return the slice — zero I/O.
/// - **Page outside the buffer:** *replace* the buffer with a fresh
///   range `[firstRow − 10 × pageSize, firstRow + 10 × pageSize)`
///   (clamped to file bounds), fetched in a single
///   [ParquetModel#readPreviewPage] call.
///
/// **`logicalTypes` toggle does not invalidate.** Each cell is
/// pre-formatted twice on fetch — once with logical-type rendering
/// active, once without — and stored alongside the expanded variants.
/// Toggling `t` in dive just selects which pair the slice returns;
/// no fetch, no decode. The cost is ~4× the per-cell string memory
/// (a few extra MB for typical Overture-shape schemas), bounded by
/// the window size and far cheaper than re-fetching on every toggle.
final class PreviewWindow {

    /// Number of `pageSize`-strides kept on each side of the requested
    /// `firstRow`. ±10 strides means each refill covers
    /// `20 × pageSize` rows total (10 before + 10 starting at
    /// firstRow). On every miss the buffer is *fully replaced* with
    /// this range, so as the user navigates forward (e.g. firstRow
    /// advances from 300 to 600) the previous front rows naturally
    /// get evicted — memory stays bounded at `20 × pageSize` rather
    /// than growing.
    private static final int SLACK_PAGES = 10;

    private ParquetModel cachedFor;
    private long start;            // inclusive
    private long end;              // exclusive
    private List<List<String>> rowsLogical;
    private List<List<String>> rowsPhysical;
    private List<List<String>> expandedLogical;
    private List<List<String>> expandedPhysical;
    private List<String> columnNames;
    private List<SchemaNode> topLevelFields;

    /// Returns a `pageSize`-row slice starting at `firstRow`, formatted
    /// according to `logicalTypes`. If the requested page is in the
    /// current buffer, it's served from memory (regardless of which
    /// mode was active when the buffer was filled).
    Slice slice(ParquetModel model, long firstRow, int pageSize, boolean logicalTypes) {
        long total = model.facts().totalRows();
        long requestStart = Math.max(0, firstRow);
        long requestEnd = Math.min(total, requestStart + pageSize);

        boolean modelChanged = cachedFor != model;
        boolean uninitialised = rowsLogical == null;
        if (modelChanged || uninitialised) {
            initialise(model);
        }

        boolean pageInBuffer = !rowsLogical.isEmpty()
                && requestStart >= start
                && requestEnd <= end;
        if (!pageInBuffer) {
            refill(model, firstRow, pageSize, total);
        }

        long sliceStart = Math.max(requestStart, start);
        long sliceEnd = Math.min(requestEnd, end);
        if (sliceStart >= sliceEnd) {
            return new Slice(List.of(), List.of(), columnNames);
        }
        int from = Math.toIntExact(sliceStart - start);
        int to = Math.toIntExact(sliceEnd - start);
        List<List<String>> rows = logicalTypes ? rowsLogical : rowsPhysical;
        List<List<String>> expanded = logicalTypes ? expandedLogical : expandedPhysical;
        return new Slice(
                List.copyOf(rows.subList(from, to)),
                List.copyOf(expanded.subList(from, to)),
                columnNames);
    }

    private void initialise(ParquetModel model) {
        this.cachedFor = model;
        this.topLevelFields = model.schema().getRootNode().children();
        this.columnNames = new ArrayList<>(topLevelFields.size());
        for (SchemaNode node : topLevelFields) {
            columnNames.add(node.name());
        }
        this.rowsLogical = new ArrayList<>();
        this.rowsPhysical = new ArrayList<>();
        this.expandedLogical = new ArrayList<>();
        this.expandedPhysical = new ArrayList<>();
        this.start = 0;
        this.end = 0;
    }

    /// Drops the current buffer and fetches a fresh `[firstRow −
    /// 10·pageSize, firstRow + 10·pageSize)` range (clamped to file
    /// bounds) in a single call. Each row is formatted in both
    /// logical-type and physical-type modes so subsequent `t` toggles
    /// don't trigger I/O.
    private void refill(ParquetModel model, long firstRow, int pageSize, long total) {
        long newStart = Math.max(0, firstRow - (long) SLACK_PAGES * pageSize);
        long newEnd = Math.min(total, firstRow + (long) SLACK_PAGES * pageSize);
        rowsLogical.clear();
        rowsPhysical.clear();
        expandedLogical.clear();
        expandedPhysical.clear();
        start = newStart;
        end = newStart;
        long count = newEnd - newStart;
        if (count <= 0) {
            return;
        }
        fetch(model, newStart, Math.toIntExact(count));
        end = start + rowsLogical.size();
    }

    private void fetch(ParquetModel model, long firstRow, int count) {
        int fieldCount = topLevelFields.size();
        try {
            model.readPreviewPage(firstRow, count, reader -> {
                List<String> rowLogical = new ArrayList<>(fieldCount);
                List<String> rowPhysical = new ArrayList<>(fieldCount);
                List<String> expLogical = new ArrayList<>(fieldCount);
                List<String> expPhysical = new ArrayList<>(fieldCount);
                for (int c = 0; c < fieldCount; c++) {
                    SchemaNode field = topLevelFields.get(c);
                    rowLogical.add(RowValueFormatter.format(reader, c, field, true));
                    rowPhysical.add(RowValueFormatter.format(reader, c, field, false));
                    expLogical.add(RowValueFormatter.formatExpanded(reader, c, field, true));
                    expPhysical.add(RowValueFormatter.formatExpanded(reader, c, field, false));
                }
                rowsLogical.add(rowLogical);
                rowsPhysical.add(rowPhysical);
                expandedLogical.add(expLogical);
                expandedPhysical.add(expPhysical);
            });
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /// A `pageSize`-row slice carved from the window — exactly what the
    /// `DataPreview` screen state holds.
    record Slice(List<List<String>> rows, List<List<String>> expandedRows, List<String> columnNames) {
    }
}
