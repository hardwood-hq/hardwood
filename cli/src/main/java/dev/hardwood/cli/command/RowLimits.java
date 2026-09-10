/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.util.List;

import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetFileReader.RowReaderBuilder;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

/// Shared `-n / --rows`, `--skip` and `--row-group` parsing and application
/// for CLI commands that read rows (`print`, `convert`, ...).
final class RowLimits {

    static final String ALL = "ALL";

    private RowLimits() {
    }

    /// Parses the raw `-n` argument into a row-limit integer:
    /// positive for head, negative for tail, `0` for no limit (`ALL`).
    /// Throws [IllegalArgumentException] for invalid input, including
    /// an explicit `0` (which would otherwise collide with the no-limit sentinel).
    static int parse(String value) {
        if (ALL.equalsIgnoreCase(value)) {
            return 0;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(value);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid value for option '-n': expected a non-zero integer or 'ALL', got '" + value + "'");
        }
        if (parsed == 0) {
            throw new IllegalArgumentException(
                    "Invalid value for option '-n': expected a non-zero integer or 'ALL', got '0'");
        }
        return parsed;
    }

    /// The rows a command reads: where it starts, and how many it takes.
    /// `limit` is positive for head, negative for tail, `0` for no limit.
    record RowWindow(long skip, long limit) {
    }

    /// Resolves `--skip` and `--row-group` into the window to read, on top of
    /// the already parsed `-n` limit. The two options name a starting point in
    /// two ways, so only one of them may be given.
    static RowWindow resolveWindow(FileMetaData metadata, Long skip, Integer rowGroup, int rowLimit) {
        if (skip != null && rowGroup != null) {
            throw new IllegalArgumentException("--skip and --row-group cannot be combined: both name where to start reading");
        }
        if (skip == null && rowGroup == null) {
            return new RowWindow(0, rowLimit);
        }
        if (rowLimit < 0) {
            throw new IllegalArgumentException("A negative '-n' counts the last rows of the file, so it cannot be combined with "
                    + (skip != null ? "--skip" : "--row-group"));
        }
        if (skip != null) {
            if (skip < 0) {
                throw new IllegalArgumentException(
                        "Invalid value for option '--skip': expected a non-negative row number, got '" + skip + "'");
            }
            if (skip >= metadata.numRows()) {
                throw new IllegalArgumentException(
                        "Cannot skip " + skip + " rows (file has " + metadata.numRows() + ")");
            }
            return new RowWindow(skip, rowLimit);
        }
        List<RowGroup> rowGroups = metadata.rowGroups();
        if (rowGroup < 0 || rowGroup >= rowGroups.size()) {
            throw new IllegalArgumentException("No such row group: " + rowGroup + " (file has " + rowGroups.size() + ")");
        }
        long firstRow = 0;
        for (int i = 0; i < rowGroup; i++) {
            firstRow += rowGroups.get(i).numRows();
        }
        // The row group bounds the read; an explicit `-n` may narrow it further,
        // but never past the group into the next one.
        long numRows = rowGroups.get(rowGroup).numRows();
        return new RowWindow(firstRow, rowLimit > 0 ? Math.min(rowLimit, numRows) : numRows);
    }

    /// Builds a [RowReader] over the window: `skip` rows are passed over, then
    /// a positive limit takes that many rows from the start, a negative one
    /// that many from the end, and zero takes every row.
    static RowReader buildRowReader(ParquetFileReader reader, ColumnProjection projection, RowWindow window) throws IOException {
        RowReaderBuilder builder = reader.buildRowReader().projection(projection);
        if (window.skip() > 0) {
            builder.skip(window.skip());
        }
        if (window.limit() > 0) {
            builder.head(window.limit());
        }
        else if (window.limit() < 0) {
            builder.tail(-window.limit());
        }
        return builder.build();
    }
}
