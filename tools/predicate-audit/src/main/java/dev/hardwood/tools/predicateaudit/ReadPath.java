/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.tools.predicateaudit;

import java.util.ArrayList;
import java.util.List;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ReaderConfig;
import dev.hardwood.reader.RowReader;

/// The routes a filter takes through Hardwood. A literal resolved wrongly can show on one of them only.
enum ReadPath {

    ROW_READER("RowReader", false, true, false),
    ROW_READER_RECORD_PATH("RowReader/record-path", false, true, true),
    ROW_READER_NO_METADATA("RowReader/no-metadata", false, false, false),
    COLUMN_READER("ColumnReader", true, true, false),
    COLUMN_READER_NO_METADATA("ColumnReader/no-metadata", true, false, false);

    /// A config that makes the reader decode every row group and page and answer from values alone.
    static final ReaderConfig NO_METADATA = ReaderConfig.builder().option("hardwood.metadata-filtering", "false").build();

    /// A comparison no batch matcher takes, on the required column `zz` whose every value is `z`: it
    /// matches no row, and `or`-ing it in pushes a filter onto the record-level path.
    private static final FilterPredicate NEVER = FilterPredicate.lt("zz", new byte[0]);

    final String label;
    private final boolean columnReader;
    final boolean metadataFiltering;
    private final boolean forceRecordPath;

    ReadPath(String label, boolean columnReader, boolean metadataFiltering, boolean forceRecordPath) {
        this.label = label;
        this.columnReader = columnReader;
        this.metadataFiltering = metadataFiltering;
        this.forceRecordPath = forceRecordPath;
    }

    /// The `__row__` values of the rows `filter` returns, as [#show(List)] renders them, or the
    /// exception it throws.
    String read(ParquetFileReader reader, FilterPredicate filter) {
        FilterPredicate effective = forceRecordPath ? FilterPredicate.or(filter, NEVER) : filter;
        List<Long> rows = new ArrayList<>();
        try {
            if (columnReader) {
                try (ColumnReader columns = reader.buildColumnReader("__row__").filter(effective).build()) {
                    while (columns.nextBatch()) {
                        long[] values = columns.getLongs();
                        for (int i = 0; i < columns.getRecordCount(); i++) {
                            rows.add(values[i]);
                        }
                    }
                }
            }
            else {
                try (RowReader rowReader = reader.buildRowReader().filter(effective).build()) {
                    while (rowReader.hasNext()) {
                        rowReader.next();
                        rows.add(rowReader.getLong("__row__"));
                    }
                }
            }
        }
        catch (Exception e) {
            return "THROW " + e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        return show(rows);
    }

    /// A row list as its count, followed by the rows where there are few, or a hash of them.
    static String show(List<Long> rows) {
        return rows.size() + " " + (rows.size() <= 12 ? rows.toString() : "#" + Integer.toHexString(rows.hashCode()));
    }
}
