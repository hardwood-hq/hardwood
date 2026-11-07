/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;

import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

/// The two ways a test reads a filtered projection of a file: through a [RowReader], or through
/// [ColumnReaders].
enum ReadKind {

    ROW_READER {
        @Override
        long count(ParquetFileReader reader, ColumnProjection projection, FilterPredicate filter) throws IOException {
            long rows = 0;
            try (RowReader rowReader = reader.buildRowReader().projection(projection).filter(filter).build()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                    rows++;
                }
            }
            return rows;
        }
    },
    COLUMN_READERS {
        @Override
        long count(ParquetFileReader reader, ColumnProjection projection, FilterPredicate filter) throws IOException {
            long rows = 0;
            try (ColumnReaders columns = reader.buildColumnReaders(projection).filter(filter).build()) {
                while (columns.nextBatch()) {
                    rows += columns.getRecordCount();
                }
            }
            return rows;
        }
    };

    /// Reads `projection` of `reader`'s files under `filter` to the end; returns the rows read.
    abstract long count(ParquetFileReader reader, ColumnProjection projection, FilterPredicate filter)
            throws IOException;
}
