/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.CountingInputFile;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// A [ParquetFileReader] tracks the [dev.hardwood.internal.reader.RowGroupIterator]s
/// it hands to child readers so that [ParquetFileReader#close()] can tear down an
/// iterator whose child reader the caller never closed.
///
/// Where a child exclusively owns its iterator — the single-column
/// [ParquetFileReader#columnReader(int)] path — that tracking must not outlive the
/// child, or a reader used for many sequential column reads retains every finished
/// child's work list until it is itself closed.
class IteratorTrackingTest {

    private static final String FILE = "src/test/resources/page_index_test.parquet";
    /// A nested schema, so the row path routes through `NestedRowReader` rather
    /// than the `FlatRowReader` every other test here exercises.
    private static final String NESTED_FILE = "src/test/resources/deep_nested_struct_test.parquet";
    /// Higher than any `id` in [#FILE], so statistics pruning drops every row
    /// group and the read is served by an all-exhausted group.
    private static final long PRUNES_EVERY_ROW_GROUP = 100_000_000L;

    @Test
    void closingAChildReaderStopsTrackingItsIterator() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(Paths.get(FILE)))) {
            assertThat(reader.trackedIteratorCount()).isZero();

            for (int i = 0; i < 4; i++) {
                ColumnReader columnReader = reader.columnReader(0);
                assertThat(reader.trackedIteratorCount())
                        .as("an open child reader's iterator is tracked")
                        .isEqualTo(1);
                while (columnReader.nextBatch()) {
                    // drain
                }
                columnReader.close();
                assertThat(reader.trackedIteratorCount())
                        .as("iteration %d must not leave its iterator behind", i)
                        .isZero();
            }

            // The row-reader and multi-column paths share one iterator across
            // sibling readers. No individual child owns it, so the group that does
            // — the RowReader, the ColumnReaders — closes it.
            try (RowReader rows = reader.rowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
            assertThat(reader.trackedIteratorCount())
                    .as("a closed row reader must not leave its iterator behind")
                    .isZero();

            try (ColumnReaders columns = reader.columnReaders(ColumnProjection.columns("id"))) {
                while (columns.nextBatch()) {
                    // drain
                }
            }
            assertThat(reader.trackedIteratorCount())
                    .as("a closed column-readers group must not leave its iterator behind")
                    .isZero();
        }
    }

    /// The retention this tracking can cause is unbounded rather than one-off: a
    /// reader driving many sequential reads holds every finished read's work list,
    /// page-index buffers and fetch plans until it is itself closed.
    @Test
    void sequentialReadsDoNotAccumulateIterators() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(Paths.get(FILE)))) {
            for (int i = 0; i < 8; i++) {
                try (RowReader rows = reader.rowReader()) {
                    rows.next();
                }
                try (ColumnReaders columns = reader.columnReaders(ColumnProjection.columns("id"))) {
                    columns.nextBatch();
                }
                try (ColumnReader column = reader.columnReader(0)) {
                    column.nextBatch();
                }
                assertThat(reader.trackedIteratorCount())
                        .as("iteration %d must not leave an iterator behind", i)
                        .isZero();
            }
        }
    }

    /// The filtered paths hand their readers to a `FilterCoordinator`, which closes
    /// the whole projection rather than one child. That must release the shared
    /// iterator too.
    @Test
    void closingAFilteredChildReaderStopsTrackingItsIterator() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(Paths.get(FILE)))) {
            for (int i = 0; i < 4; i++) {
                try (RowReader rows = reader.buildRowReader()
                        .filter(FilterPredicate.gt("id", 1L))
                        .build()) {
                    if (rows.hasNext()) {
                        rows.next();
                    }
                }
                try (ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("id"))
                        .filter(FilterPredicate.gt("id", 1L))
                        .build()) {
                    columns.nextBatch();
                }
                assertThat(reader.trackedIteratorCount())
                        .as("filtered iteration %d must not leave an iterator behind", i)
                        .isZero();
            }
        }
    }

    /// A filtered single-column read is served by the shared filtered-projection
    /// engine and exposes one of its readers. The enclosing group is never handed
    /// to the caller, so closing that one reader has to release the iterator the
    /// group was built around.
    @Test
    void closingAFilteredSingleColumnReaderStopsTrackingItsIterator() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(Paths.get(FILE)))) {
            for (int i = 0; i < 4; i++) {
                try (ColumnReader column = reader.buildColumnReader(0)
                        .filter(FilterPredicate.gt("id", 1L))
                        .build()) {
                    column.nextBatch();
                }
                assertThat(reader.trackedIteratorCount())
                        .as("filtered single-column iteration %d must not leave an iterator behind", i)
                        .isZero();
            }
        }
    }

    /// The nested row path is a separate reader class with its own `close()`, so
    /// the flat fixture every other test uses would not catch a regression there.
    @Test
    void closingANestedRowReaderStopsTrackingItsIterator() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(Paths.get(NESTED_FILE)))) {
            for (int i = 0; i < 4; i++) {
                try (RowReader rows = reader.rowReader()) {
                    while (rows.hasNext()) {
                        rows.next();
                    }
                }
                assertThat(reader.trackedIteratorCount())
                        .as("nested iteration %d must not leave an iterator behind", i)
                        .isZero();
            }
        }
    }

    /// A read whose row groups are all pruned is served by a group of exhausted
    /// readers with no coordinator. The group still holds the iterator, so closing
    /// it releases it.
    @Test
    void closingAPrunedColumnReadersGroupStopsTrackingItsIterator() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(Paths.get(FILE)))) {
            for (int i = 0; i < 4; i++) {
                try (ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("id"))
                        .filter(FilterPredicate.gt("id", PRUNES_EVERY_ROW_GROUP))
                        .build()) {
                    assertThat(columns.nextBatch()).isFalse();
                }
                assertThat(reader.trackedIteratorCount())
                        .as("pruned iteration %d must not leave an iterator behind", i)
                        .isZero();
            }
        }
    }

    /// The pruned counterpart of
    /// [#closingAFilteredSingleColumnReaderStopsTrackingItsIterator]: the caller is
    /// handed one exhausted reader out of a group it never sees, and that group has
    /// no coordinator to route the close through, so the reader itself has to
    /// release the iterator.
    @Test
    void closingAPrunedFilteredSingleColumnReaderStopsTrackingItsIterator() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(Paths.get(FILE)))) {
            for (int i = 0; i < 4; i++) {
                try (ColumnReader column = reader.buildColumnReader(0)
                        .filter(FilterPredicate.gt("id", PRUNES_EVERY_ROW_GROUP))
                        .build()) {
                    assertThat(column.nextBatch()).isFalse();
                }
                assertThat(reader.trackedIteratorCount())
                        .as("pruned single-column iteration %d must not leave an iterator behind", i)
                        .isZero();
            }
        }
    }

    @Test
    void parentCloseStillTearsDownAnUnclosedChildsIterator() throws Exception {
        CountingInputFile file = new CountingInputFile(InputFile.of(Paths.get(FILE)));
        ParquetFileReader reader = ParquetFileReader.open(file);

        ColumnReader leaked = reader.columnReader(0);
        assertThat(leaked.nextBatch()).isTrue();
        assertThat(reader.trackedIteratorCount())
                .as("the never-closed child is still tracked")
                .isEqualTo(1);

        reader.close();

        assertThat(reader.trackedIteratorCount()).isZero();
        assertThat(file.closeCount())
                .as("the parent still owns and closes the input")
                .isEqualTo(1);
    }
}
