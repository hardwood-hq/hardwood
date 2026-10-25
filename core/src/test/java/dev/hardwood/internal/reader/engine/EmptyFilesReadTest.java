/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.plan.RowGroupIterators;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A read of several files that are all empty, without a row group or with row groups of no rows,
/// has no live work item: the readers start no column, and a column read and a row read return no
/// row.
class EmptyFilesReadTest {

    private static final int FILES = 3;

    enum Kind {
        /// Written without a row, so the footer lists no row group.
        NO_ROW_GROUPS,
        /// One row group of 0 rows (`empty_row_group.parquet`, written by PyArrow).
        ZERO_ROWS
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aReadOfEmptyFilesHasNoLiveWorkItem(Kind kind) throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             RowGroupIterator iterator = RowGroupIterators.initialized(files(kind), context, ColumnProjection.all(),
                     null, 0)) {
            assertThat(iterator.hasLiveWorkItem()).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void columnAndRowReadsOfEmptyFilesReturnNoRow(Kind kind) throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             ParquetFileReader reader = ParquetFileReader.openAll(files(kind), context)) {
            try (ColumnReaders columns = reader.columnReaders(ColumnProjection.all())) {
                assertThat(columns.nextBatch()).isFalse();
            }
            try (RowReader rows = reader.rowReader()) {
                assertThat(rows.hasNext()).isFalse();
            }
        }
    }

    private static List<InputFile> files(Kind kind) throws Exception {
        List<InputFile> files = new ArrayList<>();
        for (int i = 0; i < FILES; i++) {
            files.add(kind == Kind.ZERO_ROWS ? InputFile.of(Path.of("src/test/resources/empty_row_group.parquet"))
                    : InputFile.of(ByteBuffer.wrap(noRowGroups())));
        }
        return files;
    }

    private static byte[] noRowGroups() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out,
                FileSchema.builder("empty").addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED).build(),
                WriterConfig.builder().build())) {
            // no row
        }
        return InMemoryFiles.toByteArray(out);
    }
}
