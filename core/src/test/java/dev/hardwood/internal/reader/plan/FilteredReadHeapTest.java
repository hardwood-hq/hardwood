/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.forked.ForkedJvm;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.ObservedPool;
import dev.hardwood.internal.reader.plan.GatedReads.ReadGate;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A filtered read whose filter column is not projected holds no more of the file than its
/// look-ahead while its consumer is behind, where statistics prove the filter for whole row
/// groups, so that the filter column reads none of their pages (#1814).
///
/// The read runs in a JVM of 192 MiB ([ForkedJvm]), which exits on an `OutOfMemoryError`, over 60
/// row groups of 100,000 rows: a key and eight `INT64` columns. The filter drops the first row
/// group and statistics prove it for every other, whose eight columns are 378 MB once decoded. The
/// consumer holds after its first batch, as one slower than the decodes would, until the read can
/// make no progress: a read whose look-ahead is bounded settles at it, and one whose look-ahead
/// runs away decodes the rest of the file and exhausts the heap first.
@ForkedJvm(jvmArgs = { "-Xmx192m", "-XX:+ExitOnOutOfMemoryError" }, timeoutSeconds = 60)
class FilteredReadHeapTest {

    private static final int ROW_GROUPS = 60;
    private static final int ROWS_PER_GROUP = 100_000;
    private static final int COLUMNS = 8;
    /// the filter's lower bound, the second row group's first key: statistics drop the first row
    /// group and prove every later one
    private static final long FROM_KEY = ROWS_PER_GROUP;
    private static final long ROWS = (long) ROW_GROUPS * ROWS_PER_GROUP - FROM_KEY;

    /// Reads the file's eight `INT64` columns where the key is at least [#FROM_KEY], holding after
    /// the first batch until the read settles, and reads every matching row.
    @Test
    void consumerBehindTheDecodesHoldsNoMoreThanTheLookAhead(@TempDir Path workDir) throws Exception {
        Path file = workDir.resolve("filtered.parquet");
        write(file);
        String[] columns = new String[COLUMNS];
        for (int c = 0; c < COLUMNS; c++) {
            columns[c] = "c" + c;
        }
        long rows = 0;
        ObservedPool pool = new ObservedPool(Runtime.getRuntime().availableProcessors(), "filtered-read-heap-test");
        QuiescentExecutor io = new QuiescentExecutor();
        try (HardwoodContextImpl context = pool.context(io);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file), context);
                ColumnReaders readers = reader.buildColumnReaders(ColumnProjection.columns(columns))
                        .filter(FilterPredicate.gtEq("key", FROM_KEY))
                        .build()) {
            while (readers.nextBatch()) {
                if (rows == 0) {
                    GatedReads.awaitSettled(io, new ReadGate(reason -> false), pool);
                }
                rows += readers.getRecordCount();
            }
        }
        assertThat(rows).isEqualTo(ROWS);
    }

    private static void write(Path file) throws Exception {
        FileSchema.Builder schema = FileSchema.builder("schema")
                .primitive("key", PhysicalType.INT64, RepetitionType.REQUIRED);
        for (int c = 0; c < COLUMNS; c++) {
            schema.primitive("c" + c, PhysicalType.INT64, RepetitionType.REQUIRED);
        }
        long[] keys = new long[ROWS_PER_GROUP];
        long[] values = new long[ROWS_PER_GROUP];
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema.build(),
                WriterConfig.builder().rowGroupTargetRows(ROWS_PER_GROUP).build())) {
            for (int g = 0; g < ROW_GROUPS; g++) {
                for (int i = 0; i < ROWS_PER_GROUP; i++) {
                    keys[i] = (long) g * ROWS_PER_GROUP + i;
                    values[i] = keys[i] % 7;
                }
                writer.columnWriter().writeBatch(batch -> {
                    batch.longs("key", keys);
                    for (int c = 0; c < COLUMNS; c++) {
                        batch.longs("c" + c, values);
                    }
                });
            }
        }
    }
}
