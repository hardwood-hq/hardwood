/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A filtered read whose filter column is not projected holds no more of the file than its
/// look-ahead while its consumer is behind, where statistics prove the filter for whole row
/// groups, so that the filter column reads none of their pages (#1814).
///
/// The read runs in a child JVM of 192 MiB, which exits on an `OutOfMemoryError`, over 60 row
/// groups of 100,000 rows: a key and eight `INT64` columns. The filter drops the first row group
/// and statistics prove it for every other, whose eight columns are 378 MB once decoded. The
/// consumer holds after its first batch, as one slower than the decodes would. A read whose
/// look-ahead is bounded passes however long the consumer holds; the hold only gives a read
/// whose look-ahead runs away the time to decode the rest of the file.
class FilteredReadHeapTest {

    private static final int ROW_GROUPS = 60;
    private static final int ROWS_PER_GROUP = 100_000;
    private static final int COLUMNS = 8;
    /// the filter's lower bound, the second row group's first key: statistics drop the first row
    /// group and prove every later one
    private static final long FROM_KEY = ROWS_PER_GROUP;
    private static final long ROWS = (long) ROW_GROUPS * ROWS_PER_GROUP - FROM_KEY;

    @Test
    void consumerBehindTheDecodesHoldsNoMoreThanTheLookAhead(@TempDir Path workDir) throws Exception {
        Path file = workDir.resolve("filtered.parquet");
        write(file);
        Path stdout = workDir.resolve("stdout.txt");
        Path stderr = workDir.resolve("stderr.txt");
        Process process = new ProcessBuilder(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx192m",
                "-XX:+ExitOnOutOfMemoryError",
                "-cp", System.getProperty("java.class.path"),
                Read.class.getName(), file.toString()))
                .redirectOutput(stdout.toFile())
                .redirectError(stderr.toFile())
                .start();
        boolean exited = process.waitFor(1, TimeUnit.MINUTES);
        if (!exited) {
            process.destroyForcibly().waitFor();
        }
        String output = Files.readString(stdout) + Files.readString(stderr);

        assertThat(exited).as("the child JVM exited within a minute; its output:%n%s", output).isTrue();
        assertThat(process.exitValue()).as("the child JVM's exit status (3 on OutOfMemoryError); its output:%n%s", output)
                .isZero();
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

    /// Reads the file's eight `INT64` columns where the key is at least [#FROM_KEY], holding after the
    /// first batch, and exits with 0 where it read every matching row, 1 otherwise.
    public static final class Read {

        /// how long the consumer holds after its first batch
        private static final long HOLD_MILLIS = 2_000;

        public static void main(String[] args) throws Exception {
            String[] columns = new String[COLUMNS];
            for (int c = 0; c < COLUMNS; c++) {
                columns[c] = "c" + c;
            }
            long rows = 0;
            try (HardwoodContext context = HardwoodContext.create();
                    ParquetFileReader reader = ParquetFileReader.open(InputFile.of(Path.of(args[0])), context);
                    ColumnReaders readers = reader.buildColumnReaders(ColumnProjection.columns(columns))
                            .filter(FilterPredicate.gtEq("key", FROM_KEY))
                            .build()) {
                while (readers.nextBatch()) {
                    if (rows == 0) {
                        Thread.sleep(HOLD_MILLIS);
                    }
                    rows += readers.getRecordCount();
                }
            }
            System.exit(rows == ROWS ? 0 : 1);
        }
    }
}
