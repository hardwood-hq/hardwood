/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// The read's scheduler submits the pages of its columns in row order across the columns, never
/// below the lowest queued page and, on this read, whose read-ahead cap spans the file, never
/// past it, and keeps at most two pages per decode thread in flight. The columns' pages differ in
/// size: under one page byte target, the `INT64` column cuts its pages at about half the rows the
/// `INT32` columns do.
class ReadSchedulerTest {

    private static final int ROWS = 60_000;
    private static final int THREADS = 3;
    private static final int BATCH = 1_000;

    @AfterEach
    void removeObserver() {
        ReadScheduler.submissionObserver = null;
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void pagesAreSubmittedInRowOrder() throws Exception {
        List<long[]> submissions = Collections.synchronizedList(new ArrayList<>());
        // A sealed fill completes a cell its fetch step sealed, and can be queued ahead of the page
        // the scheduler chose; it holds no page of its own
        ReadScheduler.submissionObserver = (column, page, inFlight, lowestRow, readPublishedRow) -> {
            if (!(page instanceof SealedFillTask)) {
                submissions.add(new long[]{ page.firstRow, inFlight, lowestRow, readPublishedRow });
            }
        };

        Map<String, Long> sums = new HashMap<>();
        try (HardwoodContext context = HardwoodContext.create(THREADS);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file())), context);
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("longs", "ints", "small"))
                     .batchSize(BATCH)
                     .build()) {
            while (columns.nextBatch()) {
                for (String name : List.of("longs", "ints", "small")) {
                    ColumnReader column = columns.getColumnReader(name);
                    long sum = 0;
                    for (int i = 0; i < column.getRecordCount(); i++) {
                        sum += name.equals("longs") ? column.getLongs()[i] : column.getInts()[i];
                    }
                    sums.merge(name, sum, Long::sum);
                }
            }
        }

        long expected = (long) ROWS * (ROWS - 1) / 2;
        assertThat(sums).containsExactlyInAnyOrderEntriesOf(Map.of("longs", expected, "ints", expected,
                "small", expected));
        assertThat(submissions).hasSizeGreaterThan(10);
        long lastLowest = -1;
        for (int i = 0; i < submissions.size(); i++) {
            long[] submission = submissions.get(i);
            // The lowest queued page, with no column of an empty queue able to start lower: these
            // submissions follow row order
            assertThat(submission[0]).as("submission %d at row %d, not the lowest queued row %d", i,
                    submission[0], submission[2]).isEqualTo(submission[2]);
            assertThat(submission[0]).as("submission %d at row %d, after one at row %d", i, submission[0],
                    lastLowest).isGreaterThanOrEqualTo(lastLowest);
            lastLowest = submission[0];
        }
        for (long[] submission : submissions) {
            assertThat(submission[1]).isLessThanOrEqualTo(2L * THREADS);
        }
    }

    /// Three required columns holding `0..ROWS-1`, under a small page byte target.
    private static byte[] file() throws Exception {
        FileSchema schema = FileSchema.builder("paged")
                .addColumn("longs", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("ints", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("small", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        long[] longs = new long[ROWS];
        int[] ints = new int[ROWS];
        for (int i = 0; i < ROWS; i++) {
            longs[i] = i;
            ints[i] = i;
        }
        WriterConfig config = WriterConfig.builder().pageTargetBytes(16_384).build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> {
                batch.longs("longs", longs);
                batch.ints("ints", ints);
                batch.ints("small", ints);
            });
        }
        return InMemoryFiles.toByteArray(out);
    }
}
