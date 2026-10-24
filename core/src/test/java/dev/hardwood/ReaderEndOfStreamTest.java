/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// A read that runs to completion learns of the end of every column's stream.
///
/// This is the read-path counterpart of [ReaderCloseMidReadTest]. That one covers teardown: a
/// fetch stopped for room in its column's queue ends only if the close queues it a step. This
/// covers the read itself: the *consumer* waiting inside
/// [dev.hardwood.internal.reader.BatchExchange#poll()] cannot see the end-of-stream flag, and
/// learns of the end only from the sentinel the column's end hands its queue. Teardown strands a
/// producer whose consumer has stopped consuming; this strands a consumer whose producer has
/// stopped producing, which is the ordinary path rather than the abandoned one.
///
/// The consumer's wait is untimed, so an end that never reaches it leaves the last `nextBatch()`
/// waiting forever, which the `@Timeout` catches.
///
/// Each column is opened through its own [ParquetFileReader#columnReader(String)] and drained
/// before the next is opened, so each column's consumer reaches the exchange on its own: a
/// projection read through one [dev.hardwood.reader.ColumnReaders] runs its columns concurrently,
/// so all but the first find their streams already ended.
class ReaderEndOfStreamTest {

    private static final int COLUMNS = 16;

    /// Few enough rows that a column is one batch, which is the state this is about: the consumer
    /// takes that batch, asks again, and reaches the exchange before the column has ended its
    /// stream. A file large enough to need many batches gives the column time to end the stream
    /// while the consumer is still working through them, and the last ask finds the flag already
    /// set.
    private static final int ROWS = 1_000;

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void readingEveryColumnToTheEndReturns() throws Exception {
        byte[] file = writeSyntheticFile();

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.open(InputFile.of(ByteBuffer.wrap(file)))) {
            for (String column : columnNames()) {
                long rows = 0;
                try (ColumnReader reader = parquet.columnReader(column)) {
                    while (reader.nextBatch()) {
                        rows += reader.getRecordCount();
                    }
                }
                assertThat(rows).as("rows read from %s", column).isEqualTo(ROWS);
            }
        }
    }

    private static List<String> columnNames() {
        List<String> names = new ArrayList<>(COLUMNS);
        for (int i = 0; i < COLUMNS; i++) {
            names.add("c" + i);
        }
        return names;
    }

    /// A synthetic file of `COLUMNS` required `INT64` columns and `ROWS` rows. Nothing about the
    /// values matters, only that every column has a chunk to decode.
    private static byte[] writeSyntheticFile() throws Exception {
        FileSchema.Builder schema = FileSchema.builder("end_of_stream");
        for (String name : columnNames()) {
            schema.addColumn(name, PhysicalType.INT64, RepetitionType.REQUIRED);
        }

        long[] values = new long[ROWS];
        for (int i = 0; i < ROWS; i++) {
            values[i] = i;
        }

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema.build())) {
            writer.columnWriter().writeBatch(batch -> {
                for (String name : columnNames()) {
                    batch.longs(name, values);
                }
            });
        }
        return InMemoryFiles.toByteArray(out);
    }
}
