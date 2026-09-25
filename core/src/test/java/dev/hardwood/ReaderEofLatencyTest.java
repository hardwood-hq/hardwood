/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.jfr.AbstractJfrRecorderTest;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import jdk.jfr.consumer.RecordedEvent;

import static org.assertj.core.api.Assertions.assertThat;

/// What a read that runs to completion spends waiting on the pipeline, as a function of how many
/// columns are read.
///
/// This is the read-path counterpart of [ReaderCloseLatencyTest], and the same defect one step
/// earlier. That one covers teardown: a drain blocked inside its exchange could not see the
/// `finished` flag until its own timed queue operation expired, so `close()` inherited the wait
/// once per column. This covers the read itself — the *consumer* waiting inside
/// [dev.hardwood.internal.reader.BatchExchange#poll()] could not see the flag either, so the last
/// `nextBatch()`, the one that reports the end, waited out the same window. Teardown strands a
/// producer whose consumer has stopped consuming; this strands a consumer whose producer has
/// stopped producing, which is the ordinary path rather than the abandoned one.
///
/// The assertion is on `dev.hardwood.BatchWait`, which records exactly how long a consumer sat in
/// the exchange, rather than on how long the read took end to end. Wall clock cannot separate the
/// two things it sums: a stall is a fixed 10 ms quantum on any machine, while the work around it
/// is whatever the machine is worth, and on a two-core runner the second term alone approached
/// what a bound tight enough to catch the first would allow. The recorded stall is the property,
/// so it is what is measured.
///
/// Each column is opened through its own [ParquetFileReader#columnReader(String)] and drained
/// before the next is opened, which is what makes the stalls add rather than overlap: a
/// projection read through one [dev.hardwood.reader.ColumnReaders] runs its columns concurrently,
/// so all but the first find their streams already ended and pay nothing.
class ReaderEofLatencyTest extends AbstractJfrRecorderTest {

    private static final String BATCH_WAIT_EVENT = "dev.hardwood.BatchWait";

    private static final String ROW_GROUP_SCANNED_EVENT = "dev.hardwood.RowGroupScanned";

    /// Wide enough that one stall per column is unmistakable against the bound.
    private static final int COLUMNS = 16;

    /// Few enough rows that a column is one batch, which is the state this is about: the consumer
    /// takes that batch, asks again, and reaches the exchange before the drain has finished
    /// itself. A file large enough to need many batches gives the drain time to end the stream
    /// while the consumer is still working through them, and the last ask finds the flag already
    /// set — the same read, none of the wait.
    private static final int ROWS = 1_000;

    /// What one column costs when the end of the stream reaches the consumer only through the
    /// expiry of its own poll. The exchange polls on a 10 ms window, so that is the quantum.
    private static final Duration PER_COLUMN_STALL = Duration.ofMillis(10L);

    /// What counts as having waited out the window rather than merely having waited. A consumer
    /// that outruns a busy producer waits too, and on a loaded machine it waits longer, so the
    /// question is not whether any wait happened but whether it was the poll interval expiring.
    private static final Duration WAITED_OUT_THE_WINDOW = PER_COLUMN_STALL.dividedBy(2);

    /// How many columns may have waited out the window. The defect gives every column one, so any
    /// small number separates it from a machine that was merely slow for a column or two — which
    /// is what makes this hold on a two-core runner without being tuned to one.
    private static final long TOLERATED_STALLED_COLUMNS = COLUMNS / 2;

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void readingToTheEndDoesNotStallOncePerColumn() throws Exception {
        byte[] file = writeSyntheticFile();

        try (Hardwood hardwood = Hardwood.create()) {
            // A first pass pays the one-off costs — class loading, the executor's threads, the
            // footer parse. Those can stall a consumer for real, so the pass is excluded rather
            // than merely untimed: the events are filtered by when the measured pass began.
            readEveryColumnToTheEnd(hardwood, file);

            Instant measuredFrom = Instant.now();
            readEveryColumnToTheEnd(hardwood, file);

            awaitEvents();

            // Presence, not a count: the scan events are committed on worker threads, and one
            // of them can miss this recording's flush (see AbstractJfrRecorderTest).
            assertThat(eventsSince(ROW_GROUP_SCANNED_EVENT, measuredFrom).count())
                    .as("the recording saw the measured pass, so an empty stall total means "
                            + "no stalls rather than no recording")
                    .isPositive();

            long stalledColumns = eventsSince(BATCH_WAIT_EVENT, measuredFrom)
                    .map(RecordedEvent::getDuration)
                    .filter(waited -> waited.compareTo(WAITED_OUT_THE_WINDOW) >= 0)
                    .count();

            assertThat(stalledColumns)
                    .as("of %d columns read to the end, how many waited at least %d ms in the "
                            + "exchange; an end that reaches the consumer only when its own poll "
                            + "expires costs every one of them the full %d ms window",
                            COLUMNS, WAITED_OUT_THE_WINDOW.toMillis(), PER_COLUMN_STALL.toMillis())
                    .isLessThan(TOLERATED_STALLED_COLUMNS);
        }
    }

    private Stream<RecordedEvent> eventsSince(String eventName, Instant from) {
        return events(eventName).filter(event -> !event.getStartTime().isBefore(from));
    }

    /// Opens each column in turn and drains it past its last batch, because the `nextBatch()`
    /// that returns `false` is the one this is about.
    private static void readEveryColumnToTheEnd(Hardwood hardwood, byte[] file) throws Exception {
        try (ParquetFileReader parquet = hardwood.open(InputFile.of(ByteBuffer.wrap(file)))) {
            for (String column : columnNames()) {
                int batches = 0;
                try (ColumnReader reader = parquet.columnReader(column)) {
                    while (reader.nextBatch()) {
                        batches++;
                    }
                }
                assertThat(batches).as("batches read from %s", column).isPositive();
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
        FileSchema.Builder schema = FileSchema.builder("eof_scaling");
        for (String name : columnNames()) {
            schema.addColumn(name, PhysicalType.INT64, RepetitionType.REQUIRED);
        }

        long[] values = new long[ROWS];
        for (int i = 0; i < ROWS; i++) {
            values[i] = i;
        }

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema.build())) {
            writer.columnWriter().writeBatch(batch -> {
                for (String name : columnNames()) {
                    batch.longs(name, values);
                }
            });
        }
        return out.toByteArray();
    }
}
