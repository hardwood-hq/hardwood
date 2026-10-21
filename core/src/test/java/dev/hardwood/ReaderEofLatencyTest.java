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
/// earlier. That one covers teardown: a fetch stopped for room in its column's queue ends only if
/// the close queues it a step. This covers the read itself: the *consumer* waiting inside
/// [dev.hardwood.internal.reader.BatchExchange#poll()] cannot see the end-of-stream flag, and
/// learns of the end only from the sentinel the column's end hands its queue. Teardown strands a
/// producer whose consumer has stopped consuming; this strands a consumer whose producer has
/// stopped producing, which is the ordinary path rather than the abandoned one.
///
/// The consumer's wait is untimed, so an end that never reaches it leaves the last `nextBatch()`
/// waiting forever, which the `@Timeout` catches. An end that reaches it only late, through some
/// later event rather than the column's end itself, shows as a long wait in the exchange, which
/// the assertion catches.
///
/// The assertion is on `dev.hardwood.BatchWait`, which records exactly how long a consumer sat in
/// the exchange, rather than on how long the read took end to end. Wall clock sums the wait with
/// the work around it, which is whatever the machine is worth, and on a two-core runner the work
/// alone approaches a bound tight enough to catch the wait. The recorded wait is the property, so
/// it is what is measured.
///
/// Each column is opened through its own [ParquetFileReader#columnReader(String)] and drained
/// before the next is opened, which is what makes the stalls add rather than overlap: a
/// projection read through one [dev.hardwood.reader.ColumnReaders] runs its columns concurrently,
/// so all but the first find their streams already ended and pay nothing.
///
/// Only the waits after a column's first batch count. The wait for the first batch is the
/// pipeline starting up, which the end of the stream has no part in, and on a loaded machine it
/// alone can reach the threshold.
class ReaderEofLatencyTest extends AbstractJfrRecorderTest {

    private static final String BATCH_WAIT_EVENT = "dev.hardwood.BatchWait";

    private static final String ROW_GROUP_SCANNED_EVENT = "dev.hardwood.RowGroupScanned";

    /// Wide enough that a late end in every column is unmistakable against a slow column or two.
    private static final int COLUMNS = 16;

    /// Few enough rows that a column is one batch, which is the state this is about: the consumer
    /// takes that batch, asks again, and reaches the exchange before the column has ended its
    /// stream. A file large enough to need many batches gives the column time to end the stream
    /// while the consumer is still working through them, and the last ask finds the flag already
    /// set — the same read, none of the wait.
    private static final int ROWS = 1_000;

    /// What counts as a late end rather than merely a wait. A consumer that outruns a busy
    /// producer waits too, and on a loaded machine it waits longer, so the question is not
    /// whether any wait happened but whether it lasted far beyond decoding a one-batch column.
    private static final Duration LATE_END = Duration.ofMillis(5L);

    /// How many columns may have waited that long. A late end costs every column a wait, so any
    /// small number separates it from a machine that was merely slow for a column or two, which
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
            readEveryColumnToTheEnd(hardwood, file, new ArrayList<>());

            Instant measuredFrom = Instant.now();
            List<Instant[]> pastFirstBatch = new ArrayList<>();
            readEveryColumnToTheEnd(hardwood, file, pastFirstBatch);

            awaitEvents();

            // Presence, not a count: the scan events are committed on the pool's threads, and one
            // of them can miss this recording's flush (see AbstractJfrRecorderTest).
            assertThat(eventsSince(ROW_GROUP_SCANNED_EVENT, measuredFrom).count())
                    .as("the recording saw the measured pass, so an empty stall total means "
                            + "no stalls rather than no recording")
                    .isPositive();

            long stalledColumns = eventsSince(BATCH_WAIT_EVENT, measuredFrom)
                    .filter(event -> startsWithin(event, pastFirstBatch))
                    .map(RecordedEvent::getDuration)
                    .filter(waited -> waited.compareTo(LATE_END) >= 0)
                    .count();

            assertThat(stalledColumns)
                    .as("of %d columns read to the end, how many waited at least %d ms in the "
                            + "exchange", COLUMNS, LATE_END.toMillis())
                    .isLessThan(TOLERATED_STALLED_COLUMNS);
        }
    }

    /// Whether `event` starts within one of `windows`, each the time from a column's first batch
    /// to its close.
    private static boolean startsWithin(RecordedEvent event, List<Instant[]> windows) {
        Instant start = event.getStartTime();
        for (Instant[] window : windows) {
            if (!start.isBefore(window[0]) && !start.isAfter(window[1])) {
                return true;
            }
        }
        return false;
    }

    private Stream<RecordedEvent> eventsSince(String eventName, Instant from) {
        return events(eventName).filter(event -> !event.getStartTime().isBefore(from));
    }

    /// Opens each column in turn and drains it past its last batch, because the `nextBatch()`
    /// that returns `false` is the one this is about. Adds to `pastFirstBatch`, per column, the
    /// time from its first batch's arrival to its close.
    private static void readEveryColumnToTheEnd(Hardwood hardwood, byte[] file, List<Instant[]> pastFirstBatch)
            throws Exception {
        try (ParquetFileReader parquet = hardwood.open(InputFile.of(ByteBuffer.wrap(file)))) {
            for (String column : columnNames()) {
                int batches = 0;
                Instant firstBatch = null;
                try (ColumnReader reader = parquet.columnReader(column)) {
                    while (reader.nextBatch()) {
                        if (batches++ == 0) {
                            firstBatch = Instant.now();
                        }
                    }
                }
                assertThat(batches).as("batches read from %s", column).isPositive();
                pastFirstBatch.add(new Instant[]{ firstBatch, Instant.now() });
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
