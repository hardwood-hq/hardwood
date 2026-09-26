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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// What tearing a reader down costs, as a function of how many columns are projected —
/// asked of both consumer shapes, [ColumnReaders#close()] and [RowReader#close()].
///
/// Teardown is not on the hot path of a long scan, so it is easy for it to acquire a cost
/// nobody notices. It is on the hot path of a *short* read — one batch, a `head(n)`, a point
/// lookup — where the read itself is single-digit milliseconds and anything teardown adds is
/// the dominant term.
///
/// The property under test is that a close costs the same whether one column is projected or
/// sixteen. It fails as soon as a worker's stop signal does not reach its drain thread
/// directly, because [dev.hardwood.internal.reader.ColumnWorker#close()] joins that thread and
/// [ColumnReaders#close()] closes the columns one at a time: a per-column delay of any kind
/// adds up rather than overlapping, and the total goes linear in the projection width.
///
/// Both consumer shapes have to be covered, because the two `BatchExchange` modes block their
/// drain thread at *different* points and so are separately breakable. [ColumnReaders] uses
/// the detaching mode, whose drain blocks in `publish()` on a full ready queue; [RowReader]
/// uses the recycling mode, whose drain blocks in `takeBatch()` on an empty free pool. A
/// teardown that releases only one of the two looks fixed from one shape and is still broken
/// from the other.
///
/// On the bound: sub-millisecond closes are typical, but the distribution has a heavy tail,
/// and under 8x CPU oversubscription maxima of 27 / 61 / 67 / 101 ms have been observed across
/// four runs — a badly loaded machine can fail this test. The bound is deliberately *not*
/// raised to paper that over: the regression's floor at this width is about 150 ms, and every
/// millimetre the bound moves toward it costs discriminating power. 60 ms keeps a real gap on
/// both sides; a failure here on a saturated CI box is a false positive worth re-running
/// rather than a reason to widen it.
class ReaderCloseLatencyTest {

    /// Wide enough that one per-column stall is unmistakable against the bound, and still a
    /// projection a caller might plausibly ask for.
    ///
    /// Also wide enough for the row-reader case to reproduce at all. Batch size there is
    /// derived from the projection (see [dev.hardwood.internal.reader.BatchSizing]) rather
    /// than set by the caller, so a narrow projection gets batches big enough to swallow the
    /// whole file, every drain reaches EOF and finishes itself, and nothing is left parked for
    /// close() to release. The effect appears from about 8 columns upward; 16 is comfortably
    /// inside that.
    private static final int COLUMNS = 16;

    /// Many more rows than the one batch the test consumes. This is the state the property
    /// needs: with data still to come, each column's drain thread has filled the two-deep ready
    /// queue and is blocked — trying to publish a third batch on the column path, or waiting
    /// for a recycled holder on the row path. A file consumed entirely by the first batch does
    /// not exercise it — the drain reaches EOF and exits on its own before close() is called.
    private static final int ROWS = 200_000;

    /// Forces many batches per column, so one batch is a small fraction of the file.
    /// Applies to the column path only; the row path has no caller-set batch size.
    private static final int BATCH_SIZE = 1_024;

    /// How many rows the row-reader case consumes before closing. Enough to have started the
    /// pipeline, far short of the file.
    private static final int ROWS_READ = 10;

    /// The per-column cost a teardown that fails to signal its drain directly settles at —
    /// 10 ms was what the timed `BatchExchange` waits this test was written against charged.
    /// Used only to state the expected magnitude in the assertion messages.
    private static final Duration PER_COLUMN_STALL_DURATION = Duration.ofMillis(10L);

    /// Passes comfortably when no close stalls, and cannot pass when they all do: a stalling
    /// close needs about `(COLUMNS - 1) * 10 ms` = 150 ms. See the class comment for the
    /// actual margin and its tail.
    private static final Duration BOUND = Duration.ofMillis(60L);

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void closeDoesNotScaleWithProjectionWidth() throws Exception {
        byte[] file = writeSyntheticFile();
        String[] columns = columnNames().toArray(new String[0]);

        try (Hardwood hardwood = Hardwood.create()) {
            // A first read pays the one-off costs — class loading, the executor's threads, the
            // footer parse — so they are not attributed to the close being measured.
            timeCloseAfterOneBatch(hardwood, file, columns);

            Duration elapsed = timeCloseAfterOneBatch(hardwood, file, columns);

            assertThat(elapsed)
                    .as("close() of a %d-column projection; a %d ms stall per column "
                            + "would be about %d ms",
                            COLUMNS, PER_COLUMN_STALL_DURATION.toMillis(),
                            stallAcrossColumns().toMillis())
                    .isLessThan(BOUND);
        }
    }

    /// The same question asked of the filtered path.
    ///
    /// Both paths close through the same `ColumnScan`, which on the filtered path closes the
    /// *augmented* set of columns: the payload columns plus the predicate columns that never
    /// surface to the caller. This covers that wider set.
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void filteredCloseDoesNotScaleWithProjectionWidth() throws Exception {
        byte[] file = writeSyntheticFile();
        String[] columns = columnNames().toArray(new String[0]);

        try (Hardwood hardwood = Hardwood.create()) {
            timeFilteredCloseAfterOneBatch(hardwood, file, columns);

            Duration elapsed = timeFilteredCloseAfterOneBatch(hardwood, file, columns);

            assertThat(elapsed)
                    .as("close() of a filtered %d-column projection", COLUMNS)
                    .isLessThan(BOUND);
        }
    }

    /// And of the row-reader path, which is the *other* exchange mode.
    ///
    /// [RowReader] recycles its batch holders, so its drain blocks in `takeBatch()` waiting for
    /// the consumer to hand a holder back, not in `publish()` waiting for ready-queue room.
    /// Releasing only the publish side leaves this path stalling per column, which is exactly
    /// the regression the two cases above cannot see.
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void rowReaderCloseDoesNotScaleWithProjectionWidth() throws Exception {
        byte[] file = writeSyntheticFile();
        String[] columns = columnNames().toArray(new String[0]);

        try (Hardwood hardwood = Hardwood.create()) {
            timeRowReaderCloseAfterFewRows(hardwood, file, columns);

            Duration elapsed = timeRowReaderCloseAfterFewRows(hardwood, file, columns);

            assertThat(elapsed)
                    .as("RowReader.close() over a %d-column projection after reading %d rows; "
                            + "a %d ms stall per column would be about %d ms",
                            COLUMNS, ROWS_READ, PER_COLUMN_STALL_DURATION.toMillis(),
                            stallAcrossColumns().toMillis())
                    .isLessThan(BOUND);
        }
    }

    /// Opens a row reader over the full projection, reads a handful of rows, and returns how
    /// long the close took.
    ///
    /// The rows matter for the same reason the single batch does on the column path: they are
    /// what gets the drain threads running and their exchanges occupied. Reading only a
    /// handful — rather than draining — is what leaves them parked mid-file when close()
    /// arrives.
    private static Duration timeRowReaderCloseAfterFewRows(
            Hardwood hardwood, byte[] file, String[] columns) throws Exception {
        try (ParquetFileReader parquet = hardwood.open(InputFile.of(ByteBuffer.wrap(file)))) {
            RowReader rows = parquet.buildRowReader()
                    .projection(ColumnProjection.columns(columns))
                    .build();
            for (int i = 0; i < ROWS_READ; i++) {
                assertThat(rows.hasNext()).as("row %d of %d", i, ROWS_READ).isTrue();
                rows.next();
            }

            long start = System.nanoTime();
            rows.close();
            return Duration.ofNanos(System.nanoTime() - start);
        }
    }

    /// A predicate that keeps essentially every row, so the readers still have work outstanding
    /// when close() arrives — the same precondition the unfiltered case relies on.
    private static Duration timeFilteredCloseAfterOneBatch(
            Hardwood hardwood, byte[] file, String[] columns) throws Exception {
        try (ParquetFileReader parquet = hardwood.open(InputFile.of(ByteBuffer.wrap(file)))) {
            ColumnReaders readers = parquet.buildColumnReaders(ColumnProjection.columns(columns))
                    .filter(FilterPredicate.gt("c0", -1L))
                    .batchSize(BATCH_SIZE)
                    .build();
            assertThat(readers.nextBatch()).as("first filtered batch").isTrue();

            long start = System.nanoTime();
            readers.close();
            return Duration.ofNanos(System.nanoTime() - start);
        }
    }

    /// Opens the projection, consumes exactly one batch, and returns how long the close took.
    ///
    /// The batch matters: it is what gets the drain threads running and the ready queues
    /// occupied, which is the state a close has to unwind. Closing a reader that never read
    /// does not reach the path this is about.
    private static Duration timeCloseAfterOneBatch(Hardwood hardwood, byte[] file, String[] columns)
            throws Exception {
        try (ParquetFileReader parquet = hardwood.open(InputFile.of(ByteBuffer.wrap(file)))) {
            ColumnReaders readers = parquet.buildColumnReaders(ColumnProjection.columns(columns))
                    .batchSize(BATCH_SIZE)
                    .build();
            assertThat(readers.nextBatch()).as("first batch").isTrue();

            long start = System.nanoTime();
            readers.close();
            return Duration.ofNanos(System.nanoTime() - start);
        }
    }

    /// What a teardown that stalls once per column would cost in total, for the assertion
    /// messages. The first column's stall overlaps nothing, so it is `COLUMNS - 1` of them.
    private static Duration stallAcrossColumns() {
        return PER_COLUMN_STALL_DURATION.multipliedBy(COLUMNS - 1L);
    }

    private static List<String> columnNames() {
        List<String> names = new ArrayList<>(COLUMNS);
        for (int i = 0; i < COLUMNS; i++) {
            names.add("c" + i);
        }
        return names;
    }

    /// A synthetic file of `COLUMNS` required `INT64` columns and `ROWS` rows. Nothing about
    /// the values matters, only that every projected column has a chunk to decode.
    private static byte[] writeSyntheticFile() throws Exception {
        FileSchema.Builder schema = FileSchema.builder("close_scaling");
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
