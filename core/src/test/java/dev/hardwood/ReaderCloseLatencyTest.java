/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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
/// sixteen. [dev.hardwood.internal.reader.ScheduledColumn#close()] waits for its column's fetch
/// steps and page tasks, and [ColumnReaders#close()] closes the columns one at a time. The first
/// close stops every column of the read, so the columns' waits overlap. The waits are untimed, so
/// a column whose stop does not end its fetch, or whose last task does not wake the close,
/// leaves the close waiting forever, which the
/// `@Timeout` catches. A column stopped only when its own close is reached waits for its page
/// tasks in turn rather than alongside the others, and the total goes linear in the projection
/// width, which the bound catches.
///
/// When close() arrives mid-file, each column's ready queue is at its limit, so its completed
/// cells wait for the consumer to publish them, the scheduler submits none of its pages past the
/// read-ahead cap, and its fetch has stopped for room in the column's page queue. The close has
/// to end that fetch, and its wait for the tasks has to end as the last task finishes.
/// The cases differ in where the pipeline comes to rest: [ColumnReaders] uses the detaching
/// `BatchExchange` mode, whose ready queue is limited in rows, and [RowReader] the recycling mode,
/// whose ready queue is limited in batch holders; a flat column places its pages on its fetch
/// steps ([dev.hardwood.internal.reader.ScheduledFlatColumn]), and a struct field's column places
/// them on its page tasks and also stops its fetch for room among its pages not yet placed
/// ([dev.hardwood.internal.reader.ScheduledNestedColumn]).
///
/// On the bound: sub-millisecond closes are typical, but the distribution has a heavy tail,
/// and under 8x CPU oversubscription maxima of 27 / 61 / 67 / 101 ms have been observed across
/// four runs — a badly loaded machine can fail this test. A failure here on a saturated CI box
/// is a false positive worth re-running rather than a reason to widen the bound.
class ReaderCloseLatencyTest {

    /// Wide enough that per-column waits adding up show against the bound, and still a
    /// projection a caller might plausibly ask for.
    ///
    /// Also wide enough for the row-reader case to reproduce at all. Batch size there is
    /// derived from the projection (see [dev.hardwood.internal.reader.BatchSizing]) rather
    /// than set by the caller, so a narrow projection gets batches big enough to swallow the
    /// whole file, every column reaches EOF and finishes itself, and no fetch is left stopped for
    /// close() to end. The effect appears from about 8 columns upward; 16 is comfortably
    /// inside that.
    private static final int COLUMNS = 16;

    /// Many more rows than the one batch the test consumes. This is the state the property
    /// needs: with data still to come, each column has filled its ready queue and its fetch has
    /// stopped for room in its page queue. A file consumed entirely by the first batch does
    /// not exercise it — the fetch reaches EOF and ends on its own before close() is called.
    private static final int ROWS = 200_000;

    /// Forces many batches per column, so one batch is a small fraction of the file.
    /// Applies to the column path only; the row path has no caller-set batch size.
    private static final int BATCH_SIZE = 1_024;

    /// How many rows the row-reader case consumes before closing. Enough to have started the
    /// pipeline, far short of the file.
    private static final int ROWS_READ = 10;

    /// The struct of the nested row-reader case, whose fields are named like the flat columns.
    private static final String STRUCT = "s";

    /// Passes comfortably when the columns' waits overlap. See the class comment for its tail.
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
                    .as("close() of a %d-column projection", COLUMNS)
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
    /// [RowReader] recycles its batch holders, so its ready queue is limited in holders rather
    /// than rows, and the pipeline comes to rest at another point of the file than on the column
    /// path. Its file's columns are flat `BYTE_ARRAY`, whose segments copy their bytes into chunks
    /// of their own.
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void rowReaderCloseDoesNotScaleWithProjectionWidth() throws Exception {
        byte[] file = writeSyntheticBinaryFile();
        String[] columns = columnNames().toArray(new String[0]);

        try (Hardwood hardwood = Hardwood.create()) {
            timeRowReaderCloseAfterFewRows(hardwood, file, columns);

            Duration elapsed = timeRowReaderCloseAfterFewRows(hardwood, file, columns);

            assertThat(elapsed)
                    .as("RowReader.close() over a %d-column projection after reading %d rows",
                            COLUMNS, ROWS_READ)
                    .isLessThan(BOUND);
        }
    }

    /// And of the nested row reader, which recycles its batch holders too and whose struct fields
    /// are [dev.hardwood.internal.reader.ScheduledNestedColumn]s: they place their pages on their
    /// page tasks, and their fetches also stop for room among the pages not yet placed.
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void nestedRowReaderCloseDoesNotScaleWithProjectionWidth() throws Exception {
        byte[] file = writeSyntheticStructFile();
        String[] columns = { STRUCT };

        try (Hardwood hardwood = Hardwood.create()) {
            timeRowReaderCloseAfterFewRows(hardwood, file, columns);

            Duration elapsed = timeRowReaderCloseAfterFewRows(hardwood, file, columns);

            assertThat(elapsed)
                    .as("RowReader.close() over a struct of %d fields after reading %d rows",
                            COLUMNS, ROWS_READ)
                    .isLessThan(BOUND);
        }
    }

    /// Opens a row reader over the full projection, reads a handful of rows, and returns how
    /// long the close took.
    ///
    /// The rows matter for the same reason the single batch does on the column path: they are
    /// what gets the fetchers running and their exchanges occupied. Reading only a
    /// handful — rather than the whole file — is what leaves them parked mid-file when close()
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
    /// The batch matters: it is what gets the fetchers running and the ready queues
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

    private static List<String> columnNames() {
        List<String> names = new ArrayList<>(COLUMNS);
        for (int i = 0; i < COLUMNS; i++) {
            names.add("c" + i);
        }
        return names;
    }

    /// A synthetic file of `COLUMNS` required `INT64` columns and `ROWS` rows. Nothing about
    /// the values matters, only that every projected column has a chunk to decode. The read's
    /// scheduler decodes these columns ([dev.hardwood.internal.reader.ReadScheduler]).
    private static byte[] writeSyntheticFile() throws Exception {
        FileSchema.Builder schema = FileSchema.builder("close_scaling");
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

    /// The same shape with required `BYTE_ARRAY` columns.
    private static byte[] writeSyntheticBinaryFile() throws Exception {
        FileSchema.Builder schema = FileSchema.builder("close_scaling_binary");
        for (String name : columnNames()) {
            schema.addColumn(name, PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED);
        }

        byte[][] values = new byte[ROWS][];
        for (int i = 0; i < ROWS; i++) {
            values[i] = Long.toString(i).getBytes(StandardCharsets.US_ASCII);
        }

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema.build())) {
            writer.columnWriter().writeBatch(batch -> {
                for (String name : columnNames()) {
                    batch.bytes(name, values);
                }
            });
        }
        return InMemoryFiles.toByteArray(out);
    }

    /// A struct of `COLUMNS` required `INT64` fields, which a row read decodes through
    /// [dev.hardwood.internal.reader.ScheduledNestedColumn]s.
    private static byte[] writeSyntheticStructFile() throws Exception {
        FileSchema schema = FileSchema.builder("close_scaling_struct")
                .struct(STRUCT, RepetitionType.REQUIRED, struct -> {
                    for (String name : columnNames()) {
                        struct.addColumn(name, PhysicalType.INT64, RepetitionType.REQUIRED);
                    }
                })
                .build();

        long[] values = new long[ROWS];
        for (int i = 0; i < ROWS; i++) {
            values[i] = i;
        }

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> {
                for (String name : columnNames()) {
                    batch.longs(STRUCT + "." + name, values);
                }
            });
        }
        return InMemoryFiles.toByteArray(out);
    }
}
