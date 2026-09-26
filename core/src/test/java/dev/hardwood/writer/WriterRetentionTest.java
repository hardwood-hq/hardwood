/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.sun.management.ThreadMXBean;

import dev.hardwood.InputFile;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// What the writer holds while a row group is open, against the target that is supposed to bound
/// it.
///
/// `rowGroupBufferTargetBytes` is the writer's only memory bound: a row group's column chunks have to
/// be encoded before any of their metadata is known, so the values stay resident until the group
/// flushes. The target is compared against the writer's account of what it retains (the value
/// store, an `int` index per interned value, the dictionary's entries and table, the level
/// streams), but every store also holds growth headroom beyond what it is charged for, so the
/// true peak sits above the target.
///
/// How far above is what this pins. It is deliberately loose: the number is a tripwire for a
/// structure being retained that was not before, not a specification of the writer's footprint,
/// and a bound tight enough to be exact would fail on GC timing rather than on a regression.
///
/// The second case covers what the writer holds before a row group is open at all. A schema's
/// width is known at creation and its data is not, so the buffers a column starts with are the
/// one part of the writer's footprint no target can bound after the fact.
class WriterRetentionTest {

    /// Columns in the wide-schema case. Wide enough that what the columns hold is several times
    /// the noise of a used-heap reading — at four columns the correct footprint is *below* that
    /// noise and the sample comes back negative — and narrow enough that a regression to
    /// page-capacity sizing fails the assertion on a normal heap rather than exhausting it.
    private static final int WIDE_SCHEMA_COLUMNS = 32;

    /// What one column may hold before it has been given a value.
    ///
    /// A column starts with the buffers it reads and accumulates through — a value window, a
    /// value store, a level store — and each of them grows with the data. What none of them may
    /// do is start at a size derived from the page target: page capacity scales inversely with a
    /// column's width, so sizing a `BOOLEAN` column's buffers from it reserves tens of megabytes
    /// per column for a file that may hold four rows. 64 KiB leaves room for a windowed read of
    /// a few thousand values and fails on a buffer sized by the page target.
    private static final long ALLOWED_BYTES_PER_IDLE_COLUMN = 64 << 10;

    /// What a column allocates at the least, so the ceiling above cannot be met by measuring
    /// nothing. A column that has read no value still allocates the window it will read through,
    /// and a reading below this would mean the measurement itself had stopped working.
    private static final long EXPECTED_BYTES_PER_IDLE_COLUMN = 1 << 10;

    /// Small enough to keep the test quick and to fit any CI heap, large enough that the measured
    /// retention is well above the noise of a settled JVM.
    private static final long ROW_GROUP_TARGET = 8L << 20;

    /// How many times the target the writer may retain before this fails.
    ///
    /// The measurement was 1.25× when this was written — about 10 MiB held against an 8 MiB
    /// target — so three leaves room for GC timing and for the allocator's own slack while still
    /// failing on a regression that doubles what a chunk keeps.
    private static final int ALLOWED_MULTIPLE = 3;

    /// Enough distinct values to be worth a dictionary and few enough to keep the dictionary
    /// itself negligible, so what is measured is the per-value cost rather than the table.
    private static final int DISTINCT = 1_000;

    private static final int VALUES_PER_BATCH = 8_192;

    @Test
    void anOpenRowGroupRetainsABoundedMultipleOfItsTarget() throws Exception {
        long ceiling = ALLOWED_MULTIPLE * ROW_GROUP_TARGET;
        assumeTrue(Runtime.getRuntime().maxMemory() > 2 * ceiling,
                "needs a heap that can hold the measurement with room to spare");

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder()
                .rowGroupBufferTargetBytes(ROW_GROUP_TARGET)
                // What the byte target holds is the subject, so the row target is put out of the
                // way rather than left to cut the group first.
                .rowGroupTargetRows(Long.MAX_VALUE)
                .build();

        // One batch short of the target, so nothing has flushed and what is resident at the end
        // is the peak rather than whatever survived a flush.
        int batches = (int) (ROW_GROUP_TARGET / Integer.BYTES / VALUES_PER_BATCH) - 1;
        int[] values = new int[VALUES_PER_BATCH];

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        long retained;
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            ColumnWriter columns = writer.columnWriter();
            long baseline = usedHeap();
            for (int b = 0; b < batches; b++) {
                for (int i = 0; i < VALUES_PER_BATCH; i++) {
                    values[i] = (b * VALUES_PER_BATCH + i) % DISTINCT;
                }
                columns.writeBatch(batch -> batch.ints("v", values));
            }
            retained = usedHeap() - baseline;
        }

        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(reader.getFileMetaData().rowGroups())
                    .as("the run stayed inside one row group, so the sample was its peak")
                    .hasSize(1);
        }

        // A floor as well as a ceiling. A measurement that collapsed — an explicit collection
        // disabled, a collector whose used-heap reading works differently, a refactor that moved
        // the retention off the heap being sampled — would otherwise pass this test while
        // measuring nothing, and a disarmed tripwire reads exactly like a held one.
        assertThat(retained)
                .as("bytes retained by an open row group against a %d MiB target", ROW_GROUP_TARGET >> 20)
                .isGreaterThan(ROW_GROUP_TARGET / 2)
                .isLessThan(ceiling);
    }

    /// What the writer reports it is holding, against what the heap says it is holding.
    ///
    /// `rowGroupBufferTargetBytes` is compared against [ParquetFileWriter#retainedBytes], so that
    /// number is the writer's memory bound and nothing else checks that it corresponds to memory.
    /// A term left out of it — a level stream, a dictionary's table, a store that grew — would
    /// make the writer hold more than a caller asked for while reporting that it had not.
    ///
    /// The two are held within a factor rather than to the byte: the report charges what the chunk
    /// holds, the heap carries what its buffers have grown to, and the gap between them is the
    /// growth factor plus the read windows and the sources the batch loop is holding.
    @Test
    void theReportedRetentionIsTheRetention() throws Exception {
        long ceiling = ALLOWED_MULTIPLE * ROW_GROUP_TARGET;
        assumeTrue(Runtime.getRuntime().maxMemory() > 2 * ceiling,
                "needs a heap that can hold the measurement with room to spare");

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder()
                .rowGroupBufferTargetBytes(ROW_GROUP_TARGET)
                .rowGroupTargetRows(Long.MAX_VALUE)
                .build();

        int batches = (int) (ROW_GROUP_TARGET / Integer.BYTES / VALUES_PER_BATCH) - 1;
        int[] values = new int[VALUES_PER_BATCH];

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            ColumnWriter columns = writer.columnWriter();
            long baseline = usedHeap();
            for (int b = 0; b < batches; b++) {
                for (int i = 0; i < VALUES_PER_BATCH; i++) {
                    values[i] = (b * VALUES_PER_BATCH + i) % DISTINCT;
                }
                columns.writeBatch(batch -> batch.ints("v", values));
            }
            long measured = usedHeap() - baseline;
            long reported = writer.retainedBytes();

            assertThat(reported)
                    .as("the writer reported holding %,d bytes where the heap grew by %,d",
                            reported, measured)
                    .isGreaterThan(measured / ALLOWED_MULTIPLE)
                    .isLessThanOrEqualTo(measured);
        }
    }

    /// The physical types the writer produces. `INT96` is not one of them.
    static Stream<PhysicalType> writableTypes() {
        return Stream.of(PhysicalType.values()).filter(t -> t != PhysicalType.INT96);
    }

    /// Every physical type the writer produces, because what a column holds before a record
    /// arrives differs by type and the cheapest one proves nothing.
    ///
    /// A `BOOLEAN` column has no dictionary index array and stores a bit a value, so it is the
    /// *least* a column can hold; a schema of them passes a ceiling that every other type
    /// exceeds. The types that carry an index per value and a wider store are the ones a bound
    /// on eager allocation has to hold, so the sweep is the test and `BOOLEAN` is one row of it.
    ///
    /// Counted as bytes allocated rather than bytes resident. Creating a writer allocates its
    /// buffers and keeps them, so on this one path the two measure the same thing — and the
    /// allocation counter reads the same whatever the collector has just done, where a used-heap
    /// reading of a footprint this small is dominated by whichever test ran before it.
    @ParameterizedTest(name = "{0}")
    @MethodSource("writableTypes")
    void aColumnHoldsBoundedBuffersBeforeAnyDataArrives(PhysicalType type) throws Exception {
        FileSchema.Builder schema = FileSchema.builder("schema");
        for (int c = 0; c < WIDE_SCHEMA_COLUMNS; c++) {
            if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
                schema.addColumn("c" + c, type, RepetitionType.REQUIRED, 8);
            }
            else {
                schema.addColumn("c" + c, type, RepetitionType.REQUIRED);
            }
        }
        WriterConfig config = WriterConfig.builder()
                .rowGroupBufferTargetBytes(1L << 20)
                .build();
        FileSchema built = schema.build();

        // Once through first: the classes a writer loads on its way up allocate too, and they
        // load once per JVM rather than once per writer.
        ParquetFileWriter.create(new ByteBufferOutputFile(), built, config).close();

        long baseline = allocatedBytes();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(),
                built, config)) {
            long allocated = allocatedBytes() - baseline;
            assertThat(allocated / WIDE_SCHEMA_COLUMNS)
                    .as("bytes allocated per %s column by a writer that has not been given a row", type)
                    .isGreaterThan(EXPECTED_BYTES_PER_IDLE_COLUMN)
                    .isLessThan(ALLOWED_BYTES_PER_IDLE_COLUMN);
            // The writer has to survive the measurement, or what was sampled is a collected
            // writer's buffers rather than a live one's.
            assertThat(writer).isNotNull();
        }
    }

    /// Bytes this thread has allocated since it started. Exact, monotonic, and independent of
    /// when the collector runs.
    private static long allocatedBytes() {
        assumeTrue(ManagementFactory.getThreadMXBean() instanceof ThreadMXBean,
                "needs the HotSpot thread allocation counter");
        return ((ThreadMXBean) ManagementFactory.getThreadMXBean())
                .getCurrentThreadAllocatedBytes();
    }

    /// Live bytes after a collection. Three passes because the first frees the garbage of the
    /// last batch and the ones after it settle what that freeing itself produced; without them
    /// the reading carries whatever the allocator had not yet reclaimed.
    private static long usedHeap() {
        for (int pass = 0; pass < 3; pass++) {
            System.gc();
        }
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
