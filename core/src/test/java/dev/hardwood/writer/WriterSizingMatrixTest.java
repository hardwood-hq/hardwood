/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.sun.management.ThreadMXBean;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.writer.RowGroupBuffer;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// The two sizing promises, over the shapes that break them.
///
/// A row group is cut on the bytes the writer holds for it, so two things follow that no single
/// fixture demonstrates: **a row group passes its byte target by at most one record**, a record
/// not being divisible across row groups, and **what the writer holds follows the target rather
/// than the data**. Both are properties of every schema and every arrival pattern, and the ways
/// they fail are not the ordinary cases — they are the record that widens part way through a
/// batch, the batch far larger than a whole row group, the column whose values are all distinct
/// and so retain a dictionary entry each, the schema wide enough that per-column overheads
/// dominate, and the value larger than the target itself.
///
/// So the cases are swept rather than chosen. Each states what one of its records can retain,
/// and the peak the writer reports is held to the target plus that — the tightest bound the
/// promise allows, so a case that overshoots by two records fails as loudly as one that
/// overshoots by a thousand.
///
/// [WriterRetentionTest] is what makes the reported peak worth asserting on: it holds the
/// writer's own measure against a measurement of the heap. This test then uses that measure
/// across a breadth of shapes that a heap measurement could not be run over.
class WriterSizingMatrixTest {

    /// One shape to write, and what one of its records may retain.
    ///
    /// @param name what the case is probing
    /// @param schema the schema to write
    /// @param rows how many records to write in total
    /// @param batchRows records per `writeBatch` call, which sets the batch-to-row-group ratio
    /// @param targetBytes the row-group byte target
    /// @param recordAllowanceBytes the most one record of this shape can retain, which is what a
    ///        row group may exceed its target by
    /// @param filler writes one batch's worth of values starting at a record offset
    private record Case(String name, FileSchema schema, int rows, int batchRows, long targetBytes,
            long recordAllowanceBytes, BatchFiller filler) {

        @Override
        public String toString() {
            return name;
        }
    }

    @FunctionalInterface
    private interface BatchFiller {
        void fill(ColumnBatch batch, int fromRecord, int count);
    }

    private static final int WIDE_COLUMNS = 200;

    /// A value wider than any target this test sets, so the row group holding it has no choice
    /// but to exceed what it was asked to hold.
    private static final int OVERSIZED_VALUE_BYTES = 512 * 1024;

    static Stream<Case> cases() {
        return Stream.of(
                flatLongs("flat INT64, all distinct, batch inside the row group", 40_000, 1024, 256 * 1024),
                flatLongs("flat INT64, batch far larger than the row group", 40_000, 40_000, 64 * 1024),
                flatLongs("flat INT64, tiny target", 20_000, 512, 4096),
                lowCardinalityLongs("flat INT64, few distinct values so a dictionary pays", 40_000, 1024, 128 * 1024),
                booleans("flat BOOLEAN, the narrowest column the format has", 200_000, 4096, 64 * 1024),
                widenedBinary("BYTE_ARRAY that widens part way through a batch", 6_000, 4096, 1024 * 1024),
                oversizedBinary("a single value larger than the whole target", 40, 8, 64 * 1024),
                nullableBinary("optional BYTE_ARRAY, half of it null", 20_000, 1024, 256 * 1024),
                lists("list<int32>, levels retained a byte an entry", 20_000, 1024, 256 * 1024),
                wideSchema("a schema wide enough that per-column overhead dominates", 2_000, 256, 4 * 1024 * 1024));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void aRowGroupPassesItsTargetByAtMostOneRecord(Case testCase) throws IOException {
        WriterConfig config = WriterConfig.builder()
                .rowGroupBufferTargetBytes(testCase.targetBytes())
                // The byte target is the subject, so the row target is put out of its way.
                .rowGroupTargetRows(Long.MAX_VALUE)
                .build();

        InMemoryOutputFile out = OutputFile.inMemory();
        long peak;
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, testCase.schema(), config)) {
            ColumnWriter columns = writer.columnWriter();
            for (int from = 0; from < testCase.rows(); from += testCase.batchRows()) {
                int count = Math.min(testCase.batchRows(), testCase.rows() - from);
                int start = from;
                columns.writeBatch(batch -> testCase.filler().fill(batch, start, count));
            }
            peak = writer.peakRetainedBytes();
        }

        assertThat(peak)
                .as("peak bytes held against a %,d-byte target, one record allowed to exceed it by %,d",
                        testCase.targetBytes(), testCase.recordAllowanceBytes())
                .isLessThanOrEqualTo(testCase.targetBytes() + testCase.recordAllowanceBytes());

        // A floor as well as a ceiling: a writer that flushed after every record would satisfy
        // the bound above while making the target meaningless, and this data is many times the
        // target in every case.
        assertThat(peak)
                .as("the target was actually approached rather than flushed past on every record")
                .isGreaterThan(testCase.targetBytes() / 4);

        assertRoundTrips(out, testCase);
    }

    private static void assertRoundTrips(InMemoryOutputFile out, Case testCase) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(out.buffer()))) {
            assertThat(reader.getFileMetaData().numRows())
                    .as("every record reached the file")
                    .isEqualTo(testCase.rows());
            List<RowGroup> groups = reader.getFileMetaData().rowGroups();
            assertThat(groups).as("the file was banded").isNotEmpty();
            assertThat(groups.stream().map(RowGroup::numRows))
                    .as("no row group is empty, and none exceeds what its buffers can index")
                    .allSatisfy(rows -> assertThat(rows).isBetween(1L, (long) RowGroupBuffer.MAX_ROWS));
            assertThat(groups.stream().mapToLong(RowGroup::numRows).sum())
                    .as("the groups account for every record exactly once")
                    .isEqualTo(testCase.rows());
        }
    }

    /// What a writer holds before a single record has reached it, which is the schema's own cost
    /// and the one a caller cannot reduce by writing less.
    ///
    /// A wide schema is where this bites: every column brings buffers of its own, so a bound that
    /// holds per column can still be a large multiple of the target once there are hundreds of
    /// them.
    /// How far past its row-group target an idle writer may sit, per regime.
    ///
    /// Two different arithmetics decide it. Where a column's share of the target exceeds what the
    /// floor reserves, the share decides and the whole schema opens inside its target — a
    /// thousand columns against the default target land under 1×. Where the share falls below the
    /// floor, the floor decides, and what a schema holds is its column count times the floor
    /// however small the target: 200 columns against 1 MiB measured 2.31×. The bound is stated
    /// per case so that a regression in either regime fails rather than being absorbed by the
    /// other's slack.
    static Stream<Arguments> idleShapes() {
        // Two regimes, because different arithmetic decides each. At a small target the floor
        // under a column's share sets what it holds; at a large one the share itself does, and a
        // share divided by too little reserves a multiple of the target that only shows up once
        // the column count is high.
        return Stream.of(
                Arguments.of(200, 1L << 20, PhysicalType.INT64, 3),
                Arguments.of(1000, 128L << 20, PhysicalType.INT32, 1),
                Arguments.of(1000, 128L << 20, PhysicalType.INT64, 1),
                Arguments.of(1000, 128L << 20, PhysicalType.BYTE_ARRAY, 1),
                // A fixed width its content buffer does not eagerly allocate: charging the width
                // rather than the offset started this one 33x below its share.
                Arguments.of(1000, 128L << 20, PhysicalType.FIXED_LEN_BYTE_ARRAY, 1));
    }

    @ParameterizedTest(name = "{0} x {2} against {1} bytes")
    @MethodSource("idleShapes")
    void anIdleWriterHoldsLittleForAWideSchema(int columns, long target, PhysicalType type,
            int allowedMultiple) throws IOException {
        FileSchema schema = wideSchema(columns, type);
        WriterConfig config = WriterConfig.builder().rowGroupBufferTargetBytes(target).build();

        long before = allocatedBytes();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            long allocated = allocatedBytes() - before;
            assertThat(allocated)
                    .as("bytes allocated opening a %d-column %s writer against a %,d-byte target",
                            columns, type, target)
                    .isLessThan(allowedMultiple * target);
            assertThat(writer).isNotNull();
        }
    }

    private static long allocatedBytes() {
        return ((ThreadMXBean) ManagementFactory.getThreadMXBean())
                .getCurrentThreadAllocatedBytes();
    }

    // ==================== The shapes ====================

    private static FileSchema oneColumn(PhysicalType type, RepetitionType repetition) {
        return FileSchema.builder("schema").addColumn("v", type, repetition).build();
    }

    private static FileSchema wideLongSchema() {
        return wideSchema(WIDE_COLUMNS, PhysicalType.INT64);
    }

    private static FileSchema wideSchema(int columns, PhysicalType type) {
        FileSchema.Builder schema = FileSchema.builder("schema");
        for (int c = 0; c < columns; c++) {
            if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
                schema.addColumn("c" + c, type, RepetitionType.REQUIRED, column -> column.typeLength(256));
            }
            else {
                schema.addColumn("c" + c, type, RepetitionType.REQUIRED);
            }
        }
        return schema.build();
    }

    /// A `long` retains 8 bytes stored, or a 4-byte index and a dictionary entry the table charges
    /// it for while the chunk is still interning — the larger of the two is the allowance.
    private static final long LONG_RECORD_BYTES = 4 + 8 + 2L * (8 + 4);

    private static Case flatLongs(String name, int rows, int batchRows, long target) {
        return new Case(name, oneColumn(PhysicalType.INT64, RepetitionType.REQUIRED), rows, batchRows,
                target, LONG_RECORD_BYTES, (batch, from, count) -> {
                    long[] values = new long[count];
                    for (int i = 0; i < count; i++) {
                        values[i] = from + i; // all distinct
                    }
                    batch.longs("v", values);
                });
    }

    private static Case lowCardinalityLongs(String name, int rows, int batchRows, long target) {
        return new Case(name, oneColumn(PhysicalType.INT64, RepetitionType.REQUIRED), rows, batchRows,
                target, LONG_RECORD_BYTES, (batch, from, count) -> {
                    long[] values = new long[count];
                    for (int i = 0; i < count; i++) {
                        values[i] = (from + i) % 8;
                    }
                    batch.longs("v", values);
                });
    }

    private static Case booleans(String name, int rows, int batchRows, long target) {
        return new Case(name, oneColumn(PhysicalType.BOOLEAN, RepetitionType.REQUIRED), rows, batchRows,
                target, 1, (batch, from, count) -> {
                    boolean[] values = new boolean[count];
                    for (int i = 0; i < count; i++) {
                        values[i] = ((from + i) & 1) == 0;
                    }
                    batch.booleans("v", values);
                });
    }

    /// Narrow records first and wide ones after — a file sorted by size, or a column whose values
    /// grow with time. Nothing about the records already appended anticipates the widening, so
    /// only reading what is arriving keeps the row group inside its target.
    private static Case widenedBinary(String name, int rows, int batchRows, long target) {
        int wideFrom = rows / 2;
        int wideBytes = 16 * 1024;
        return new Case(name, oneColumn(PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED), rows, batchRows,
                target, wideBytes + 64, (batch, from, count) -> {
                    byte[][] values = new byte[count][];
                    for (int i = 0; i < count; i++) {
                        int row = from + i;
                        values[i] = distinctBytes(row, row < wideFrom ? 8 : wideBytes);
                    }
                    batch.bytes("v", values);
                });
    }

    private static Case oversizedBinary(String name, int rows, int batchRows, long target) {
        return new Case(name, oneColumn(PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED), rows, batchRows,
                target, OVERSIZED_VALUE_BYTES + 64, (batch, from, count) -> {
                    byte[][] values = new byte[count][];
                    for (int i = 0; i < count; i++) {
                        values[i] = distinctBytes(from + i, OVERSIZED_VALUE_BYTES);
                    }
                    batch.bytes("v", values);
                });
    }

    private static Case nullableBinary(String name, int rows, int batchRows, long target) {
        return new Case(name, oneColumn(PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL), rows, batchRows,
                target, 128, (batch, from, count) -> {
                    byte[][] values = new byte[count][];
                    boolean[] nulls = new boolean[count];
                    for (int i = 0; i < count; i++) {
                        nulls[i] = ((from + i) & 1) == 1;
                        values[i] = nulls[i] ? null : distinctBytes(from + i, 32);
                    }
                    batch.bytes("v", values, nulls);
                });
    }

    /// A levelled column retains a byte per entry for each of its two level streams, where the
    /// value it carries is four bytes — so the levels are a third of what the column holds and
    /// the target has to count them.
    private static Case lists(String name, int rows, int batchRows, long target) {
        FileSchema schema = FileSchema.builder("schema")
                .list("tags", RepetitionType.OPTIONAL,
                        el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();
        return new Case(name, schema, rows, batchRows, target, 4 * (2 + 24), (batch, from, count) -> {
            int[] offsets = new int[count + 1];
            for (int i = 0; i < count; i++) {
                offsets[i + 1] = offsets[i] + ((from + i) % 4);
            }
            int[] values = new int[offsets[count]];
            for (int i = 0; i < values.length; i++) {
                values[i] = from + i;
            }
            batch.list("tags", offsets).ints("tags.list.element", values);
        });
    }

    private static Case wideSchema(String name, int rows, int batchRows, long target) {
        return new Case(name, wideLongSchema(), rows, batchRows, target,
                WIDE_COLUMNS * LONG_RECORD_BYTES, (batch, from, count) -> {
                    for (int c = 0; c < WIDE_COLUMNS; c++) {
                        long[] values = new long[count];
                        for (int i = 0; i < count; i++) {
                            values[i] = (long) (from + i) * WIDE_COLUMNS + c;
                        }
                        batch.longs("c" + c, values);
                    }
                });
    }

    private static byte[] distinctBytes(int row, int length) {
        byte[] value = new byte[length];
        value[0] = (byte) row;
        value[length - 1] = (byte) (row >>> 8);
        return value;
    }
}
