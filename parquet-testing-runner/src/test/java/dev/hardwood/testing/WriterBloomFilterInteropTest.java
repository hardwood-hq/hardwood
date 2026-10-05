/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.OutputFile;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// The interop gate over the Bloom filters Hardwood writes: parquet-java locates every chunk's
/// filter from the footer, parses its header, and finds every value the chunk holds under its own
/// hashing, so a reader other than Hardwood's prunes on them without dropping a row group that
/// holds a match.
///
/// Hardwood's round trip cannot vouch for this: a writer and reader that agree on the bytes they
/// hash, or on the layout of a block, agree whether or not that is what the format specifies.
class WriterBloomFilterInteropTest {

    private static final int ROWS = 30_000;
    private static final int ROW_GROUP_ROWS = 10_000;

    /// Each column at high cardinality, which `AUTO` writes `PLAIN`, and at low cardinality,
    /// which it writes with a dictionary, so the filter is built from both stores.
    enum Cardinality {
        HIGH(ROWS),
        LOW(97);

        private final int distinct;

        Cardinality(int distinct) {
            this.distinct = distinct;
        }
    }

    @ParameterizedTest
    @EnumSource(Cardinality.class)
    void parquetJavaFindsEveryWrittenValue(Cardinality cardinality, @TempDir Path dir) throws IOException {
        int[] ints = new int[ROWS];
        long[] longs = new long[ROWS];
        float[] floats = new float[ROWS];
        double[] doubles = new double[ROWS];
        byte[][] strings = new byte[ROWS][];
        byte[][] fixed = new byte[ROWS][];
        boolean[] nulls = new boolean[ROWS];
        for (int r = 0; r < ROWS; r++) {
            int v = r % cardinality.distinct;
            ints[r] = v * 3;
            longs[r] = v * 1_000_003L;
            floats[r] = v * 0.5f;
            doubles[r] = v * 0.25;
            strings[r] = ("key-" + v).getBytes(StandardCharsets.UTF_8);
            fixed[r] = ByteBuffer.allocate(8).putLong(v).array();
            nulls[r] = r % 7 == 0;
        }
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("i", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("l", PhysicalType.INT64, RepetitionType.OPTIONAL)
                .addColumn("f", PhysicalType.FLOAT, RepetitionType.REQUIRED)
                .addColumn("d", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .addColumn("s", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        c -> c.logicalType(LogicalType.string()))
                .addColumn("x", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, c -> c.typeLength(8))
                .addColumn("delta", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder()
                .codec(CompressionCodec.UNCOMPRESSED)
                .rowGroupTargetRows(ROW_GROUP_ROWS)
                .encoding("delta", ColumnEncoding.DELTA_BINARY_PACKED)
                .bloomFilter("i").bloomFilter("l").bloomFilter("f").bloomFilter("d")
                .bloomFilter("s").bloomFilter("x").bloomFilter("delta")
                .build();
        Path file = dir.resolve("bloom-" + cardinality + ".parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .ints("i", ints).longs("l", longs, nulls).floats("f", floats).doubles("d", doubles)
                    .bytes("s", strings).fixed("x", fixed).ints("delta", ints));
        }

        List<BloomFilter> filters = ParquetJavaReader.readBloomFilters(file);
        int columns = schema.getColumnCount();
        assertThat(filters).hasSize(columns * ROWS / ROW_GROUP_ROWS).doesNotContainNull();
        for (int r = 0; r < ROWS; r++) {
            int rowGroup = r / ROW_GROUP_ROWS;
            BloomFilter i = filters.get(rowGroup * columns);
            assertThat(i.findHash(i.hash(ints[r]))).isTrue();
            BloomFilter l = filters.get(rowGroup * columns + 1);
            assertThat(nulls[r] || l.findHash(l.hash(longs[r]))).isTrue();
            BloomFilter f = filters.get(rowGroup * columns + 2);
            assertThat(f.findHash(f.hash(floats[r]))).isTrue();
            BloomFilter d = filters.get(rowGroup * columns + 3);
            assertThat(d.findHash(d.hash(doubles[r]))).isTrue();
            BloomFilter s = filters.get(rowGroup * columns + 4);
            assertThat(s.findHash(s.hash(Binary.fromConstantByteArray(strings[r])))).isTrue();
            BloomFilter x = filters.get(rowGroup * columns + 5);
            assertThat(x.findHash(x.hash(Binary.fromConstantByteArray(fixed[r])))).isTrue();
            BloomFilter delta = filters.get(rowGroup * columns + 6);
            assertThat(delta.findHash(delta.hash(ints[r]))).isTrue();
        }
        // A value no row holds: the filter rules it out, short of a false positive at the 1%
        // the filters are sized for, which this value does not hit.
        BloomFilter i = filters.getFirst();
        assertThat(i.findHash(i.hash(1))).isFalse();
    }

    /// parquet-java prunes on the filters, not merely parses them: with its statistics and
    /// dictionary filters off, the row groups it drops for an equality literal are the ones whose
    /// filter rules the literal out, and with its Bloom filter off as well it drops none.
    @ParameterizedTest
    @EnumSource(Cardinality.class)
    void parquetJavaPrunesRowGroupsOnTheFilters(Cardinality cardinality, @TempDir Path dir) throws IOException {
        // Row group g holds the values [g * 10,000, (g + 1) * 10,000), times 3 when high and
        // modulo 97 when low; every row group of the low case holds the same 97 values.
        int[] ints = new int[ROWS];
        byte[][] strings = new byte[ROWS][];
        for (int r = 0; r < ROWS; r++) {
            int v = cardinality == Cardinality.HIGH ? r : r % cardinality.distinct;
            ints[r] = v * 3;
            strings[r] = ("key-" + v).getBytes(StandardCharsets.UTF_8);
        }
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("i", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("s", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        c -> c.logicalType(LogicalType.string()))
                .build();
        WriterConfig config = WriterConfig.builder()
                .codec(CompressionCodec.UNCOMPRESSED)
                .rowGroupTargetRows(ROW_GROUP_ROWS)
                .bloomFilter("i").bloomFilter("s")
                .build();
        Path file = dir.resolve("bloom-pruning-" + cardinality + ".parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints("i", ints).bytes("s", strings));
        }

        // Absent from every row group: 1 is no multiple of 3, and no row holds "key-absent".
        FilterPredicate absentInt = FilterApi.eq(FilterApi.intColumn("i"), 1);
        FilterPredicate absentString = FilterApi.eq(FilterApi.binaryColumn("s"), Binary.fromString("key-absent"));
        assertThat(ParquetJavaReader.rowGroupsKept(file, absentInt, true)).isEmpty();
        assertThat(ParquetJavaReader.rowGroupsKept(file, absentString, true)).isEmpty();
        assertThat(ParquetJavaReader.rowGroupsKept(file, absentInt, false)).containsExactly(0, 1, 2);
        assertThat(ParquetJavaReader.rowGroupsKept(file, absentString, false)).containsExactly(0, 1, 2);

        // Present: a value of the middle row group's range when high, of every row group when low.
        int present = cardinality == Cardinality.HIGH ? 15_000 : 50;
        FilterPredicate presentInt = FilterApi.eq(FilterApi.intColumn("i"), present * 3);
        FilterPredicate presentString = FilterApi.eq(FilterApi.binaryColumn("s"), Binary.fromString("key-" + present));
        List<Integer> expected = cardinality == Cardinality.HIGH ? List.of(1) : List.of(0, 1, 2);
        assertThat(ParquetJavaReader.rowGroupsKept(file, presentInt, true)).isEqualTo(expected);
        assertThat(ParquetJavaReader.rowGroupsKept(file, presentString, true)).isEqualTo(expected);
    }
}
