/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Round-trip tests for the stage-12b variable-width types (`BYTE_ARRAY`,
/// `FIXED_LEN_BYTE_ARRAY`): values, null positions, dictionary encoding, lexicographic
/// statistics with `BYTE_ARRAY` truncation, and the buffered-byte row-group flush.
class WriterVariableWidthTypeRoundTripTest {

    @Test
    void writesAndReadsBackByteArrays() throws Exception {
        byte[][] values = { bytes("hello"), new byte[0], bytes("a longer value than the rest"), bytes("x") };
        FileSchema schema = oneColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, null);

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            assertThat(readBinaries(reader, 0)).isDeepEqualTo(values);
        }
    }

    @Test
    void writesAndReadsBackNullableByteArrays() throws Exception {
        byte[][] values = { bytes("a"), bytes("ignored"), bytes("c"), bytes("ignored"), bytes("e") };
        boolean[] nulls = { false, true, false, true, false };
        FileSchema schema = oneColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, null);

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes(0, values, nulls));
        }

        try (ParquetFileReader reader = openReader(out)) {
            byte[][] got = new byte[values.length][];
            try (ColumnReader c = reader.columnReader(0)) {
                int pos = 0;
                while (c.nextBatch()) {
                    int count = c.getRecordCount();
                    byte[][] batch = c.getBinaries();
                    Validity validity = c.getLeafValidity();
                    for (int j = 0; j < count; j++) {
                        got[pos + j] = validity.isNull(j) ? null : batch[j];
                    }
                    pos += count;
                }
            }
            assertThat(got[0]).isEqualTo(bytes("a"));
            assertThat(got[1]).isNull();
            assertThat(got[2]).isEqualTo(bytes("c"));
            assertThat(got[3]).isNull();
            assertThat(got[4]).isEqualTo(bytes("e"));
            assertThat(columnMeta(reader, 0).statistics().nullCount()).isEqualTo(2L);
        }
    }

    @Test
    void writesAndReadsBackFixedLenByteArrays() throws Exception {
        byte[][] values = { bytes("abcd"), bytes("wxyz"), bytes("0000") };
        FileSchema schema = oneColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 4);

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.fixed(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            assertThat(reader.getFileSchema().getColumn(0).typeLength()).isEqualTo(4);
            assertThat(readBinaries(reader, 0)).isDeepEqualTo(values);
        }
    }

    @Test
    void dictionaryEncodesLowCardinalityByteArrays() throws Exception {
        byte[][] dictionary = { bytes("red"), bytes("green"), bytes("blue"), bytes("yellow") };
        byte[][] values = new byte[1000][];
        for (int i = 0; i < values.length; i++) {
            values[i] = dictionary[i % dictionary.length];
        }
        FileSchema schema = oneColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, null);

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            assertThat(columnMeta(reader, 0).dictionaryPageOffset()).isNotNull();
            assertThat(readBinaries(reader, 0)).isDeepEqualTo(values);
        }
    }

    @Test
    void byteArrayStatisticsUseUnsignedLexicographicOrder() throws Exception {
        // Includes a high-bit byte (0x80) that would sort negative under signed comparison but
        // is the maximum under the unsigned order Parquet mandates for BYTE_ARRAY.
        byte[][] values = { bytes("banana"), bytes("apple"), { (byte) 0x80 }, bytes("cherry") };
        FileSchema schema = oneColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, null);

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(stats.minValue()).isEqualTo(bytes("apple"));
            assertThat(stats.maxValue()).isEqualTo(new byte[] { (byte) 0x80 });
            assertThat(stats.isMinValueExact()).isTrue();
            assertThat(stats.isMaxValueExact()).isTrue();
        }
    }

    @Test
    void longByteArrayBoundsAreTruncatedAndFlaggedInexact() throws Exception {
        // Two values sharing a long common prefix, longer than the truncation length.
        byte[] min = bytes("aaaaaaaaaa_min_suffix");
        byte[] max = bytes("aaaaaaaaaa_max_suffix");
        byte[][] values = { max, min };
        FileSchema schema = oneColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, null);
        WriterConfig config = WriterConfig.builder().statisticsTruncationLength(8).build();

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(stats.isMinValueExact()).isFalse();
            assertThat(stats.isMaxValueExact()).isFalse();
            assertThat(stats.minValue().length).isLessThanOrEqualTo(8);
            // The truncated bounds must still bracket every value in unsigned order.
            assertThat(Arrays.compareUnsigned(stats.minValue(), min)).isLessThanOrEqualTo(0);
            assertThat(Arrays.compareUnsigned(stats.maxValue(), max)).isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    void allOnesByteArrayMaxIsDroppedWhenTruncated() throws Exception {
        // A max whose truncated prefix is all 0xFF has no valid shorter upper bound, so the max
        // bound is omitted while the min is still written.
        byte[] allOnes = { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
        byte[][] values = { allOnes, bytes("a") };
        FileSchema schema = oneColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, null);
        WriterConfig config = WriterConfig.builder().statisticsTruncationLength(2).build();

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(stats.minValue()).isNotNull();
            assertThat(stats.maxValue()).isNull();
        }
    }

    @Test
    void variableWidthColumnFlushesByBufferedBytes() throws Exception {
        // Each value is ~100 bytes; a small row-group target must split into several groups,
        // proving the flush tracks actual buffered bytes rather than a fixed rows-per-group proxy.
        byte[][] values = new byte[400][];
        for (int i = 0; i < values.length; i++) {
            values[i] = bytes(("value-" + i + "-").repeat(10));
        }
        FileSchema schema = oneColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, null);
        WriterConfig config = WriterConfig.builder().rowGroupBufferTargetBytes(8192).encoding(ColumnEncoding.PLAIN).build();

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            assertThat(reader.getFileMetaData().rowGroups().size()).isGreaterThan(1);
            assertThat(readBinaries(reader, 0)).isDeepEqualTo(values);
        }
    }

    @Test
    void writesAndReadsBackByteArrayInsideList() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("tags", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED))
                .build();

        int[] offsets = { 0, 2, 2, 5 };
        Validity listNulls = Validity.ofNulls(new boolean[] { false, false, false });
        byte[][] elements = { bytes("a"), bytes("b"), bytes("c"), bytes("d"), bytes("e") };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .list("tags", offsets, listNulls)
                    .bytes("tags.list.element", elements));
        }

        try (ParquetFileReader reader = openReader(out)) {
            int leaf = reader.getFileSchema().getColumn("tags.list.element").columnIndex();
            try (ColumnReader c = reader.columnReader(leaf)) {
                c.nextBatch();
                int[] listOffsets = c.getLayerOffsets(0);
                byte[][] got = c.getBinaries();
                List<List<byte[]>> lists = new ArrayList<>();
                for (int r = 0; r < c.getRecordCount(); r++) {
                    List<byte[]> entry = new ArrayList<>();
                    for (int e = listOffsets[r]; e < listOffsets[r + 1]; e++) {
                        entry.add(got[e]);
                    }
                    lists.add(entry);
                }
                assertThat(lists.get(0)).containsExactly(bytes("a"), bytes("b"));
                assertThat(lists.get(1)).isEmpty();
                assertThat(lists.get(2)).containsExactly(bytes("c"), bytes("d"), bytes("e"));
            }
        }
    }

    @Test
    void fixedColumnRejectsWrongLengthValue() throws Exception {
        FileSchema schema = oneColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 4);
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), schema)) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch.fixed(0, new byte[][] { bytes("abcde") })))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void builderRejectsFixedWithoutTypeLength() {
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED)
                .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void builderRejectsTypeLengthOnNonFixedType() {
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, c -> c.typeLength(4))
                .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void writesAndReadsBackNullableFixedLenByteArrays() throws Exception {
        byte[][] values = { bytes("aaaa"), bytes("xxxx"), bytes("cccc"), bytes("xxxx"), bytes("eeee") };
        boolean[] nulls = { false, true, false, true, false };
        FileSchema schema = oneColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 4);

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.fixed(0, values, nulls));
        }

        try (ParquetFileReader reader = openReader(out)) {
            byte[][] got = new byte[values.length][];
            try (ColumnReader c = reader.columnReader(0)) {
                int pos = 0;
                while (c.nextBatch()) {
                    int count = c.getRecordCount();
                    byte[][] batch = c.getBinaries();
                    Validity validity = c.getLeafValidity();
                    for (int j = 0; j < count; j++) {
                        got[pos + j] = validity.isNull(j) ? null : batch[j];
                    }
                    pos += count;
                }
            }
            assertThat(got[0]).isEqualTo(bytes("aaaa"));
            assertThat(got[1]).isNull();
            assertThat(got[2]).isEqualTo(bytes("cccc"));
            assertThat(got[3]).isNull();
            assertThat(got[4]).isEqualTo(bytes("eeee"));
            assertThat(columnMeta(reader, 0).statistics().nullCount()).isEqualTo(2L);
        }
    }

    @Test
    void dictionaryEncodesLowCardinalityFixedLenByteArrays() throws Exception {
        byte[][] dictionary = { bytes("red"), bytes("grn"), bytes("blu"), bytes("ylw") };
        byte[][] values = new byte[1000][];
        for (int i = 0; i < values.length; i++) {
            values[i] = dictionary[i % dictionary.length];
        }
        FileSchema schema = oneColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 3);

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.fixed(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            assertThat(columnMeta(reader, 0).dictionaryPageOffset()).isNotNull();
            assertThat(readBinaries(reader, 0)).isDeepEqualTo(values);
        }
    }

    @Test
    void fixedLenByteArrayBoundsAreWholeAndExactBeyondTruncationLength() throws Exception {
        // A FIXED_LEN_BYTE_ARRAY wider than the truncation length must still carry whole, exact
        // bounds: a fixed width already bounds the footer, so BYTE_ARRAY truncation must not apply.
        byte[] min = bytes("aaaaaaaaaaaaaaaaaaaa"); // 20 bytes
        byte[] max = bytes("zzzzzzzzzzzzzzzzzzzz"); // 20 bytes
        byte[][] values = { max, min };
        FileSchema schema = oneColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 20);
        WriterConfig config = WriterConfig.builder().statisticsTruncationLength(8).build();

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.fixed(0, values));
        }

        try (ParquetFileReader reader = openReader(out)) {
            Statistics stats = columnMeta(reader, 0).statistics();
            assertThat(stats.minValue()).isEqualTo(min);
            assertThat(stats.maxValue()).isEqualTo(max);
            assertThat(stats.isMinValueExact()).isTrue();
            assertThat(stats.isMaxValueExact()).isTrue();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static FileSchema oneColumn(String name, PhysicalType type, RepetitionType repetition, Integer typeLength) {
        return typeLength == null
                ? FileSchema.builder("schema").addColumn(name, type, repetition).build()
                : FileSchema.builder("schema").addColumn(name, type, repetition, c -> c.typeLength(typeLength)).build();
    }

    private static ParquetFileReader openReader(InMemoryOutputFile out) throws Exception {
        return ParquetFileReader.open(InputFile.of(out.buffer()));
    }

    private static ColumnMetaData columnMeta(ParquetFileReader reader, int columnIndex) {
        return reader.getFileMetaData().rowGroups().get(0).columns().get(columnIndex).metaData();
    }

    private static byte[][] readBinaries(ParquetFileReader reader, int columnIndex) throws IOException {
        try (ColumnReader column = reader.columnReader(columnIndex)) {
            byte[][] result = new byte[Math.toIntExact(reader.getFileMetaData().numRows())][];
            int pos = 0;
            while (column.nextBatch()) {
                int count = column.getValueCount();
                byte[][] batch = column.getBinaries();
                System.arraycopy(batch, 0, result, pos, count);
                pos += count;
            }
            return result;
        }
    }
}
