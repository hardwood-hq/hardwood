/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.IntToLongFunction;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.internal.bloomfilter.BloomFilterHeader;
import dev.hardwood.internal.bloomfilter.XxHash64;
import dev.hardwood.internal.reader.CountingInputFile;
import dev.hardwood.internal.thrift.BloomFilterReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The Bloom filters the writer produces for the columns configured to carry one: what each
/// filter holds, how it is sized, where it is placed, and what configuration is rejected. Read
/// back from the file's own bytes, so what is asserted is what a reader finds.
class WriterBloomFilterTest {

    private static final int ROWS = 20_000;

    @Test
    void holdsEveryValueOfEachFixedWidthType() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("i", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("l", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("f", PhysicalType.FLOAT, RepetitionType.REQUIRED)
                .addColumn("d", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .build();
        int[] ints = new int[ROWS];
        long[] longs = new long[ROWS];
        float[] floats = new float[ROWS];
        double[] doubles = new double[ROWS];
        for (int i = 0; i < ROWS; i++) {
            ints[i] = i * 7;
            longs[i] = i * 1_000_003L;
            floats[i] = i * 0.5f;
            doubles[i] = i * 0.25;
        }
        // -0.0 replaces the one +0.0 the column would hold.
        floats[0] = -0.0f;
        doubles[1] = Double.NaN;
        WriterConfig config = config().bloomFilter("i").bloomFilter("l").bloomFilter("f").bloomFilter("d").build();
        byte[] file = write(schema, config,
                batch -> batch.ints(0, ints).longs(1, longs).floats(2, floats).doubles(3, doubles));

        BloomFilter i = filter(file, 0, 0);
        BloomFilter l = filter(file, 0, 1);
        BloomFilter f = filter(file, 0, 2);
        BloomFilter d = filter(file, 0, 3);
        for (int r = 0; r < ROWS; r++) {
            assertThat(i.mightContain(XxHash64.hash(ints[r]))).isTrue();
            assertThat(l.mightContain(XxHash64.hash(longs[r]))).isTrue();
            assertThat(f.mightContain(XxHash64.hash(floats[r]))).isTrue();
            assertThat(d.mightContain(XxHash64.hash(doubles[r]))).isTrue();
        }
        // Floating-point values are hashed over their raw bits, so the zeros are told apart.
        assertThat(f.mightContain(XxHash64.hash(0.0f))).isFalse();
        assertThat(absentRate(i, n -> XxHash64.hash(n * 7 + 1))).isLessThan(0.02);
    }

    @Test
    void holdsEveryValueOfEachBinaryType() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("b", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL)
                .addColumn("x", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 8)
                .build();
        byte[][] strings = new byte[ROWS][];
        boolean[] nulls = new boolean[ROWS];
        byte[][] fixed = new byte[ROWS][];
        for (int r = 0; r < ROWS; r++) {
            strings[r] = ("key-" + r).getBytes(StandardCharsets.UTF_8);
            nulls[r] = r % 5 == 0;
            fixed[r] = ByteBuffer.allocate(8).putLong(r).array();
        }
        WriterConfig config = config().bloomFilter("b").bloomFilter("x").build();
        byte[] file = write(schema, config, batch -> batch.bytes(0, strings, nulls).fixed(1, fixed));

        BloomFilter b = filter(file, 0, 0);
        BloomFilter x = filter(file, 0, 1);
        for (int r = 0; r < ROWS; r++) {
            if (!nulls[r]) {
                assertThat(b.mightContain(XxHash64.hash(strings[r]))).isTrue();
            }
            assertThat(x.mightContain(XxHash64.hash(fixed[r]))).isTrue();
        }
    }

    @Test
    void sizesADictionaryChunksFilterByItsDistinctValues() throws Exception {
        // 100 distinct values: dictionary-encoded, so the filter is built from the 100 entries.
        int[] values = new int[ROWS];
        for (int r = 0; r < ROWS; r++) {
            values[r] = r % 100;
        }
        byte[] file = write(ints(), config().bloomFilter("v").build(), batch -> batch.ints(0, values));

        assertThat(chunk(file, 0, 0).metaData().encodings()).contains(Encoding.RLE_DICTIONARY);
        // -8 * 100 / ln(1 - 0.01^(1/8)) = 968 bits, 121 bytes, rounded up to 128.
        assertThat(filter(file, 0, 0).header().numBytes()).isEqualTo(128);
    }

    @Test
    void foldsTheFilterOfAChunkWithoutADistinctCount() throws Exception {
        // A named policy counts no distinct values, so the filter is sized for the 20,000 present
        // values and folded down to what the 100 distinct ones need.
        int[] values = new int[ROWS];
        for (int r = 0; r < ROWS; r++) {
            values[r] = r % 100;
        }
        WriterConfig config = config().encoding("v", ColumnEncoding.DELTA_BINARY_PACKED).bloomFilter("v").build();
        byte[] file = write(ints(), config, batch -> batch.ints(0, values));

        BloomFilter filter = filter(file, 0, 0);
        assertThat(filter.header().numBytes()).isLessThanOrEqualTo(256);
        for (int v = 0; v < 100; v++) {
            assertThat(filter.mightContain(XxHash64.hash(v))).isTrue();
        }
    }

    @Test
    void usesTheConfiguredProbability() throws Exception {
        int[] values = new int[ROWS];
        for (int r = 0; r < ROWS; r++) {
            values[r] = r;
        }
        byte[] tight = write(ints(), config().bloomFilter("v", 0.001).build(), batch -> batch.ints(0, values));
        byte[] loose = write(ints(), config().bloomFilter("v", 0.1).build(), batch -> batch.ints(0, values));

        assertThat(filter(tight, 0, 0).header().numBytes()).isGreaterThan(filter(loose, 0, 0).header().numBytes());
        assertThat(absentRate(filter(tight, 0, 0), n -> XxHash64.hash(ROWS + n))).isLessThan(0.002);
    }

    @Test
    void writesAFilterPerRowGroupHoldingOnlyThatRowGroupsValues() throws Exception {
        int[] values = new int[ROWS];
        for (int r = 0; r < ROWS; r++) {
            values[r] = r;
        }
        WriterConfig config = config().rowGroupTargetRows(ROWS / 2).bloomFilter("v").build();
        byte[] file = write(ints(), config, batch -> batch.ints(0, values));

        BloomFilter first = filter(file, 0, 0);
        BloomFilter second = filter(file, 1, 0);
        for (int r = 0; r < ROWS / 2; r++) {
            assertThat(first.mightContain(XxHash64.hash(r))).isTrue();
            assertThat(second.mightContain(XxHash64.hash(ROWS / 2 + r))).isTrue();
        }
        assertThat(absentRate(first, n -> XxHash64.hash(ROWS / 2 + n))).isLessThan(0.02);
    }

    @Test
    void writesAFilterForANestedLeaf() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT64, RepetitionType.OPTIONAL))
                .build();
        int records = 1_000;
        int[] offsets = new int[records + 1];
        for (int r = 0; r < records; r++) {
            offsets[r + 1] = offsets[r] + r % 4;
        }
        long[] elements = new long[offsets[records]];
        for (int e = 0; e < elements.length; e++) {
            elements[e] = e * 31L;
        }
        byte[] file = write(schema, config().bloomFilter("v.list.element").build(),
                batch -> batch.list("v", offsets).longs("v.list.element", elements));

        BloomFilter filter = filter(file, 0, 0);
        for (long element : elements) {
            assertThat(filter.mightContain(XxHash64.hash(element))).isTrue();
        }
    }

    @Test
    void writesAnEmptyFilterForAChunkOfNulls() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL)
                .build();
        boolean[] nulls = new boolean[100];
        Arrays.fill(nulls, true);
        byte[] file = write(schema, config().bloomFilter("v").build(), batch -> batch.ints(0, new int[100], nulls));

        BloomFilter filter = filter(file, 0, 0);
        assertThat(filter.header().numBytes()).isEqualTo(32);
        assertThat(filter.mightContain(XxHash64.hash(0))).isFalse();
    }

    @Test
    void writesNoFilterForAColumnNotConfigured() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("a", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("b", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        byte[] file = write(schema, config().bloomFilter("b").build(),
                batch -> batch.ints(0, new int[10]).ints(1, new int[10]));

        assertThat(chunk(file, 0, 0).metaData().bloomFilterOffset()).isNull();
        assertThat(chunk(file, 0, 0).metaData().bloomFilterLength()).isNull();
        assertThat(chunk(file, 0, 1).metaData().bloomFilterOffset()).isNotNull();
    }

    @Test
    void placesTheFiltersTogetherBetweenTheRowGroupsAndThePageIndex() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("a", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("b", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        int[] a = new int[ROWS];
        long[] b = new long[ROWS];
        for (int r = 0; r < ROWS; r++) {
            a[r] = r;
            b[r] = r;
        }
        WriterConfig config = config().rowGroupTargetRows(ROWS / 4).bloomFilter("a").bloomFilter("b").build();
        byte[] file = write(schema, config, batch -> batch.ints(0, a).longs(1, b));

        List<RowGroup> rowGroups = rowGroups(file);
        ColumnChunk lastChunk = rowGroups.getLast().columns().getLast();
        long at = lastChunk.metaData().dataPageOffset() + lastChunk.metaData().totalCompressedSize();
        for (RowGroup rowGroup : rowGroups) {
            for (ColumnChunk chunk : rowGroup.columns()) {
                assertThat(chunk.metaData().bloomFilterOffset()).isEqualTo(at);
                at += chunk.metaData().bloomFilterLength();
            }
        }
        assertThat(rowGroups.getFirst().columns().getFirst().columnIndexOffset()).isEqualTo(at);
    }

    /// Hardwood's reader prunes a Hardwood-written file on its filters, fetched from where the
    /// writer placed them: every row group's `min` / `max` span the literal, the column is
    /// `PLAIN` so no dictionary can decide it, and only the filters rule the literal out.
    @Test
    void letsTheReaderSkipRowGroupsItsFiltersRuleOut() throws Exception {
        int rowGroupRows = ROWS / 4;
        int[] values = new int[ROWS];
        for (int r = 0; r < ROWS; r++) {
            values[r] = (r % rowGroupRows) * 3;
        }
        WriterConfig config = config().rowGroupTargetRows(rowGroupRows)
                .encoding("v", ColumnEncoding.PLAIN).bloomFilter("v").build();
        byte[] file = write(ints(), config, batch -> batch.ints(0, values));
        List<RowGroup> rowGroups = rowGroups(file);

        // 4 lies between every row group's bounds of 0 and 29,997 and is no multiple of 3.
        CountingInputFile absent = new CountingInputFile(ByteBuffer.wrap(file));
        assertThat(countRows(absent, FilterPredicate.eq("v", 4))).isZero();
        assertThat(absent.reads()).noneMatch(read -> touchesData(read, rowGroups));

        CountingInputFile present = new CountingInputFile(ByteBuffer.wrap(file));
        assertThat(countRows(present, FilterPredicate.eq("v", 6))).isEqualTo(4);
        assertThat(present.reads()).anyMatch(read -> touchesData(read, rowGroups));
    }

    @Test
    void writesTheHeaderTheFormatSpecifies() throws Exception {
        byte[] file = write(ints(), config().bloomFilter("v").build(), batch -> batch.ints(0, new int[] { 1, 2, 3 }));

        BloomFilterHeader header = filter(file, 0, 0).header();
        assertThat(header.algorithm()).isEqualTo(BloomFilterHeader.Algorithm.BLOCK);
        assertThat(header.hash()).isEqualTo(BloomFilterHeader.Hash.XXHASH);
        assertThat(header.compression()).isEqualTo(BloomFilterHeader.Compression.UNCOMPRESSED);
    }

    @Test
    void rejectsAFilterForAColumnTheSchemaDoesNotHave() {
        WriterConfig config = config().bloomFilter("w").build();
        assertThatThrownBy(() -> ParquetFileWriter.create(OutputFile.inMemory(), ints(), config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bloom filter configured for column 'w', which the schema does not have. "
                        + "Its leaf columns are: [v]");
    }

    @Test
    void rejectsAFilterForABooleanColumn() {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("flag", PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = config().bloomFilter("flag").build();
        assertThatThrownBy(() -> ParquetFileWriter.create(OutputFile.inMemory(), schema, config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bloom filter configured for column 'flag', which is BOOLEAN. "
                        + "A Bloom filter can be written for any other type.");
    }

    @Test
    void rejectsAProbabilityOutsideTheOpenUnitInterval() {
        assertThatThrownBy(() -> WriterConfig.builder().bloomFilter("v", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bloom filter false-positive probability must be between 0 and 1 (exclusive) "
                        + "but was 0.0 for column v");
        assertThatThrownBy(() -> WriterConfig.builder().bloomFilter("v", 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bloom filter false-positive probability must be between 0 and 1 (exclusive) "
                        + "but was 1.0 for column v");
        assertThatThrownBy(() -> WriterConfig.builder().bloomFilter("v", Double.NaN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bloom filter false-positive probability must be between 0 and 1 (exclusive) "
                        + "but was NaN for column v");
        assertThatThrownBy(() -> WriterConfig.builder().bloomFilter(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("columnPath must not be null");
    }

    @Test
    void replacesTheProbabilityOfAColumnNamedAgain() {
        WriterConfig config = WriterConfig.builder().bloomFilter("v", 0.2).bloomFilter("v").build();
        assertThat(config.bloomFilters()).containsExactly(Map.entry("v", 0.01));
        assertThat(WriterConfig.defaults().bloomFilters()).isEmpty();
    }

    private static long countRows(InputFile input, FilterPredicate filter) throws Exception {
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(input);
                RowReader rowReader = reader.buildRowReader().filter(filter).build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }
        return rows;
    }

    /// Whether `read` overlaps the pages of any column chunk.
    private static boolean touchesData(CountingInputFile.Read read, List<RowGroup> rowGroups) {
        for (RowGroup rowGroup : rowGroups) {
            for (ColumnChunk chunk : rowGroup.columns()) {
                long start = chunk.metaData().dataPageOffset();
                long end = start + chunk.metaData().totalCompressedSize();
                if (read.offset() < end && read.end() > start) {
                    return true;
                }
            }
        }
        return false;
    }

    private static WriterConfig.Builder config() {
        return WriterConfig.builder().codec(CompressionCodec.UNCOMPRESSED);
    }

    private static FileSchema ints() {
        return FileSchema.builder("schema").addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED).build();
    }

    private static byte[] write(FileSchema schema, WriterConfig config, Consumer<ColumnBatch> filler) throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(filler);
        }
        return InMemoryFiles.toByteArray(out);
    }

    private static List<RowGroup> rowGroups(byte[] file) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            return reader.getFileMetaData().rowGroups();
        }
    }

    private static ColumnChunk chunk(byte[] file, int rowGroup, int column) throws Exception {
        return rowGroups(file).get(rowGroup).columns().get(column);
    }

    /// Reads a chunk's Bloom filter from exactly the range its metadata gives.
    private static BloomFilter filter(byte[] file, int rowGroup, int column) throws Exception {
        ColumnChunk chunk = chunk(file, rowGroup, column);
        ByteBuffer range = ByteBuffer.wrap(file, Math.toIntExact(chunk.metaData().bloomFilterOffset()),
                chunk.metaData().bloomFilterLength()).slice();
        ThriftCompactReader reader = new ThriftCompactReader(range);
        BloomFilter filter = BloomFilterReader.read(reader);
        assertThat(reader.remaining()).isZero();
        return filter;
    }

    /// The share of 100,000 probes the filter passes, for values the caller knows are absent.
    private static double absentRate(BloomFilter filter, IntToLongFunction hashOfAbsent) {
        int passed = 0;
        for (int n = 0; n < 100_000; n++) {
            if (filter.mightContain(hashOfAbsent.applyAsLong(n))) {
                passed++;
            }
        }
        return passed / 100_000.0;
    }
}
