/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageEncodingStats;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.writer.WriterTestSupport.columnMeta;
import static dev.hardwood.writer.WriterTestSupport.expectedNullable;
import static dev.hardwood.writer.WriterTestSupport.oneOptionalColumn;
import static dev.hardwood.writer.WriterTestSupport.readListOfInts;
import static dev.hardwood.writer.WriterTestSupport.readNullable;
import static org.assertj.core.api.Assertions.assertThat;

/// Data pages written as `DATA_PAGE_V2`: the header's counts and level lengths, the levels stored
/// uncompressed ahead of the compressed values, and the file reading back through Hardwood.
/// parquet-java reads the same shapes in the interop gate (`WriterInteropTest`,
/// `WriterCodecInteropTest`, `WriterNestedInteropTest`).
class WriterDataPageV2Test {

    private static final int ROWS = 10_000;
    private static final int PAGE_ROWS = 1_000;

    @Test
    void headersStateEachPagesRowsNullsAndLevelLengths() throws Exception {
        int[] values = new int[ROWS];
        boolean[] nulls = new boolean[ROWS];
        for (int i = 0; i < ROWS; i++) {
            values[i] = i;
            nulls[i] = i % 3 == 0 || (i >= 4_000 && i < 5_000);
        }
        byte[] file = writeOptional(values, nulls, v2(CompressionCodec.ZSTD));

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            ColumnMetaData meta = columnMeta(reader, 0);
            List<Page> pages = dataPages(file, meta);
            assertThat(pages).hasSize(ROWS / PAGE_ROWS);
            for (int p = 0; p < pages.size(); p++) {
                PageHeader header = pages.get(p).header();
                assertThat(header.type()).isEqualTo(PageType.DATA_PAGE_V2);
                DataPageHeaderV2 v2 = header.dataPageHeaderV2();
                int from = p * PAGE_ROWS;
                long pageNulls = countNulls(nulls, from, from + PAGE_ROWS);
                assertThat(v2.numValues()).as("page %d values", p).isEqualTo(PAGE_ROWS);
                assertThat(v2.numRows()).as("page %d rows", p).isEqualTo(PAGE_ROWS);
                assertThat((long) v2.numNulls()).as("page %d nulls", p).isEqualTo(pageNulls);
                assertThat(v2.repetitionLevelsByteLength()).as("page %d repetition levels", p).isZero();
                assertThat(v2.definitionLevelsByteLength()).as("page %d definition levels", p).isPositive();
                // The page of nulls alone has no values to compress.
                assertThat(v2.isCompressed()).as("page %d compressed", p).isEqualTo(pageNulls < PAGE_ROWS);
                assertThat(v2.encoding()).isEqualTo(Encoding.PLAIN);
                assertThat(header.crc()).as("page %d CRC over levels and values as stored", p)
                        .isEqualTo(crc(file, pages.get(p).bodyStart(), header.compressedPageSize()));
            }
            assertThat(meta.encodingStats())
                    .containsExactly(new PageEncodingStats(PageType.DATA_PAGE_V2, Encoding.PLAIN, pages.size()));
            assertThat(readNullable(reader, 0)).containsExactly(expectedNullable(values, nulls));
        }
    }

    /// The definition levels a V2 page stores are the stream a V1 page of the same rows carries
    /// after its length prefix, uncompressed although the page's values are compressed.
    @Test
    void storesTheLevelsUncompressedAndUnprefixedAheadOfTheValues() throws Exception {
        int[] values = new int[ROWS];
        boolean[] nulls = new boolean[ROWS];
        for (int i = 0; i < ROWS; i++) {
            values[i] = i * 7;
            nulls[i] = i % 5 == 1;
        }
        WriterConfig v1 = WriterConfig.builder()
                .encoding(ColumnEncoding.PLAIN)
                .pageTargetRows(PAGE_ROWS)
                .compression(Compression.uncompressed())
                .build();
        byte[] v1File = writeOptional(values, nulls, v1);
        byte[] v2File = writeOptional(values, nulls, v2(CompressionCodec.ZSTD));

        List<Page> v1Pages = dataPages(v1File, meta(v1File));
        List<Page> v2Pages = dataPages(v2File, meta(v2File));
        assertThat(v2Pages).hasSameSizeAs(v1Pages);
        for (int p = 0; p < v2Pages.size(); p++) {
            int levels = v2Pages.get(p).header().dataPageHeaderV2().definitionLevelsByteLength();
            int v1Body = v1Pages.get(p).bodyStart();
            assertThat(ByteBuffer.wrap(v1File, v1Body, 4).order(ByteOrder.LITTLE_ENDIAN).getInt())
                    .as("page %d V1 level prefix", p).isEqualTo(levels);
            int v2Body = v2Pages.get(p).bodyStart();
            assertThat(Arrays.copyOfRange(v2File, v2Body, v2Body + levels)).as("page %d levels", p)
                    .isEqualTo(Arrays.copyOfRange(v1File, v1Body + 4, v1Body + 4 + levels));
        }
    }

    @Test
    void uncompressedPagesSayTheirValuesAreNotCompressed() throws Exception {
        int[] values = new int[ROWS];
        boolean[] nulls = new boolean[ROWS];
        for (int i = 0; i < ROWS; i++) {
            values[i] = i;
            nulls[i] = i % 4 == 0;
        }
        byte[] file = writeOptional(values, nulls, v2(CompressionCodec.UNCOMPRESSED));

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            for (Page page : dataPages(file, columnMeta(reader, 0))) {
                assertThat(page.header().dataPageHeaderV2().isCompressed()).isFalse();
                assertThat(page.header().compressedPageSize()).isEqualTo(page.header().uncompressedPageSize());
            }
            assertThat(readNullable(reader, 0)).containsExactly(expectedNullable(values, nulls));
        }
    }

    /// A page whose values do not shrink under the codec stores them as they are and says so,
    /// page by page: here the pages of a constant run compress, and those of random values do not.
    @Test
    void storesValuesThatDoNotCompressAsTheyAre() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .primitive("v", PhysicalType.INT64, RepetitionType.OPTIONAL)
                .build();
        long[] values = new long[ROWS];
        boolean[] nulls = new boolean[ROWS];
        Random random = new Random(1820);
        for (int i = 0; i < ROWS; i++) {
            values[i] = i < ROWS / 2 ? 42 : random.nextLong();
            nulls[i] = i % 9 == 0;
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, v2(CompressionCodec.ZSTD))) {
            writer.columnWriter().writeBatch(batch -> batch.longs("v", values, nulls));
        }
        byte[] file = InMemoryFiles.toByteArray(out);

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            List<Page> pages = dataPages(file, columnMeta(reader, 0));
            assertThat(pages).hasSize(ROWS / PAGE_ROWS);
            for (int p = 0; p < pages.size(); p++) {
                PageHeader header = pages.get(p).header();
                boolean randomPage = p >= pages.size() / 2;
                assertThat(header.dataPageHeaderV2().isCompressed()).as("page %d compressed", p)
                        .isEqualTo(!randomPage);
                if (randomPage) {
                    assertThat(header.compressedPageSize()).as("page %d stored size", p)
                            .isEqualTo(header.uncompressedPageSize());
                }
                else {
                    assertThat(header.compressedPageSize()).as("page %d stored size", p)
                            .isLessThan(header.uncompressedPageSize());
                }
                assertThat(header.crc()).as("page %d CRC", p)
                        .isEqualTo(crc(file, pages.get(p).bodyStart(), header.compressedPageSize()));
            }
            int row = 0;
            try (ColumnReader column = reader.columnReader(0)) {
                while (column.nextBatch()) {
                    long[] read = column.getLongs();
                    Validity validity = column.getLeafValidity();
                    for (int i = 0; i < column.getRecordCount(); i++, row++) {
                        assertThat(validity.isNull(i)).as("row %d null", row).isEqualTo(nulls[row]);
                        if (!nulls[row]) {
                            assertThat(read[i]).as("row %d", row).isEqualTo(values[row]);
                        }
                    }
                }
            }
            assertThat(row).isEqualTo(ROWS);
        }
    }

    /// A dictionary chunk keeps its dictionary page; only its data pages change type.
    @Test
    void dictionaryChunkCountsItsDataPagesAsV2() throws Exception {
        int[] values = new int[ROWS];
        boolean[] nulls = new boolean[ROWS];
        for (int i = 0; i < ROWS; i++) {
            values[i] = i % 10;
            nulls[i] = i % 7 == 0;
        }
        WriterConfig config = WriterConfig.builder()
                .dataPageVersion(DataPageVersion.V2)
                .pageTargetRows(PAGE_ROWS)
                .build();
        byte[] file = writeOptional(values, nulls, config);

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            ColumnMetaData meta = columnMeta(reader, 0);
            assertThat(meta.encodingStats()).containsExactly(
                    new PageEncodingStats(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1),
                    new PageEncodingStats(PageType.DATA_PAGE_V2, Encoding.RLE_DICTIONARY, ROWS / PAGE_ROWS));
            assertThat(readNullable(reader, 0)).containsExactly(expectedNullable(values, nulls));
        }
    }

    /// A page with no present value has an empty value section, which compressing would only grow.
    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class,
            names = { "UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD", "LZ4_RAW", "BROTLI" })
    void allNullPagesReadBack(CompressionCodec codec) throws Exception {
        int[] values = new int[ROWS];
        boolean[] nulls = new boolean[ROWS];
        Arrays.fill(nulls, true);
        byte[] file = writeOptional(values, nulls, v2(codec));

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            for (Page page : dataPages(file, columnMeta(reader, 0))) {
                DataPageHeaderV2 v2 = page.header().dataPageHeaderV2();
                assertThat(v2.numNulls()).isEqualTo(PAGE_ROWS);
                assertThat(v2.isCompressed()).as("an empty value section is stored as it is").isFalse();
                assertThat(page.header().compressedPageSize()).isEqualTo(v2.definitionLevelsByteLength());
            }
            assertThat(readNullable(reader, 0)).containsExactly(expectedNullable(values, nulls));
        }
    }

    /// A repeated column's pages each start a record, so a page's row count is the records it
    /// holds, and its levels carry a repetition stream ahead of the definition stream.
    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class,
            names = { "UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD", "LZ4_RAW", "BROTLI" })
    void listColumnPagesCountTheirRecords(CompressionCodec codec) throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("l", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();
        int records = ROWS;
        int[] offsets = new int[records + 1];
        boolean[] listNulls = new boolean[records];
        List<List<Integer>> expected = new ArrayList<>();
        List<Integer> elements = new ArrayList<>();
        List<Boolean> elementNulls = new ArrayList<>();
        for (int r = 0; r < records; r++) {
            offsets[r] = elements.size();
            listNulls[r] = r % 11 == 0;
            if (listNulls[r]) {
                expected.add(null);
                continue;
            }
            List<Integer> list = new ArrayList<>();
            for (int e = 0; e < r % 4; e++) {
                boolean isNull = (r + e) % 5 == 0;
                elements.add(r + e);
                elementNulls.add(isNull);
                list.add(isNull ? null : r + e);
            }
            expected.add(list);
        }
        offsets[records] = elements.size();
        int[] elementValues = elements.stream().mapToInt(Integer::intValue).toArray();
        boolean[] elementNullFlags = new boolean[elementNulls.size()];
        for (int i = 0; i < elementNullFlags.length; i++) {
            elementNullFlags[i] = elementNulls.get(i);
        }

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, v2(codec))) {
            writer.columnWriter().writeBatch(batch -> batch
                    .list("l", offsets, Validity.ofNulls(listNulls))
                    .ints("l.list.element", elementValues, elementNullFlags));
        }
        byte[] file = InMemoryFiles.toByteArray(out);

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            List<Page> pages = dataPages(file, columnMeta(reader, 0));
            assertThat(pages).hasSize(records / PAGE_ROWS);
            for (Page page : pages) {
                DataPageHeaderV2 v2 = page.header().dataPageHeaderV2();
                assertThat(v2.numRows()).isEqualTo(PAGE_ROWS);
                assertThat(v2.repetitionLevelsByteLength()).isPositive();
                assertThat(v2.definitionLevelsByteLength()).isPositive();
            }
            assertThat(readListOfInts(reader, 0)).containsExactlyElementsOf(expected);
        }
    }

    /// `ALP` has no reader but Hardwood's, so its value section, sliced out of a V2 page past
    /// the levels, is held to the values here.
    @ParameterizedTest
    @EnumSource(value = PhysicalType.class, names = { "FLOAT", "DOUBLE" })
    void alpValuesReadBackFromV2Pages(PhysicalType type) throws Exception {
        FileSchema schema = FileSchema.builder("schema").primitive("v", type, RepetitionType.OPTIONAL).build();
        double[] doubles = new double[ROWS];
        float[] floats = new float[ROWS];
        boolean[] nulls = new boolean[ROWS];
        for (int i = 0; i < ROWS; i++) {
            doubles[i] = (i % 1_000) / 100.0;
            floats[i] = (float) doubles[i];
            nulls[i] = i % 6 == 0;
        }
        WriterConfig config = WriterConfig.builder()
                .dataPageVersion(DataPageVersion.V2)
                .encoding(ColumnEncoding.ALP)
                .pageTargetRows(PAGE_ROWS)
                .compression(Compression.zstd())
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> {
                if (type == PhysicalType.DOUBLE) {
                    batch.doubles("v", doubles, nulls);
                }
                else {
                    batch.floats("v", floats, nulls);
                }
            });
        }
        byte[] file = InMemoryFiles.toByteArray(out);

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            ColumnMetaData meta = columnMeta(reader, 0);
            assertThat(meta.encodingStats())
                    .containsExactly(new PageEncodingStats(PageType.DATA_PAGE_V2, Encoding.ALP, ROWS / PAGE_ROWS));
            int row = 0;
            try (ColumnReader column = reader.columnReader(0)) {
                while (column.nextBatch()) {
                    Validity validity = column.getLeafValidity();
                    double[] readDoubles = type == PhysicalType.DOUBLE ? column.getDoubles() : null;
                    float[] readFloats = type == PhysicalType.FLOAT ? column.getFloats() : null;
                    for (int i = 0; i < column.getRecordCount(); i++, row++) {
                        assertThat(validity.isNull(i)).as("row %d null", row).isEqualTo(nulls[row]);
                        if (nulls[row]) {
                            continue;
                        }
                        if (readDoubles != null) {
                            assertThat(readDoubles[i]).as("row %d", row).isEqualTo(doubles[row]);
                        }
                        else {
                            assertThat(readFloats[i]).as("row %d", row).isEqualTo(floats[row]);
                        }
                    }
                }
            }
            assertThat(row).isEqualTo(ROWS);
        }
    }

    private record Page(PageHeader header, int bodyStart) {
    }

    private static WriterConfig v2(CompressionCodec codec) {
        return WriterConfig.builder()
                .dataPageVersion(DataPageVersion.V2)
                .encoding(ColumnEncoding.PLAIN)
                .pageTargetRows(PAGE_ROWS)
                .compression(Compression.of(codec))
                .build();
    }

    private static byte[] writeOptional(int[] values, boolean[] nulls, WriterConfig config) throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values, nulls));
        }
        return InMemoryFiles.toByteArray(out);
    }

    private static ColumnMetaData meta(byte[] file) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            return columnMeta(reader, 0);
        }
    }

    /// The chunk's data pages, walked from its first until they cover its values.
    private static List<Page> dataPages(byte[] file, ColumnMetaData meta) throws Exception {
        List<Page> pages = new ArrayList<>();
        int offset = Math.toIntExact(meta.dataPageOffset());
        long seen = 0;
        while (seen < meta.numValues()) {
            ThriftCompactReader thrift = new ThriftCompactReader(ByteBuffer.wrap(file), offset);
            PageHeader header = PageHeaderReader.read(thrift);
            int bodyStart = offset + thrift.getBytesRead();
            pages.add(new Page(header, bodyStart));
            seen += header.type() == PageType.DATA_PAGE_V2
                    ? header.dataPageHeaderV2().numValues()
                    : header.dataPageHeader().numValues();
            offset = bodyStart + header.compressedPageSize();
        }
        return pages;
    }

    private static long countNulls(boolean[] nulls, int from, int to) {
        long count = 0;
        for (int i = from; i < to; i++) {
            if (nulls[i]) {
                count++;
            }
        }
        return count;
    }

    private static int crc(byte[] file, int from, int length) {
        CRC32 crc = new CRC32();
        crc.update(file, from, length);
        return (int) crc.getValue();
    }
}
