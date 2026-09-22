/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.InputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.predicate.StatisticsDecoder;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageEncodingStats;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.writer.WriterTestSupport.columnMeta;
import static dev.hardwood.writer.WriterTestSupport.countDataPages;
import static dev.hardwood.writer.WriterTestSupport.oneColumn;
import static dev.hardwood.writer.WriterTestSupport.readInts;
import static dev.hardwood.writer.WriterTestSupport.readListOfInts;
import static org.assertj.core.api.Assertions.assertThat;

/// How a file is banded into pages and row groups, and what each page carries.
///
/// The page and row-group targets are the only layout controls a caller has, and both are
/// byte targets over buffered data rather than row counts. These assert that crossing either
/// boundary changes where the values sit and not which values they are — for flat columns,
/// for a `BOOLEAN` column whose page cut falls away from a word boundary, and for a list
/// column whose single record outgrows a page.
class WriterLayoutTest {

    @Test
    void largeColumnIsSplitAcrossMultiplePages() throws Exception {
        // Comfortably more than one target page (262,144 INT32 values per 1 MiB page).
        int n = 600_000;
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = i;
        }

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }
        byte[] bytes = out.toByteArray();

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(n);
            // One 128 MiB row group; 600k values at 262,144 per 1 MiB page ⇒ exactly 3 pages.
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(1);
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().getFirst().columns().getFirst().metaData();
            assertThat(countDataPages(bytes, meta.dataPageOffset(), meta.numValues())).isEqualTo(3);
            assertThat(meta.encodingStats()).containsExactly(
                    new PageEncodingStats(PageType.DATA_PAGE, Encoding.PLAIN, 3)
            );
            // Arrays.equals over containsExactly: the latter is O(n) with per-element
            // boxing/description and is needlessly slow at 600k elements.
            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();
        }
    }

    @Test
    void largeColumnIsSplitAcrossMultipleRowGroups() throws Exception {
        int n = 5_000;
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = i;
        }

        // 4 KiB target ⇒ 171 rows per row group ⇒ the single batch spans several groups. The
        // values are all distinct, so while the chunk is still interning, each one retains a
        // 4-byte index and a whole dictionary entry — 4 bytes of value plus the 16 the
        // open-addressing table charges it — which is 24 bytes a record, not the 4 its `PLAIN`
        // width would suggest.
        WriterConfig config = WriterConfig.builder().rowGroupBufferTargetBytes(4096).build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(n);
            // 4096 / 24 ⇒ 170 records fit and the 171st crosses, so 5000 rows are 29 full
            // groups and a 41-row tail. Pinned rather than merely asserting "more than one":
            // the cadence is the arithmetic, and an overshoot would show here first.
            assertThat(reader.getFileMetaData().rowGroups().stream().map(RowGroup::numRows))
                    .containsExactly(171L, 171L, 171L, 171L, 171L, 171L, 171L, 171L, 171L, 171L,
                            171L, 171L, 171L, 171L, 171L, 171L, 171L, 171L, 171L, 171L, 171L,
                            171L, 171L, 171L, 171L, 171L, 171L, 171L, 171L, 41L);
            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();
        }
    }

    /// A page holds what the page target says whatever a value is worth, which an entry count
    /// derived from a nominal value width cannot do. `BYTE_ARRAY` is the type that breaks such an
    /// estimate: nothing bounds how wide one value is, so a column of values wider than the
    /// estimate assumed used to carry the whole column chunk in one page.
    @Test
    void aPageHoldsThePageTargetWhateverAValueCosts() throws Exception {
        int valueLength = 8 << 10;                 // far wider than any nominal per-value estimate
        int pageTarget = 1 << 20;
        int rows = 4_096;

        byte[][] values = new byte[rows][];
        Random random = new Random(3);
        for (int i = 0; i < rows; i++) {
            values[i] = new byte[valueLength];
            random.nextBytes(values[i]);           // distinct, so the chunk is written PLAIN
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder()
                .pageTargetBytes(pageTarget)
                .codec(CompressionCodec.UNCOMPRESSED)   // so a page's bytes are its values
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes("v", values));
        }

        byte[] file = out.toByteArray();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().getFirst().columns().getFirst().metaData();
            // The page is cut before the value that would cross the target, so the target is a
            // ceiling rather than something a page overshoots. Only a value larger than the whole
            // target can breach it, having nowhere else to go.
            assertThat(largestDataPage(file, meta.dataPageOffset(), meta.numValues()))
                    .as("largest data page against a %d KiB target", pageTarget >> 10)
                    .isLessThanOrEqualTo(pageTarget);
            // A value costs its four-byte length prefix as well as its bytes, so 127 of them fit
            // the target and the 4096 values land in 33 pages.
            assertThat(countDataPages(file, meta.dataPageOffset(), meta.numValues()))
                    .as("pages over %d MiB of values", (rows * valueLength) >> 20)
                    .isEqualTo(33);
        }
    }

    /// The row target bands a file exactly, which is what the byte target cannot do: what a
    /// record costs in buffered bytes depends on its values, and what it costs on disk depends on
    /// how well they encode and compress.
    @Test
    void theRowTargetBandsAFileExactly() throws Exception {
        int rows = 25_000;
        int perGroup = 4_096;
        int[] values = new int[rows];
        for (int i = 0; i < rows; i++) {
            values[i] = i;
        }

        WriterConfig config = WriterConfig.builder().rowGroupTargetRows(perGroup).build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(reader.getFileMetaData().rowGroups().stream().map(RowGroup::numRows))
                    .containsExactly(4096L, 4096L, 4096L, 4096L, 4096L, 4096L, 424L);
            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();
        }
    }

    /// Whichever target is reached first cuts the group: here the byte target, set low enough
    /// that it lands well inside a row target that would otherwise hold the whole file.
    @Test
    void theSmallerOfTheTwoRowGroupTargetsCuts() throws Exception {
        int rows = 5_000;
        int[] values = new int[rows];
        for (int i = 0; i < rows; i++) {
            values[i] = i;
        }

        WriterConfig config = WriterConfig.builder()
                .rowGroupBufferTargetBytes(4096)      // 171 all-distinct INT32 records
                .rowGroupTargetRows(rows)             // would hold the file in one group
                .build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            List<Long> numRows = reader.getFileMetaData().rowGroups().stream()
                    .map(RowGroup::numRows)
                    .toList();
            // The byte target cut, so no group reached the row target that would have held the
            // whole file, and every group but the tail is the byte target's own cadence.
            assertThat(numRows).hasSize(30).allSatisfy(n -> assertThat(n).isLessThan(rows));
            assertThat(numRows.subList(0, numRows.size() - 1))
                    .allSatisfy(n -> assertThat(n).isEqualTo(171L));
            assertThat(numRows.getLast()).isEqualTo(41L);
        }
    }

    /// Narrow records first and wide ones after, which is what a file sorted by size, a batch of
    /// outliers, or any producer whose records grow part-way through looks like to the writer.
    ///
    /// The flush check runs between appends rather than between records, so how many records the
    /// writer commits to before looking again is what decides how far past the target a row group
    /// can run. Sizing that from what has arrived so far makes the answer wrong exactly when the
    /// data changes, and a long file is where it is most wrong: an average over millions of narrow
    /// records barely moves when the wide ones start, so the writer keeps striding as if they were
    /// still narrow.
    @Test
    void aRowGroupHoldsToItsTargetWhenRecordWidthChanges() throws Exception {
        int narrowRecords = 40_000;
        int wideRecords = 4_000;
        int wideLength = 4 << 10;
        long target = 1 << 20;

        // Distinct values throughout, so no chunk collapses into a dictionary and what each row
        // group holds is what it was charged for.
        byte[][] narrow = new byte[narrowRecords][];
        for (int i = 0; i < narrowRecords; i++) {
            narrow[i] = new byte[] { (byte) i, (byte) (i >>> 8), (byte) (i >>> 16), (byte) (i >>> 24) };
        }
        byte[][] wide = new byte[wideRecords][];
        for (int i = 0; i < wideRecords; i++) {
            wide[i] = new byte[wideLength];
            new Random(i).nextBytes(wide[i]);
        }

        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder()
                .rowGroupBufferTargetBytes(target)
                .codec(CompressionCodec.UNCOMPRESSED)
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            ColumnWriter columns = writer.columnWriter();
            columns.writeBatch(batch -> batch.bytes("v", narrow));
            columns.writeBatch(batch -> batch.bytes("v", wide));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(narrowRecords + wideRecords);
            assertThat(reader.getFileMetaData().rowGroups())
                    .as("every row group, against a %d KiB target", target >> 10)
                    .allSatisfy(group -> assertThat(group.totalByteSize())
                            .as("row group of %d records", group.numRows())
                            .isLessThan(2 * target));
        }
    }

    @Test
    void rowGroupsDoNotShareChunkState() throws Exception {
        // Two row groups whose values do not overlap: the first all-distinct, so it is written
        // PLAIN, the second low-cardinality, so it is dictionary-encoded. Whatever a row group
        // accumulates — statistics, dictionary, the encoding its values argued for — must not
        // carry into the next one.
        int perGroup = 1024;
        int[] first = new int[perGroup];
        int[] second = new int[perGroup];
        for (int i = 0; i < perGroup; i++) {
            first[i] = 1_000_000 + i;      // all distinct, so this chunk is written PLAIN
            second[i] = 10 + i % 4;        // four distinct values, so this one is dictionary-encoded
        }

        // Cut on the row target: this is about what a chunk carries into the next row group, so
        // the boundary wants to fall exactly between the two batches rather than wherever the
        // bytes the two retain happen to land — they differ, one column being all distinct and
        // the other four values repeated.
        WriterConfig config = WriterConfig.builder()
                .rowGroupTargetRows(perGroup)
                .build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, first));
            writer.columnWriter().writeBatch(batch -> batch.ints(0, second));
        }

        byte[] file = out.toByteArray();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            List<RowGroup> rowGroups = reader.getFileMetaData().rowGroups();
            assertThat(rowGroups).hasSize(2);

            // Statistics describe their own row group, not everything written so far.
            Statistics firstStats = rowGroups.getFirst().columns().getFirst().metaData().statistics();
            Statistics secondStats = rowGroups.get(1).columns().getFirst().metaData().statistics();
            assertThat(StatisticsDecoder.decodeInt(firstStats.minValue())).isEqualTo(1_000_000);
            assertThat(StatisticsDecoder.decodeInt(firstStats.maxValue())).isEqualTo(1_000_000 + perGroup - 1);
            assertThat(StatisticsDecoder.decodeInt(secondStats.minValue())).isEqualTo(10);
            assertThat(StatisticsDecoder.decodeInt(secondStats.maxValue())).isEqualTo(13);

            // The first group's values argued for PLAIN; the second's argue for a dictionary, so
            // the first group's verdict does not leak across the boundary either.
            ColumnMetaData secondChunk = rowGroups.get(1).columns().getFirst().metaData();
            assertThat(secondChunk.encodings()).contains(Encoding.RLE_DICTIONARY);

            // And that dictionary holds the second group's four values and nothing else. Reading
            // the values back would not catch a dictionary carrying the first group's entries too,
            // because the indices written against it would still resolve; the entry count does.
            ThriftCompactReader dictionaryPage = new ThriftCompactReader(ByteBuffer.wrap(file),
                    Math.toIntExact(secondChunk.dictionaryPageOffset()));
            assertThat(PageHeaderReader.read(dictionaryPage).dictionaryPageHeader().numValues()).isEqualTo(4);

            // And the values themselves survive, which a dictionary carrying the previous group's
            // entries would not manage.
            int[] expected = new int[2 * perGroup];
            System.arraycopy(first, 0, expected, 0, perGroup);
            System.arraycopy(second, 0, expected, perGroup, perGroup);
            assertThat(readInts(reader, 0)).containsExactly(expected);
        }
    }

    @Test
    void writesCorrectPageCrc() throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3, 4, 5 }));
        }
        byte[] bytes = out.toByteArray();

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)))) {
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().getFirst().columns().getFirst().metaData();
            int offset = Math.toIntExact(meta.dataPageOffset());
            ThriftCompactReader thrift = new ThriftCompactReader(ByteBuffer.wrap(bytes), offset);
            PageHeader header = PageHeaderReader.read(thrift);
            int bodyStart = offset + thrift.getBytesRead();

            assertThat(header.crc()).as("page crc must be written").isNotNull();
            CRC32 crc = new CRC32();
            crc.update(bytes, bodyStart, header.compressedPageSize());
            assertThat(header.crc().intValue()).isEqualTo((int) crc.getValue());
        }
    }

    @Test
    void booleanPagesCutAwayFromAWordBoundary() throws Exception {
        // A BOOLEAN chunk retains its values one bit each, so a page's value range starts at an
        // arbitrary bit rather than an array slot. An odd page target puts those starts away from
        // a 64-bit word boundary: at 25 bytes a page holds 200 values, so the pages begin at bits
        // 0, 8, 16, 24 … of their words. The value pattern is coprime with both 8 and 64, so a
        // shift lost anywhere in the packing changes what reads back.
        int n = 5_000;
        boolean[] values = new boolean[n];
        for (int i = 0; i < n; i++) {
            values[i] = i % 7 == 0 || i % 11 == 3;
        }

        FileSchema schema = FileSchema.builder("m")
                .addColumn("b", PhysicalType.BOOLEAN, RepetitionType.REQUIRED).build();
        WriterConfig config = WriterConfig.builder().pageTargetBytes(25).build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.booleans(0, values));
        }

        byte[] file = out.toByteArray();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            ColumnMetaData meta = columnMeta(reader, 0);
            assertThat(countDataPages(file, meta.dataPageOffset(), meta.numValues()))
                    .as("pages, so most value ranges start mid-word").isGreaterThan(1);
            assertThat(readBooleans(reader, 0)).isEqualTo(values);
        }
    }

    @Test
    void singleLargeListRecordSpansManyPages() throws Exception {
        // One record whose list is far larger than a page: streaming must seal pages part-way
        // through the record, and the reader must reassemble it across pages via rep levels.
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();

        int n = 5_000;
        int[] offsets = { 0, n };
        int[] elements = new int[n];
        List<Integer> expected = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            elements[i] = i;
            expected.add(i);
        }

        WriterConfig config = WriterConfig.builder().pageTargetBytes(64).build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.list("v", offsets).ints("v.list.element", elements));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(1);
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().getFirst().columns().getFirst().metaData();
            assertThat(meta.numValues()).isEqualTo(n); // the single record's elements span multiple pages
            int leaf = reader.getFileSchema().getColumn("v.list.element").columnIndex();
            assertThat(readListOfInts(reader, leaf)).containsExactly(expected);
        }
    }

    @Test
    void listsSurvivePageAndRowGroupBoundaries() throws Exception {
        // Many records with varying list lengths, absent lists, and interior null elements,
        // written with tiny page and row-group targets so lists straddle both boundaries.
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int records = 2_000;
        List<List<Integer>> expected = new ArrayList<>();
        List<Integer> offsets = new ArrayList<>();
        List<Boolean> listNulls = new ArrayList<>();
        List<Integer> elements = new ArrayList<>();
        List<Boolean> elementNulls = new ArrayList<>();
        offsets.add(0);
        int element = 0;
        for (int r = 0; r < records; r++) {
            if (r % 7 == 0) {
                listNulls.add(true);
                expected.add(null);
            }
            else {
                listNulls.add(false);
                List<Integer> list = new ArrayList<>();
                int length = r % 4; // 0..3, so empty and non-empty both occur
                for (int k = 0; k < length; k++) {
                    boolean isNull = element % 5 == 0;
                    int value = r * 10 + k;
                    elements.add(value);
                    elementNulls.add(isNull);
                    list.add(isNull ? null : value);
                    element++;
                }
                expected.add(list);
            }
            offsets.add(elements.size());
        }

        WriterConfig config = WriterConfig.builder().pageTargetBytes(64).rowGroupBufferTargetBytes(256).build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .list("v", toIntArray(offsets), Validity.ofNulls(toBooleanArray(listNulls)))
                    .ints("v.list.element", toIntArray(elements), toBooleanArray(elementNulls)));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(reader.getFileMetaData().rowGroups().size()).isGreaterThan(1);
            int leaf = reader.getFileSchema().getColumn("v.list.element").columnIndex();
            assertThat(readListOfInts(reader, leaf)).isEqualTo(expected);
        }
    }

    private static boolean[] readBooleans(ParquetFileReader reader, int columnIndex) throws IOException {
        try (ColumnReader column = reader.columnReader(columnIndex)) {
            boolean[] result = new boolean[Math.toIntExact(reader.getFileMetaData().numRows())];
            int pos = 0;
            while (column.nextBatch()) {
                int count = column.getValueCount();
                System.arraycopy(column.getBooleans(), 0, result, pos, count);
                pos += count;
            }
            return result;
        }
    }

    /// The largest data page of a column chunk, by uncompressed body size.
    /// The page target is a ceiling under every encoding, and the width a page is cut on is the
    /// one the type or the dictionary fixes.
    ///
    /// `PLAIN` and `BYTE_STREAM_SPLIT` write a type's own width, so a page of them lands on the
    /// target. A delta encoding's width is a property of the values — `DELTA_BINARY_PACKED` over
    /// gently ascending values spends a few bits each — and the cut charges the width the type
    /// would have taken `PLAIN`, which no delta encoding exceeds. So those pages land under the
    /// target rather than on it, and what this pins is that they land *under* it: a page that
    /// passed the ceiling would be the defect, and the shortfall costs page headers rather than
    /// correctness. Measured on this fixture it is 26 headers and 1,035 bytes in 5.16 MB.
    @ParameterizedTest(name = "{0}")
    @EnumSource(value = ColumnEncoding.class,
            names = { "PLAIN", "BYTE_STREAM_SPLIT", "DELTA_BINARY_PACKED" })
    void aPageHoldsThePageTargetUnderEveryEncodingItsTypeAllows(ColumnEncoding encoding) throws Exception {
        // Ascending by a varying step: the deltas are far narrower than the 64 bits the type
        // would take `PLAIN`, so charging the type's width cuts pages short — but not so narrow
        // that they cost nothing, which a constant step would make them (every delta equal means
        // a zero bit width, and then a whole chunk encodes to its block headers and no page can
        // fill whatever the target).
        int rows = 4_000_000;
        long[] values = new long[rows];
        Random random = new Random(20250827L);
        long value = 0;
        for (int i = 0; i < rows; i++) {
            value += random.nextInt(1 << 10);
            values[i] = value;
        }

        int pageTarget = 1 << 20;
        WriterConfig config = WriterConfig.builder()
                .pageTargetBytes(pageTarget)
                .codec(CompressionCodec.UNCOMPRESSED)
                .encoding(encoding)
                // The chunk has to outlast several pages for the cut to be observable at all.
                .rowGroupTargetRows(Long.MAX_VALUE)
                .build();
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.longs("v", values));
        }
        byte[] file = out.toByteArray();

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().getFirst().columns().getFirst().metaData();
            int largest = largestDataPage(file, meta.dataPageOffset(), meta.numValues());
            assertThat(largest)
                    .as("largest %s page against a %,d-byte target", encoding, pageTarget)
                    .isLessThanOrEqualTo(pageTarget);
            if (encoding != ColumnEncoding.DELTA_BINARY_PACKED) {
                assertThat(largest)
                        .as("a %s page fills the target, its width being the type's", encoding)
                        .isGreaterThan(pageTarget / 2);
            }
        }
    }

    private static int largestDataPage(byte[] file, long startOffset, long totalValues) {
        ByteBuffer buf = ByteBuffer.wrap(file);
        int offset = Math.toIntExact(startOffset);
        long seen = 0;
        int largest = 0;
        while (seen < totalValues) {
            ThriftCompactReader reader = new ThriftCompactReader(buf, offset);
            PageHeader header = PageHeaderReader.read(reader);
            largest = Math.max(largest, header.uncompressedPageSize());
            seen += header.dataPageHeader().numValues();
            offset += reader.getBytesRead() + header.compressedPageSize();
        }
        return largest;
    }

    private static int[] toIntArray(List<Integer> list) {
        int[] array = new int[list.size()];
        for (int i = 0; i < array.length; i++) {
            array[i] = list.get(i);
        }
        return array;
    }

    private static boolean[] toBooleanArray(List<Boolean> list) {
        boolean[] array = new boolean[list.size()];
        for (int i = 0; i < array.length; i++) {
            array[i] = list.get(i);
        }
        return array;
    }
}
