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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.predicate.BinaryComparator;
import dev.hardwood.internal.thrift.ColumnIndexReader;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The page index the writer produces: an `OffsetIndex` for every column chunk, a `ColumnIndex`
/// wherever the chunk's bounds can be stated, and the page layout the index depends on — every
/// page starting at a record boundary, and no page holding more records than
/// `pageTargetRows`. Read back from the file's own bytes, so what is asserted is what a reader
/// finds.
class WriterPageIndexTest {

    private static final WriterConfig UNCOMPRESSED = WriterConfig.builder()
            .codec(CompressionCodec.UNCOMPRESSED)
            .pageTargetRows(10_000)
            .build();

    @Test
    void locatesEveryPageAndBoundsItsValues() throws Exception {
        int[] values = new int[50_000];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }
        byte[] file = write(ints(RepetitionType.REQUIRED), UNCOMPRESSED, batch -> batch.ints(0, values));

        Index index = index(file, 0, 0);
        assertThat(index.offsets().pageLocations()).extracting(PageLocation::firstRowIndex)
                .containsExactly(0L, 10_000L, 20_000L, 30_000L, 40_000L);
        assertLocationsCoverTheChunk(file, index);
        ColumnIndex columns = index.columns();
        assertThat(columns.nullPages()).containsExactly(false, false, false, false, false);
        assertThat(columns.minValues()).containsExactly(le(0), le(10_000), le(20_000), le(30_000), le(40_000));
        assertThat(columns.maxValues()).containsExactly(le(9_999), le(19_999), le(29_999), le(39_999), le(49_999));
        assertThat(columns.nullCounts()).containsExactly(0, 0, 0, 0, 0);
        assertThat(columns.boundaryOrder()).isEqualTo(ColumnIndex.BoundaryOrder.ASCENDING);
        assertThat(columns.nanCounts()).isNull();
    }

    @Test
    void statesTheBoundaryOrderThePagesHave() throws Exception {
        int[] descending = new int[30_000];
        int[] unordered = new int[30_000];
        for (int i = 0; i < descending.length; i++) {
            descending[i] = -i;
            unordered[i] = (i / 10_000) == 1 ? i + 100_000 : i;
        }
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("d", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("u", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        byte[] file = write(schema, UNCOMPRESSED, batch -> batch.ints(0, descending).ints(1, unordered));

        assertThat(index(file, 0, 0).columns().boundaryOrder()).isEqualTo(ColumnIndex.BoundaryOrder.DESCENDING);
        // Bounds 0..9999, 110000..119999, 20000..29999: the middle page breaks both orders.
        assertThat(index(file, 0, 1).columns().boundaryOrder()).isEqualTo(ColumnIndex.BoundaryOrder.UNORDERED);
    }

    @Test
    void marksAPageOfNullsAndCountsEveryPagesNulls() throws Exception {
        int[] values = new int[30_000];
        boolean[] nulls = new boolean[30_000];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
            nulls[i] = i < 10_000 || i % 3 == 0;
        }
        byte[] file = write(ints(RepetitionType.OPTIONAL), UNCOMPRESSED, batch -> batch.ints(0, values, nulls));

        ColumnIndex columns = index(file, 0, 0).columns();
        assertThat(columns.nullPages()).containsExactly(true, false, false);
        assertThat(columns.minValues().getFirst()).isEmpty();
        assertThat(columns.maxValues().getFirst()).isEmpty();
        assertThat(columns.nullCounts()).containsExactly(10_000, 3_333, 3_333);
        assertThat(columns.minValues().get(1)).isEqualTo(le(10_000));
        assertThat(columns.maxValues().get(2)).isEqualTo(le(29_999));
        assertThat(columns.boundaryOrder()).isEqualTo(ColumnIndex.BoundaryOrder.ASCENDING);
    }

    @Test
    void cutsPagesOfARepeatedColumnAtRecordBoundaries() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        int records = 5_000;
        int[] offsets = new int[records + 1];
        for (int r = 0; r < records; r++) {
            offsets[r + 1] = offsets[r] + r % 7;
        }
        int[] elements = new int[offsets[records]];
        for (int e = 0; e < elements.length; e++) {
            elements[e] = e;
        }
        // A byte target that every page outgrows part-way through a record.
        WriterConfig config = WriterConfig.builder().codec(CompressionCodec.UNCOMPRESSED).pageTargetBytes(100).build();
        byte[] file = write(schema, config, batch -> batch.list("v", offsets).ints("v.list.element", elements));

        Index index = index(file, 0, 0);
        List<PageLocation> pages = index.offsets().pageLocations();
        assertThat(pages).hasSizeGreaterThan(10);
        assertLocationsCoverTheChunk(file, index);
        for (int p = 0; p < pages.size(); p++) {
            assertThat(firstRepetitionLevel(file, pages.get(p))).as("first repetition level of page %d", p).isZero();
        }
        assertThat(pages).extracting(PageLocation::firstRowIndex).isSorted().doesNotHaveDuplicates();
    }

    @Test
    void givesARecordLargerThanTheByteTargetAPageOfItsOwnAmongSmallerOnes() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        // Every fifth record holds 1,000 elements, far past the 100-byte target; the four between
        // hold three each and fit one page together.
        int records = 10;
        int[] offsets = new int[records + 1];
        for (int r = 0; r < records; r++) {
            offsets[r + 1] = offsets[r] + (r % 5 == 0 ? 1_000 : 3);
        }
        int[] elements = new int[offsets[records]];
        WriterConfig config = WriterConfig.builder().codec(CompressionCodec.UNCOMPRESSED).pageTargetBytes(100).build();
        byte[] file = write(schema, config, batch -> batch.list("v", offsets).ints("v.list.element", elements));

        Index index = index(file, 0, 0);
        assertThat(index.offsets().pageLocations()).extracting(PageLocation::firstRowIndex)
                .containsExactly(0L, 1L, 5L, 6L);
        assertLocationsCoverTheChunk(file, index);
    }

    @Test
    void countsRecordsNotValuesAgainstThePageTargetRows() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        int records = 1_000;
        int[] offsets = new int[records + 1];
        for (int r = 0; r < records; r++) {
            offsets[r + 1] = offsets[r] + 3;
        }
        int[] elements = new int[offsets[records]];
        WriterConfig config = WriterConfig.builder().codec(CompressionCodec.UNCOMPRESSED).pageTargetRows(250).build();
        byte[] file = write(schema, config, batch -> batch.list("v", offsets).ints("v.list.element", elements));

        assertThat(index(file, 0, 0).offsets().pageLocations()).extracting(PageLocation::firstRowIndex)
                .containsExactly(0L, 250L, 500L, 750L);
    }

    @Test
    void boundsDictionaryEncodedPagesByTheirEntries() throws Exception {
        String[] pool = { "delta", "alpha", "echo", "charlie", "bravo" };
        String[] values = new String[20_000];
        for (int i = 0; i < values.length; i++) {
            // The first page holds "echo", "charlie" and "bravo", the second "alpha", "echo" and
            // "charlie".
            values[i] = i < 10_000 ? pool[2 + i % 3] : pool[1 + i % 3];
        }
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("s", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        c -> c.logicalType(new LogicalType.StringType()))
                .build();
        byte[] file = write(schema, UNCOMPRESSED, batch -> batch.bytes(0, utf8(values)));

        ColumnChunk chunk = chunk(file, 0, 0);
        assertThat(chunk.metaData().dictionaryPageOffset()).isNotNull();
        ColumnIndex columns = index(file, 0, 0).columns();
        assertThat(columns.minValues()).containsExactly(utf8("bravo"), utf8("alpha"));
        assertThat(columns.maxValues()).containsExactly(utf8("echo"), utf8("echo"));
        Statistics statistics = chunk.metaData().statistics();
        assertThat(statistics.minValue()).isEqualTo(utf8("alpha"));
        assertThat(statistics.maxValue()).isEqualTo(utf8("echo"));
    }

    @Test
    void boundsBooleanPagesByTheirOwnValues() throws Exception {
        // Pages of 10,000 values start part-way into a 64-bit word of the store.
        boolean[] values = new boolean[30_000];
        for (int i = 0; i < values.length; i++) {
            values[i] = i >= 10_000 && (i < 20_000 || i % 7 == 0);
        }
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("b", PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
                .build();
        byte[] file = write(schema, UNCOMPRESSED, batch -> batch.booleans(0, values));

        ColumnIndex columns = index(file, 0, 0).columns();
        assertThat(columns.minValues()).containsExactly(new byte[] { 0 }, new byte[] { 1 }, new byte[] { 0 });
        assertThat(columns.maxValues()).containsExactly(new byte[] { 0 }, new byte[] { 1 }, new byte[] { 1 });
        assertThat(chunk(file, 0, 0).metaData().statistics().distinctCount()).isEqualTo(2);
    }

    @Test
    void countsNaNByOccurrenceOnADictionaryPage() throws Exception {
        float[] values = new float[20_000];
        for (int i = 0; i < values.length; i++) {
            values[i] = i % 3 == 0 ? Float.NaN : i % 3;
        }
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("f", PhysicalType.FLOAT, RepetitionType.REQUIRED)
                .build();
        byte[] file = write(schema, UNCOMPRESSED, batch -> batch.floats(0, values));

        assertThat(chunk(file, 0, 0).metaData().dictionaryPageOffset()).isNotNull();
        assertThat(index(file, 0, 0).columns().nanCounts()).containsExactly(3_334, 3_333);
        assertThat(chunk(file, 0, 0).metaData().statistics().nanCount()).isEqualTo(6_667);
    }

    @Test
    void boundsThePagesOfADictionaryTooLargeToTrackPerEntry() throws Exception {
        // 100,000 distinct values, each four times in a row: a dictionary that wins every probe, and
        // larger than the dictionaries whose entries a page scan tracks.
        int[] values = new int[400_000];
        for (int i = 0; i < values.length; i++) {
            values[i] = i / 4;
        }
        byte[] file = write(ints(RepetitionType.REQUIRED), UNCOMPRESSED, batch -> batch.ints(0, values));

        assertThat(chunk(file, 0, 0).metaData().dictionaryPageOffset()).isNotNull();
        ColumnIndex columns = index(file, 0, 0).columns();
        assertThat(columns.nullPages()).hasSize(40);
        for (int p = 0; p < 40; p++) {
            int[] page = Arrays.copyOfRange(values, p * 10_000, (p + 1) * 10_000);
            assertThat(columns.minValues().get(p)).as("min of page %d", p).isEqualTo(le(Arrays.stream(page).min().getAsInt()));
            assertThat(columns.maxValues().get(p)).as("max of page %d", p).isEqualTo(le(Arrays.stream(page).max().getAsInt()));
        }
    }

    @Test
    void writesNoColumnIndexForAPageOfNothingButNaN() throws Exception {
        double[] values = new double[20_000];
        for (int i = 0; i < values.length; i++) {
            values[i] = i < 10_000 ? Double.NaN : i;
        }
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("d", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .build();
        byte[] file = write(schema, UNCOMPRESSED, batch -> batch.doubles(0, values));

        ColumnChunk chunk = chunk(file, 0, 0);
        assertThat(chunk.columnIndexOffset()).isNull();
        assertThat(chunk.columnIndexLength()).isNull();
        assertThat(index(file, 0, 0).offsets().pageLocations()).hasSize(2);
    }

    @Test
    void countsEachPagesNaN() throws Exception {
        float[] values = new float[20_000];
        for (int i = 0; i < values.length; i++) {
            values[i] = i % 1_000 == 7 ? Float.NaN : i;
        }
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("f", PhysicalType.FLOAT, RepetitionType.REQUIRED)
                .build();
        byte[] file = write(schema, UNCOMPRESSED, batch -> batch.floats(0, values));

        ColumnIndex columns = index(file, 0, 0).columns();
        assertThat(columns.nanCounts()).containsExactly(10, 10);
        assertThat(columns.minValues().getFirst())
                .isEqualTo(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(-0.0f).array());
    }

    @Test
    void writesNoColumnIndexForAColumnWithoutAnOrder() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("i", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED,
                        c -> c.typeLength(12).logicalType(new LogicalType.IntervalType()))
                .build();
        byte[][] values = { new byte[12], new byte[12] };
        byte[] file = write(schema, UNCOMPRESSED, batch -> batch.fixed(0, values));

        assertThat(chunk(file, 0, 0).columnIndexOffset()).isNull();
        assertThat(index(file, 0, 0).offsets().pageLocations()).hasSize(1);
    }

    @Test
    void truncatesTextBoundsToValidUtf8() throws Exception {
        // 63 ASCII bytes and then a two-byte "é" straddling the 64-byte truncation length.
        String low = "a".repeat(63) + "é" + "z".repeat(40);
        String high = "b".repeat(63) + "é" + "z".repeat(40);
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("s", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        c -> c.logicalType(new LogicalType.StringType()))
                .build();
        byte[] file = write(schema, UNCOMPRESSED, batch -> batch.bytes(0, utf8(high, low)));

        ColumnIndex columns = index(file, 0, 0).columns();
        byte[] min = columns.minValues().getFirst();
        byte[] max = columns.maxValues().getFirst();
        assertThat(decodes(min)).isEqualTo("a".repeat(63));
        assertThat(decodes(max)).isEqualTo("b".repeat(62) + "c");
        assertThat(BinaryComparator.compareUnsigned(min, utf8(low))).isLessThanOrEqualTo(0);
        assertThat(BinaryComparator.compareUnsigned(max, utf8(high))).isGreaterThanOrEqualTo(0);
        Statistics statistics = chunk(file, 0, 0).metaData().statistics();
        assertThat(statistics.minValue()).isEqualTo(min);
        assertThat(statistics.maxValue()).isEqualTo(max);
        assertThat(statistics.isMinValueExact()).isFalse();
        assertThat(statistics.isMaxValueExact()).isFalse();
    }

    @Test
    void incrementsTheLastCodePointThatHasASuccessor() throws Exception {
        // The kept prefix ends in U+10FFFF, which has no successor, so the "é" before it is raised
        // to "ê" and the rest dropped.
        String value = "a".repeat(58) + "é" + "􏿿" + "z".repeat(10);
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("s", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        c -> c.logicalType(new LogicalType.StringType()))
                .build();
        byte[] file = write(schema, UNCOMPRESSED, batch -> batch.bytes(0, new byte[][] { utf8(value) }));

        assertThat(decodes(index(file, 0, 0).columns().maxValues().getFirst())).isEqualTo("a".repeat(58) + "ê");
    }

    /// A `STRING` column takes bytes as given, so a value may hold sequences that are not the
    /// well-formed encoding of a code point. Their "successor" can sort below them, so they are
    /// truncated as bytes, and the bound stays above the value.
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = { "C080", "E08080", "F8808080", "EDA080" })
    void keepsATextMaxAboveAValueThatIsNotWellFormedUtf8(String malformed) throws Exception {
        byte[] sequence = HexFormat.of().parseHex(malformed);
        // The sequence ends at the 64-byte truncation length, so it is the last one kept.
        byte[] value = new byte[70];
        Arrays.fill(value, (byte) 'a');
        System.arraycopy(sequence, 0, value, 64 - sequence.length, sequence.length);
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("s", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        c -> c.logicalType(new LogicalType.StringType()))
                .build();
        byte[] file = write(schema, UNCOMPRESSED, batch -> batch.bytes(0, new byte[][] { value }));

        byte[] indexMax = index(file, 0, 0).columns().maxValues().getFirst();
        assertThat(BinaryComparator.compareUnsigned(indexMax, value)).isGreaterThan(0);
        byte[] chunkMax = chunk(file, 0, 0).metaData().statistics().maxValue();
        assertThat(BinaryComparator.compareUnsigned(chunkMax, value)).isGreaterThan(0);
    }

    @Test
    void writesAPageMaxWholeWhereNoShorterUpperBoundExists() throws Exception {
        byte[] allOnes = new byte[100];
        Arrays.fill(allOnes, (byte) 0xFF);
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("b", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .build();
        byte[] file = write(schema, UNCOMPRESSED, batch -> batch.bytes(0, new byte[][] { allOnes }));

        assertThat(index(file, 0, 0).columns().maxValues().getFirst()).isEqualTo(allOnes);
        assertThat(chunk(file, 0, 0).metaData().statistics().maxValue()).isNull();
    }

    @Test
    void placesThePageIndexBetweenTheLastRowGroupAndTheFooter() throws Exception {
        int[] values = new int[30_000];
        WriterConfig config = WriterConfig.builder().codec(CompressionCodec.UNCOMPRESSED)
                .rowGroupTargetRows(10_000).build();
        byte[] file = write(ints(RepetitionType.REQUIRED), config, batch -> batch.ints(0, values));

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            List<ColumnChunk> chunks = reader.getFileMetaData().rowGroups().stream()
                    .map(rowGroup -> rowGroup.columns().getFirst()).toList();
            assertThat(chunks).hasSize(3);
            // Every ColumnIndex first, then every OffsetIndex, back to back from the end of the data.
            long at = chunkEnd(chunks.getLast());
            for (ColumnChunk chunk : chunks) {
                assertThat(chunk.columnIndexOffset()).isEqualTo(at);
                at += chunk.columnIndexLength();
            }
            for (ColumnChunk chunk : chunks) {
                assertThat(chunk.offsetIndexOffset()).isEqualTo(at);
                at += chunk.offsetIndexLength();
            }
        }
    }

    @Test
    void rejectsANonPositivePageTargetRows() {
        assertThatThrownBy(() -> WriterConfig.builder().pageTargetRows(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("pageTargetRows must be positive but was 0");
    }

    // ==================== Helpers ====================

    private record Index(ColumnChunk chunk, OffsetIndex offsets, ColumnIndex columns) {
    }

    private static FileSchema ints(RepetitionType repetition) {
        return FileSchema.builder("schema").addColumn("v", PhysicalType.INT32, repetition).build();
    }

    private static byte[] write(FileSchema schema, WriterConfig config, Consumer<ColumnBatch> filler) throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(filler);
        }
        return InMemoryFiles.toByteArray(out);
    }

    private static ColumnChunk chunk(byte[] file, int rowGroup, int column) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            return reader.getFileMetaData().rowGroups().get(rowGroup).columns().get(column);
        }
    }

    /// Reads a chunk's page index from its byte ranges, the way `hardwood dive` does.
    private static Index index(byte[] file, int rowGroup, int column) throws Exception {
        ColumnChunk chunk = chunk(file, rowGroup, column);
        OffsetIndex offsets = OffsetIndexReader.read(new ThriftCompactReader(
                slice(file, chunk.offsetIndexOffset(), chunk.offsetIndexLength())));
        ColumnIndex columns = chunk.columnIndexOffset() == null ? null
                : ColumnIndexReader.read(new ThriftCompactReader(
                        slice(file, chunk.columnIndexOffset(), chunk.columnIndexLength())));
        return new Index(chunk, offsets, columns);
    }

    private static ByteBuffer slice(byte[] file, long offset, int length) {
        return ByteBuffer.wrap(file, Math.toIntExact(offset), length).slice();
    }

    /// Each page location starts where the page before it ends, the first at the chunk's first
    /// data page and the last ending at the chunk's end, and its size covers its header.
    private static void assertLocationsCoverTheChunk(byte[] file, Index index) {
        List<PageLocation> pages = index.offsets().pageLocations();
        long at = index.chunk().metaData().dataPageOffset();
        for (PageLocation page : pages) {
            assertThat(page.offset()).isEqualTo(at);
            ThriftCompactReader reader = new ThriftCompactReader(ByteBuffer.wrap(file), Math.toIntExact(page.offset()));
            PageHeader header = PageHeaderReader.read(reader);
            assertThat(page.compressedPageSize()).isEqualTo(reader.getBytesRead() + header.compressedPageSize());
            at += page.compressedPageSize();
        }
        assertThat(at).isEqualTo(chunkEnd(index.chunk()));
    }

    private static long chunkEnd(ColumnChunk chunk) {
        Long dictionary = chunk.metaData().dictionaryPageOffset();
        long start = dictionary != null ? dictionary : chunk.metaData().dataPageOffset();
        return start + chunk.metaData().totalCompressedSize();
    }

    /// The first repetition level of an uncompressed V1 page whose column's maximum repetition
    /// level is 1. The body opens with the 4-byte length of the RLE/bit-packed repetition level
    /// stream, whose first run header — a ULEB128 varint — says whether the first value is a
    /// repeated one (its low bit clear, the value in the byte after it) or the first of a
    /// bit-packed group (the low bit of the byte after it, at one bit per level).
    private static int firstRepetitionLevel(byte[] file, PageLocation page) {
        ThriftCompactReader reader = new ThriftCompactReader(ByteBuffer.wrap(file), Math.toIntExact(page.offset()));
        PageHeaderReader.read(reader);
        int at = Math.toIntExact(page.offset() + reader.getBytesRead() + Integer.BYTES);
        int header = file[at] & 0xFF;
        while ((file[at] & 0x80) != 0) {
            at++;
        }
        int first = file[at + 1] & 0xFF;
        return (header & 1) == 0 ? first : first & 1;
    }

    private static byte[] le(int value) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[][] utf8(String... values) {
        byte[][] encoded = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            encoded[i] = utf8(values[i]);
        }
        return encoded;
    }

    /// Decodes strictly, so a bound that is not valid UTF-8 fails rather than decoding to
    /// replacement characters.
    private static String decodes(byte[] bytes) throws CharacterCodingException {
        return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString();
    }
}
