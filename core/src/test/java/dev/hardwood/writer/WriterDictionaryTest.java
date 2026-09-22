/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageEncodingStats;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.writer.WriterTestSupport.columnMeta;
import static dev.hardwood.writer.WriterTestSupport.countDataPages;
import static dev.hardwood.writer.WriterTestSupport.mapOf;
import static dev.hardwood.writer.WriterTestSupport.oneColumn;
import static dev.hardwood.writer.WriterTestSupport.oneOptionalColumn;
import static dev.hardwood.writer.WriterTestSupport.readInts;
import static dev.hardwood.writer.WriterTestSupport.readListOfInts;
import static dev.hardwood.writer.WriterTestSupport.readMapOfInts;
import static dev.hardwood.writer.WriterTestSupport.readNullable;
import static org.assertj.core.api.Assertions.assertThat;

/// Which encoding a column chunk is written in, and what the chunk then carries.
///
/// The choice is made once per chunk from the values it holds, as
/// `_designs-legacy/WRITER_DICTIONARY_SELECTION.md` describes, so these pin both ends of it — a chunk
/// that argues for a dictionary and one that argues against — across the repetition shapes and
/// the nested layouts, plus the analysis cap that abandons a dictionary to bound memory rather
/// than to decide an encoding.
class WriterDictionaryTest {

    @Test
    void dictionaryEncodesLowCardinalityColumn() throws Exception {
        // 1000 rows drawn from four distinct values: a small dictionary, narrow indices.
        int n = 1_000;
        int[] values = new int[n];
        int[] palette = { 7, 42, -3, 1_000_000 };
        for (int i = 0; i < n; i++) {
            values[i] = palette[i % palette.length];
        }

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }

        byte[] file = out.toByteArray();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            ColumnMetaData meta = columnMeta(reader, 0);
            int dataPages = countDataPages(file, meta.dataPageOffset(), meta.numValues());
            assertThat(meta.dictionaryPageOffset()).as("dictionary page written").isNotNull();
            assertThat(meta.encodings()).contains(Encoding.RLE_DICTIONARY, Encoding.PLAIN);
            assertThat(meta.encodingStats()).containsExactly(
                    new PageEncodingStats(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1),
                    new PageEncodingStats(PageType.DATA_PAGE, Encoding.RLE_DICTIONARY, dataPages));
            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();
        }
    }

    @Test
    void dictionaryIsGivenUpWhenItOutgrowsTheAnalysisCap() throws Exception {
        // The cap is max(rowGroupBufferTargetBytes / 2, 1 MiB), so a 4 MiB target caps the dictionary at
        // 2 MiB — reached after 524,288 distinct INT32 values, part-way through this column. From
        // there the chunk holds resolved values followed by directly appended ones, which is the
        // one path where the value store carries both, and the only path that leaves the chunk
        // unable to state its cardinality.
        int n = 900_000;
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = i * 3 + 7;
        }

        WriterConfig config = WriterConfig.builder().rowGroupBufferTargetBytes(4L << 20).build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(1);
            ColumnMetaData meta = columnMeta(reader, 0);
            assertThat(meta.dictionaryPageOffset()).as("no dictionary page").isNull();
            assertThat(meta.encodings()).doesNotContain(Encoding.RLE_DICTIONARY);
            // The count is unknown once the dictionary is gone, and absent rather than estimated.
            assertThat(meta.statistics().distinctCount()).as("distinct_count").isNull();
            // Every value survives the switch from interned to stored, in order.
            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();
        }
    }

    @Test
    void writesExactDistinctCountWhereverTheChunkKnowsIt() throws Exception {
        // The count is exact for as long as the chunk holds a dictionary, so it is written
        // whichever encoding wins and left out where the chunk never built one. These columns are
        // shorter than the first probe, so none of them gives its dictionary up early; a chunk
        // that does states no count either, which WriterDictionaryProbeTest covers.
        int n = 1_000;
        int[] repeating = new int[n];
        int[] allDistinct = new int[n];
        for (int i = 0; i < n; i++) {
            repeating[i] = i % 8;
            allDistinct[i] = i * 31 + 5;
        }

        assertThat(distinctCountOf(repeating, WriterConfig.defaults()))
                .as("dictionary-encoded chunk").isEqualTo(8L);
        assertThat(distinctCountOf(allDistinct, WriterConfig.defaults()))
                .as("chunk the comparison sent to PLAIN").isEqualTo((long) n);
        assertThat(distinctCountOf(repeating, WriterConfig.builder().encoding(ColumnEncoding.PLAIN).build()))
                .as("chunk that never built a dictionary").isNull();

        // A BOOLEAN column is never dictionary-encoded and still knows its cardinality: at most
        // false and true can occur.
        FileSchema booleans = FileSchema.builder("m")
                .addColumn("b", PhysicalType.BOOLEAN, RepetitionType.REQUIRED).build();
        boolean[] both = new boolean[n];
        boolean[] onlyFalse = new boolean[n];
        for (int i = 0; i < n; i++) {
            both[i] = i % 2 == 0;
        }
        assertThat(booleanDistinctCount(booleans, both)).as("BOOLEAN with both values").isEqualTo(2L);
        assertThat(booleanDistinctCount(booleans, onlyFalse)).as("BOOLEAN with one value").isEqualTo(1L);
    }

    @Test
    void allDistinctColumnIsWrittenPlain() throws Exception {
        // Every value distinct: a dictionary would cost its own page plus an index per value,
        // where the values alone are smaller. The chunk is PLAIN throughout and carries no
        // dictionary page at all.
        int n = 1_000;
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = i * 31 + 5;
        }

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }

        byte[] file = out.toByteArray();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            ColumnMetaData meta = columnMeta(reader, 0);
            int dataPages = countDataPages(file, meta.dataPageOffset(), meta.numValues());
            assertThat(meta.dictionaryPageOffset()).isNull();
            assertThat(meta.encodings()).doesNotContain(Encoding.RLE_DICTIONARY);
            assertThat(meta.encodingStats()).containsExactly(
                    new PageEncodingStats(PageType.DATA_PAGE, Encoding.PLAIN, dataPages));
            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();
        }
    }

    @Test
    void repeatingColumnIsDictionaryEncoded() throws Exception {
        // The mirror image: few distinct values over many rows, where the dictionary plus a
        // narrow index stream is far smaller than the values.
        int n = 1_000;
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = i % 8;
        }

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }

        byte[] file = out.toByteArray();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            ColumnMetaData meta = columnMeta(reader, 0);
            assertThat(meta.dictionaryPageOffset()).isNotNull();
            assertThat(meta.encodings()).contains(Encoding.RLE_DICTIONARY);
            assertThat(meta.encodings()).doesNotContain(Encoding.PLAIN_DICTIONARY);
            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();
        }
        // A dictionary of eight ints and 3-bit indices against 4 KB of values.
        assertThat(file.length).isLessThan(n * Integer.BYTES / 2);
    }

    @Test
    void dictionaryEncodesSingleDistinctValue() throws Exception {
        // Every row the same value: a one-entry dictionary and a zero-bit index stream.
        int[] values = new int[500];
        Arrays.fill(values, -99);

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }

        byte[] file = out.toByteArray();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            assertThat(columnMeta(reader, 0).dictionaryPageOffset()).isNotNull();
            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();

            // The index stream is more than its bit-width byte: a zero-bit run still carries
            // its header, without which parquet-java and Arrow C++ read past the end (#901).
            // Hardwood's own decoder short-circuits on the bit width, so the round trip above
            // passes either way.
            assertThat(indexStreamLength(file, columnMeta(reader, 0).dataPageOffset())).isPositive();
        }
    }

    @Test
    void dictionaryEncodesNullableColumn() throws Exception {
        // Low cardinality with interior nulls: only present rows carry an index.
        int[] values = { 5, 0, 5, 0, 9, 0, 5, 9 };
        boolean[] nulls = { false, true, false, true, false, true, false, false };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(columnMeta(reader, 0).dictionaryPageOffset()).isNotNull();
            assertThat(readNullable(reader, 0)).containsExactly(5, null, 5, null, 9, null, 5, 9);
        }
    }

    @Test
    void allNullColumnWritesNoDictionaryPage() throws Exception {
        // No present values, so the dictionary stays empty and no dictionary page is written.
        boolean[] nulls = { true, true, true, true };
        int[] values = new int[nulls.length];

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values, nulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            ColumnMetaData meta = columnMeta(reader, 0);
            assertThat(meta.dictionaryPageOffset()).isNull();
            assertThat(meta.encodings()).doesNotContain(Encoding.RLE_DICTIONARY);
            assertThat(readNullable(reader, 0)).containsExactly(null, null, null, null);
        }
    }

    @Test
    void disablingDictionaryWritesPlainPages() throws Exception {
        // With dictionary disabled, no dictionary page and PLAIN data pages — the pre-stage-9
        // layout — even for a column the dictionary would otherwise encode.
        int[] values = new int[200];
        for (int i = 0; i < values.length; i++) {
            values[i] = i % 3;
        }

        WriterConfig config = WriterConfig.builder().encoding(ColumnEncoding.PLAIN).build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            ColumnMetaData meta = columnMeta(reader, 0);
            assertThat(meta.dictionaryPageOffset()).isNull();
            assertThat(meta.encodings()).containsExactly(Encoding.PLAIN);
            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();
        }
    }

    @Test
    void dictionaryEncodesListColumn() throws Exception {
        // A LIST<INT32> of low cardinality: the index value section sits behind the rep/def
        // level streams, proving dictionary encoding composes with repetition.
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] offsets = { 0, 2, 2, 2, 5 };
        Validity listNulls = Validity.ofNulls(new boolean[] { false, false, true, false });
        int[] elements = { 8, 8, 8, 0, 3 };
        boolean[] elementNulls = { false, false, false, true, false };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .list("v", offsets, listNulls)
                    .ints("v.list.element", elements, elementNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            int leaf = reader.getFileSchema().getColumn("v.list.element").columnIndex();
            assertThat(columnMeta(reader, leaf).dictionaryPageOffset()).isNotNull();
            assertThat(readListOfInts(reader, leaf))
                    .containsExactly(List.of(8, 8), List.of(), null, Arrays.asList(8, null, 3));
        }
    }

    @Test
    void dictionaryValuesSurvivePageAndRowGroupBoundaries() throws Exception {
        // Low cardinality over many rows with tiny page and row-group targets, so dictionary
        // index pages straddle both boundaries and each row group builds its own dictionary.
        int n = 4_000;
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = (i % 5) * 100;
        }

        WriterConfig config = WriterConfig.builder().pageTargetBytes(64).rowGroupBufferTargetBytes(512).build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(reader.getFileMetaData().rowGroups().size()).isGreaterThan(1);
            assertThat(columnMeta(reader, 0).dictionaryPageOffset()).isNotNull();
            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();
        }
    }

    @Test
    void dictionaryEncodesColumnWithLeadingNullPage() throws Exception {
        // Flush-time encoding selection applies to the complete chunk, so a leading null-only
        // page remains RLE_DICTIONARY data even though it precedes the present dictionary values.
        int n = 60;
        int leadingNulls = 24; // exceeds the ~15 level entries a 64-byte page holds for INT32
        int[] values = new int[n];
        boolean[] nulls = new boolean[n];
        Integer[] expected = new Integer[n];
        for (int i = 0; i < n; i++) {
            if (i < leadingNulls) {
                nulls[i] = true;
                expected[i] = null;
            } else {
                values[i] = (i % 3) * 100;
                expected[i] = values[i];
            }
        }

        WriterConfig config = WriterConfig.builder().pageTargetBytes(64).build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneOptionalColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values, nulls));
        }

        byte[] file = out.toByteArray();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            ColumnMetaData meta = columnMeta(reader, 0);
            int dataPages = countDataPages(file, meta.dataPageOffset(), meta.numValues());
            assertThat(meta.dictionaryPageOffset()).as("dictionary page still written").isNotNull();
            assertThat(meta.encodings()).contains(Encoding.RLE_DICTIONARY, Encoding.PLAIN);
            assertThat(meta.encodingStats()).containsExactly(
                    new PageEncodingStats(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1),
                    new PageEncodingStats(PageType.DATA_PAGE, Encoding.RLE_DICTIONARY, dataPages));
            assertThat(readNullable(reader, 0)).containsExactly(expected);
        }
    }

    @Test
    void dictionaryEncodesStructColumn() throws Exception {
        // A low-cardinality INT32 leaf inside an OPTIONAL struct: the dictionary index section
        // sits behind the struct's definition-level stream, proving dictionary encoding composes
        // with a STRUCT layer.
        FileSchema schema = FileSchema.builder("schema")
                .struct("s", RepetitionType.OPTIONAL, sb -> sb
                        .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        // structs: {v:7}, null struct, {v:null}, {v:7}, {v:3} — the null-struct slot is a phantom.
        Validity structNulls = Validity.ofNulls(new boolean[] { false, true, false, false, false });
        int[] v = { 7, 0, 0, 7, 3 };
        boolean[] vNulls = { false, false, true, false, false };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .struct("s", structNulls)
                    .ints("s.v", v, vNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            int leaf = reader.getFileSchema().getColumn("s.v").columnIndex();
            assertThat(columnMeta(reader, leaf).dictionaryPageOffset()).isNotNull();
            assertThat(readNullable(reader, leaf)).containsExactly(7, null, null, 7, 3);
        }
    }

    @Test
    void dictionaryEncodesMapValueColumn() throws Exception {
        // A low-cardinality INT32 map value: the dictionary index section sits behind the MAP's
        // rep/def level streams, proving dictionary encoding composes with a MAP layer.
        FileSchema schema = FileSchema.builder("schema")
                .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        v -> v.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] offsets = { 0, 2, 2, 2, 4 };
        Validity mapNulls = Validity.ofNulls(new boolean[] { false, false, true, false });
        int[] keys = { 1, 2, 3, 4 };
        int[] values = { 5, 0, 5, 9 };
        boolean[] valueNulls = { false, true, false, false };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .map("props", offsets, mapNulls)
                    .ints("props.key_value.key", keys)
                    .ints("props.key_value.value", values, valueNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            int keyIdx = reader.getFileSchema().getColumn("props.key_value.key").columnIndex();
            int valIdx = reader.getFileSchema().getColumn("props.key_value.value").columnIndex();
            assertThat(columnMeta(reader, valIdx).dictionaryPageOffset()).isNotNull();
            try (ColumnReader kr = reader.columnReader(keyIdx); ColumnReader vr = reader.columnReader(valIdx)) {
                assertThat(kr.nextBatch()).isTrue();
                assertThat(vr.nextBatch()).isTrue();
                assertThat(readMapOfInts(kr, vr)).containsExactly(
                        mapOf(1, 5, 2, null), Map.of(), null, mapOf(3, 5, 4, 9));
            }
        }
    }

    private static Long booleanDistinctCount(FileSchema schema, boolean[] values) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.booleans(0, values));
        }
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            return columnMeta(reader, 0).statistics().distinctCount();
        }
    }

    /// The `distinct_count` a one-column file's only chunk carries, or null where it carries none.
    private static Long distinctCountOf(int[] values, WriterConfig config) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();
            return columnMeta(reader, 0).statistics().distinctCount();
        }
    }

    /// Length in bytes of the RLE index stream of the first data page at `dataPageOffset`,
    /// which for an unlevelled `RLE_DICTIONARY` page is the uncompressed body past its leading
    /// bit-width byte. Taken from the page header, so the chunk's codec does not matter.
    private static int indexStreamLength(byte[] file, long dataPageOffset) throws Exception {
        ThriftCompactReader reader = new ThriftCompactReader(ByteBuffer.wrap(file),
                Math.toIntExact(dataPageOffset));
        PageHeader header = PageHeaderReader.read(reader);
        assertThat(header.dataPageHeader().encoding()).isEqualTo(Encoding.RLE_DICTIONARY);
        return header.uncompressedPageSize() - 1;
    }
}
