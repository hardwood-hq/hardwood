/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.zip.CRC32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnBatch;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Which row group, column and page a read failure says it happened in.
///
/// The cases here are the ones where the number the reader reports and the number a
/// person would count are easy to confuse: a pruned read, where the row groups still to
/// be read are no longer the file's own; a filtered read, where the pages still to be
/// read are not the chunk's own; and a nested column, whose leaf name is not its path.
class ReadFailurePositionTest {

    private static final Path FILTER_FIXTURE =
            Path.of("src/test/resources/filter_pushdown_int.parquet");

    /// Row group 2 of `filter_pushdown_int.parquet`, corrupted so reading it fails.
    /// `id` ascends across the file, so `id > 250` prunes row groups 0 and 1 and leaves
    /// this one first in the work list — a position that is not its index in the file.
    private static ByteBuffer withCorruptRowGroup2() throws Exception {
        byte[] bytes = Files.readAllBytes(FILTER_FIXTURE);
        long corruptOffset;
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(ByteBuffer.wrap(bytes)))) {
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().get(2).columns().get(0).metaData();
            corruptOffset = meta.dataPageOffset() + 1;
        }
        bytes[Math.toIntExact(corruptOffset)] = (byte) 0xFF;
        return ByteBuffer.wrap(bytes);
    }

    /// The row group a failure names is the file's, not the read's. A reader sent to
    /// "row group 0" of a file whose row group 0 was pruned before a byte of it was read
    /// would find nothing wrong there.
    @Test
    void aPrunedReadNamesTheRowGroupTheFileHas() throws Exception {
        ByteBuffer corrupted = withCorruptRowGroup2();

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(corrupted));
                 ColumnReader column = reader.buildColumnReader("id")
                         .filter(FilterPredicate.gt("id", 250L))
                         .build()) {
                while (column.nextBatch()) {
                    // read to the corrupt page
                }
            }
        }).isInstanceOf(ParquetReadException.class).hasMessage("[<memory>: row group 2, column 'id', page "
                                                            + "0] PageHeader has unknown page type: -1408");
    }

    /// The same, unfiltered: nothing is pruned, so the two numbers coincide and the
    /// message reads the same. Without this the case above could pass on a reader that
    /// reported the wrong number in both.
    @Test
    void anUnprunedReadNamesTheSameRowGroup() throws Exception {
        ByteBuffer corrupted = withCorruptRowGroup2();

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(corrupted));
                 ColumnReader column = reader.columnReader("id")) {
                while (column.nextBatch()) {
                    // read to the corrupt page
                }
            }
        }).isInstanceOf(ParquetReadException.class).hasMessage("[<memory>: row group 2, column 'id', page "
                                                            + "0] PageHeader has unknown page type: -1408");
    }

    /// One column failing while the others read on: `value` has no fault, so its batch holds
    /// row group 2 while `id` stops before it. The failure is what the read reports, not the
    /// batches the two columns got that far with.
    @Test
    void aRowReaderReportsOneColumnsFailure() throws Exception {
        ByteBuffer corrupted = withCorruptRowGroup2();

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(corrupted));
                 RowReader rows = reader.rowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        }).isInstanceOf(ParquetReadException.class).hasMessage("[<memory>: row group 2, column 'id', page "
                                                            + "0] PageHeader has unknown page type: -1408");
    }

    /// The same, for a group of column readers, which advance in lockstep too.
    @Test
    void columnReadersReportOneColumnsFailure() throws Exception {
        ByteBuffer corrupted = withCorruptRowGroup2();

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(corrupted));
                 ColumnReaders columns = reader.columnReaders(ColumnProjection.columns("id", "value"))) {
                while (columns.nextBatch()) {
                    // read to the corrupt page
                }
            }
        }).isInstanceOf(ParquetReadException.class).hasMessage("[<memory>: row group 2, column 'id', page "
                                                            + "0] PageHeader has unknown page type: -1408");
    }

    /// The page a failure names is its ordinal in the column chunk, not its position among
    /// the pages the filter kept. Corrupting the last of ten pages and filtering to the
    /// rows only that page holds leaves it first in the work the reader does, so a reader
    /// counting what it walked would call it page 0.
    @Test
    void aFilteredReadNamesThePageTheChunkHas() throws Exception {
        Path source = Path.of("src/test/resources/column_index_pushdown.parquet");
        byte[] bytes = Files.readAllBytes(source);

        PageLocation lastPage;
        int pageCount;
        try (InputFile in = InputFile.of(ByteBuffer.wrap(bytes));
             ParquetFileReader reader = ParquetFileReader.open(in)) {
            ColumnChunk chunk = reader.getFileMetaData().rowGroups().get(0).columns().get(0);
            OffsetIndex offsets = OffsetIndexReader.read(new ThriftCompactReader(
                    in.readRange(chunk.offsetIndexOffset(), chunk.offsetIndexLength())));
            pageCount = offsets.pageLocations().size();
            lastPage = offsets.pageLocations().get(pageCount - 1);
        }
        assertThat(pageCount).as("the fixture must have pages to drop").isGreaterThan(1);
        bytes[Math.toIntExact(lastPage.offset()) + 1] = (byte) 0xFF;

        ByteBuffer corrupted = ByteBuffer.wrap(bytes);
        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(corrupted));
                 ColumnReader column = reader.buildColumnReader("id")
                         .filter(FilterPredicate.gtEq("id", lastPage.firstRowIndex()))
                         .build()) {
                while (column.nextBatch()) {
                    // read to the corrupt page
                }
            }
        }).isInstanceOf(ParquetReadException.class).hasMessage("[<memory>: row group 0, column 'id', page "
                                                            + "9] PageHeader has unknown page type: -1408");
    }

    /// Planning a work item reads the page index, the dictionary and the chunk they sit in,
    /// and a failure in there is on no page at all. The plan being walked when it happens
    /// belongs to the row group before it, so a reader that answered "which page" from it
    /// would name one that read cleanly.
    ///
    /// A file not in memory plans its row groups on a virtual thread, whose failure the column
    /// takes when it reaches the row group; it names the row group as planning on the column does.
    @ParameterizedTest(name = "in memory: {0}")
    @ValueSource(booleans = { true, false })
    void aFailureWhilePlanningNamesNoPage(boolean inMemory) throws Exception {
        Path source = Path.of("src/test/resources/multi_row_group_page_index.parquet");
        byte[] bytes = Files.readAllBytes(source);
        long columnIndexOffset;
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(ByteBuffer.wrap(bytes)))) {
            assertThat(reader.getFileMetaData().rowGroups())
                    .as("the fixture must have a row group to plan after the first")
                    .hasSizeGreaterThan(2);
            columnIndexOffset = reader.getFileMetaData().rowGroups().get(2).columns().get(0)
                    .columnIndexOffset();
        }
        bytes[Math.toIntExact(columnIndexOffset)] = (byte) 0xFF;
        bytes[Math.toIntExact(columnIndexOffset) + 1] = (byte) 0xFF;

        ByteBuffer corrupted = ByteBuffer.wrap(bytes);
        InputFile file = inMemory ? InputFile.of(corrupted) : new CountingInputFile(InputFile.of(corrupted));
        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(file);
                 // One value inside each row group, so statistics decide none of them and
                 // every row group's page index is evaluated.
                 ColumnReader column = reader.buildColumnReader("id")
                         .filter(FilterPredicate.in("id", 500L, 1500L, 2500L))
                         .build()) {
                while (column.nextBatch()) {
                    // read as far as the row group whose page index is corrupt
                }
            }
        }).isInstanceOf(ParquetReadException.class).hasMessage("[<memory>: row group 2, column 'id'] "
                                                            + "Failed to parse the page index of column "
                                                            + "0: ColumnIndex field 15 \u2014 Unknown "
                                                            + "field type: 15");
    }

    /// A column's path, not its leaf name: two leaves in one file can share a name, and
    /// the accessor is documented to tell them apart.
    @Test
    void aNestedColumnIsNamedByItsPath() throws Exception {
        Path source = Path.of("src/test/resources/nested_struct_test.parquet");
        byte[] bytes = Files.readAllBytes(source);
        long corruptOffset;
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(ByteBuffer.wrap(bytes)))) {
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().get(0).columns().get(2).metaData();
            corruptOffset = meta.dataPageOffset() + 1;
        }
        bytes[Math.toIntExact(corruptOffset)] = (byte) 0xFF;

        ByteBuffer corrupted = ByteBuffer.wrap(bytes);
        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(corrupted));
                 ColumnReader column = reader.columnReader("address.city")) {
                while (column.nextBatch()) {
                    // read to the corrupt page
                }
            }
        }).isInstanceOf(ParquetReadException.class).hasMessage("[<memory>: row group 0, column "
                                                            + "'address.city', page 0] PageHeader has "
                                                            + "unknown page type: -1408");
    }

    /// A `DELTA_BINARY_PACKED` page's blocks are decoded by the fill of the cell the page is placed
    /// into, which runs once the cell's last page is read. A block running past its page fails
    /// that fill, and the failure names the page the block is on, not the cell's last page.
    @Test
    void aDeltaBlockFailureInANestedCellNamesItsOwnPage() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT64, RepetitionType.REQUIRED))
                .build();
        int records = 600;
        int[] offsets = new int[records + 1];
        long[] elements = new long[records * 3];
        int count = 0;
        for (int r = 0; r < records; r++) {
            for (int k = 0; k < 1 + r % 3; k++) {
                elements[count++] = r * 1_000L + (r * 7_919L + k * 31L) % 997;
            }
            offsets[r + 1] = count;
        }
        long[] values = Arrays.copyOf(elements, count);
        byte[] file = write(schema, ColumnEncoding.DELTA_BINARY_PACKED, batch -> batch.list("v", offsets).longs("v.list.element", values));
        String failure = corruptFirstDeltaBlock(file, 1, 1);

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                 ColumnReader column = reader.columnReader("v.list.element")) {
                while (column.nextBatch()) {
                    // read to the corrupt page
                }
            }
        }).isInstanceOf(ParquetReadException.class).hasMessage("[<memory>: row group 0, column "
                                                            + "'v.list.element', page 0] " + failure);
    }

    /// The same for a flat column, whose pages fill their segments on their own tasks.
    @Test
    void aDeltaBlockFailureInAFlatCellNamesItsOwnPage() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        long[] values = new long[600];
        for (int i = 0; i < values.length; i++) {
            values[i] = (i * 7_919L) % 997;
        }
        byte[] file = write(schema, ColumnEncoding.DELTA_BINARY_PACKED, batch -> batch.longs("v", values));
        String failure = corruptFirstDeltaBlock(file, 0, 0);

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                 ColumnReader column = reader.columnReader("v")) {
                while (column.nextBatch()) {
                    // read to the corrupt page
                }
            }
        }).isInstanceOf(ParquetReadException.class).hasMessage("[<memory>: row group 0, column 'v', page 0] "
                                                            + failure);
    }

    /// A page no reader takes, here `RLE` declared over an `INT32` column, is refused on the
    /// scheduled path with the page's place.
    @Test
    void aRefusedPageNamesItsPlace() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        int[] values = new int[60];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }
        byte[] file = write(schema, ColumnEncoding.PLAIN, batch -> batch.ints("v", values));
        long pageOffset;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            pageOffset = reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData().dataPageOffset();
        }
        // The data page header's num_values (20) and encoding (PLAIN, 0) fields, the encoding
        // replaced by RLE (3), both zigzag-encoded: 40 and 6
        int headerStart = Math.toIntExact(pageOffset);
        int encodingAt = indexOf(file, headerStart, headerStart + 64, new byte[]{ 0x15, 40, 0x15, 0 }) + 3;
        file[encodingAt] = 6;

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                 ColumnReader column = reader.columnReader("v")) {
                while (column.nextBatch()) {
                    // read to the refused page
                }
            }
        }).isInstanceOf(ParquetReadException.class).hasMessage("[<memory>: row group 0, column 'v', page 0] "
                                                            + "RLE encodes only boolean values in a data page, not INT32");
    }

    /// One row group of uncompressed pages of 20 records each in `encoding`.
    private static byte[] write(FileSchema schema, ColumnEncoding encoding, Consumer<ColumnBatch> batch)
            throws Exception {
        WriterConfig config = WriterConfig.builder()
                .codec(CompressionCodec.UNCOMPRESSED)
                .encoding(encoding)
                .pageTargetRows(20)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch);
        }
        return InMemoryFiles.toByteArray(out);
    }

    /// Widens the first miniblock of the first data page's first delta block of the file's only
    /// column so that it runs past the page, and patches the page's CRC to match; returns the
    /// message the decoder raises for it. The width is the widest whose CRC encodes in as many
    /// bytes as the original, so the header keeps its length.
    private static String corruptFirstDeltaBlock(byte[] file, int maxRepetitionLevel, int maxDefinitionLevel)
            throws Exception {
        long pageOffset;
        int pageCount;
        try (InputFile in = InputFile.of(ByteBuffer.wrap(file));
             ParquetFileReader reader = ParquetFileReader.open(in)) {
            ColumnChunk chunk = reader.getFileMetaData().rowGroups().get(0).columns().get(0);
            pageOffset = chunk.metaData().dataPageOffset();
            pageCount = OffsetIndexReader.read(new ThriftCompactReader(
                    in.readRange(chunk.offsetIndexOffset(), chunk.offsetIndexLength()))).pageLocations().size();
        }
        assertThat(pageCount).as("the corrupt page must not be the chunk's last").isGreaterThan(2);

        int headerStart = Math.toIntExact(pageOffset);
        ThriftCompactReader headerReader = new ThriftCompactReader(ByteBuffer.wrap(file), headerStart);
        PageHeader header = PageHeaderReader.read(headerReader);
        int body = headerStart + headerReader.getBytesRead();
        int bodyEnd = body + header.compressedPageSize();
        int[] pos = { body };
        if (maxRepetitionLevel > 0) {
            pos[0] += 4 + ByteBuffer.wrap(file, pos[0], 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        }
        if (maxDefinitionLevel > 0) {
            pos[0] += 4 + ByteBuffer.wrap(file, pos[0], 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        }
        long blockSize = uleb(file, pos);
        long miniblocks = uleb(file, pos);
        uleb(file, pos); // total value count
        uleb(file, pos); // first value
        uleb(file, pos); // the block's minimum delta
        int widthAt = pos[0];
        long remaining = bodyEnd - widthAt - miniblocks;
        byte[] oldCrc = zigzagVarint(header.crc());
        int crcAt = indexOf(file, headerStart, body, oldCrc);
        for (int width = 64; width > 0; width--) {
            long bytes = ((blockSize / miniblocks) * width + 7) / 8;
            assertThat(bytes).as("a width that runs past the page").isGreaterThan(remaining);
            file[widthAt] = (byte) width;
            CRC32 crc = new CRC32();
            crc.update(file, body, bodyEnd - body);
            byte[] newCrc = zigzagVarint((int) crc.getValue());
            if (newCrc.length == oldCrc.length) {
                System.arraycopy(newCrc, 0, file, crcAt, newCrc.length);
                return "Unexpected EOF reading miniblock data: expected " + bytes + " bytes, got " + remaining;
            }
        }
        throw new IllegalStateException("No width keeps the CRC's length");
    }

    private static long uleb(byte[] data, int[] pos) {
        long result = 0;
        int shift = 0;
        int b;
        do {
            b = data[pos[0]++] & 0xFF;
            result |= (long) (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }

    /// A Thrift compact `i32`: zigzag, then an unsigned varint.
    private static byte[] zigzagVarint(int value) {
        long zigzag = Integer.toUnsignedLong((value << 1) ^ (value >> 31));
        byte[] bytes = new byte[5];
        int length = 0;
        while ((zigzag & ~0x7FL) != 0) {
            bytes[length++] = (byte) ((zigzag & 0x7F) | 0x80);
            zigzag >>>= 7;
        }
        bytes[length++] = (byte) zigzag;
        return Arrays.copyOf(bytes, length);
    }

    private static int indexOf(byte[] data, int from, int to, byte[] pattern) {
        for (int i = from; i + pattern.length <= to; i++) {
            if (Arrays.equals(data, i, i + pattern.length, pattern, 0, pattern.length)) {
                return i;
            }
        }
        throw new IllegalStateException("Pattern not found in the page header");
    }
}
