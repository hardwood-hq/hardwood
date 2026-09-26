/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.compression.CompressorFactory;
import dev.hardwood.internal.thrift.FileMetaDataWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A column chunk accumulates its row group in `int`-indexed stores, and [RowGroupBuffer#append]
/// cuts a row group before any of them would overflow, whatever the byte target says. The real
/// cap is two billion entries or two gigabytes of values per chunk, out of a test's reach, so
/// these lower it through the package-private constructor and hold the byte target far above it.
class RowGroupBufferStoreCapacityTest {

    private static final long LARGE_TARGET_BYTES = 1L << 30;
    private static final long DEFAULT_TARGET_ROWS = 1 << 20;

    @Test
    void rowGroupIsCutBeforeByteArrayContentOverflowsItsStore() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("s", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .build();
        byte[][] values = distinctValues(1000, 100);

        Buffered buffered = new Buffered(schema, DEFAULT_TARGET_ROWS, 10_000);
        buffered.append(Map.of(), new BinaryArrayColumnSource(values));

        assertThat(buffered.readBinaries()).isDeepEqualTo(values);
        // 10,000 bytes of content hold exactly 100 of the 100-byte values.
        assertThat(buffered.rowCounts()).containsExactlyElementsOf(Collections.nCopies(10, 100L));
    }

    @Test
    void rowGroupIsCutBeforeFixedLengthContentOverflowsItsStore() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("f", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 16)
                .build();
        byte[][] values = distinctValues(400, 16);

        Buffered buffered = new Buffered(schema, DEFAULT_TARGET_ROWS, 1000);
        buffered.append(Map.of(), new BinaryArrayColumnSource(values));

        // 1,000 bytes of content hold 62 of the 16-byte values, far below the 999 entries.
        List<Long> rowCounts = new ArrayList<>(Collections.nCopies(6, 62L));
        rowCounts.add(28L);
        assertThat(buffered.readBinaries()).isDeepEqualTo(values);
        assertThat(buffered.rowCounts()).containsExactlyElementsOf(rowCounts);
    }

    @Test
    void rowGroupIsCutBeforeLevelEntriesOverflowTheirStore() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        int records = 1000;
        int perRecord = 10;
        int[] offsets = new int[records + 1];
        int[] elements = new int[records * perRecord];
        for (int r = 0; r < records; r++) {
            offsets[r + 1] = offsets[r] + perRecord;
        }
        for (int i = 0; i < elements.length; i++) {
            elements[i] = i;
        }

        Buffered buffered = new Buffered(schema, DEFAULT_TARGET_ROWS, 1000);
        buffered.append(Map.of("v", offsets), new IntArrayColumnSource(elements));

        // A store of 1000 holds 999 entries, the value offsets keeping a trailing bound past the
        // last, so a group takes 99 records of 10 entries and not the 100th.
        List<Long> rowCounts = new ArrayList<>(Collections.nCopies(10, 99L));
        rowCounts.add(10L);
        assertThat(buffered.readInts()).isEqualTo(elements);
        assertThat(buffered.rowCounts()).containsExactlyElementsOf(rowCounts);
    }

    @Test
    void flatColumnIsCutAtTheRowCeilingItsEntryCapMeets() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        int[] values = new int[2500];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }

        Buffered buffered = new Buffered(schema, Long.MAX_VALUE, 1000);
        // A store of 1000 holds 999 entries, one per record of a flat column, and the row ceiling
        // is those 999 records: the group is full on both counts at once, and the row ceiling
        // closes it as the 999th record goes in rather than the entry cap as the 1000th is
        // offered.
        buffered.append(Map.of(), new IntArrayColumnSource(Arrays.copyOf(values, 999)));
        assertThat(buffered.rowCounts()).containsExactly(999L);
        assertThat(buffered.buffer.isEmpty()).isTrue();

        buffered.append(Map.of(), new IntArrayColumnSource(Arrays.copyOfRange(values, 999, values.length)));
        assertThat(buffered.readInts()).isEqualTo(values);
        assertThat(buffered.rowCounts()).containsExactly(999L, 999L, 502L);
    }

    @Test
    void dictionaryTooLargeForAPageIsNotChosen() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        // 300 distinct values, each three times: a dictionary would win on size, but its body is
        // 2,400 bytes, past a store capacity of 1,000.
        long[] values = new long[900];
        for (int i = 0; i < values.length; i++) {
            values[i] = i % 300;
        }

        Buffered buffered = new Buffered(schema, DEFAULT_TARGET_ROWS, 1000);
        buffered.append(Map.of(), new LongArrayColumnSource(values));

        assertThat(buffered.readLongs()).isEqualTo(values);
        assertThat(buffered.rowGroups).hasSize(1);
        ColumnMetaData chunk = buffered.rowGroups.getFirst().columns().getFirst().metaData();
        assertThat(chunk.dictionaryPageOffset()).isNull();
        assertThat(chunk.encodings()).doesNotContain(Encoding.RLE_DICTIONARY);
    }

    @Test
    void recordLargerThanAStoreCanHoldIsRejected() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED,
                        el -> el.primitive(PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED))
                .build();
        // The first record fits a group; the second's three values are 1,200 bytes on their own.
        int[] offsets = { 0, 1, 4 };
        byte[][] elements = { new byte[10], new byte[400], new byte[400], new byte[400] };

        Buffered buffered = new Buffered(schema, DEFAULT_TARGET_ROWS, 1000);
        assertThatThrownBy(() -> buffered.append(Map.of("v", offsets), new BinaryArrayColumnSource(elements)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A record is too large for a row group: its values for column 'v.list.element'"
                        + " take up to 4 entries and 1200 bytes, and a column chunk holds at most 999 entries"
                        + " and 1000 bytes of values");
        // The first record's group is cut to make room before the second is found not to fit.
        assertThat(buffered.rowCounts()).containsExactly(1L);
    }

    @Test
    void flushThatLeavesRecordsBufferedFailsTheAppend() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();

        Buffered buffered = new Buffered(schema, 10, 1000);
        ColumnSource[] sources = { new IntArrayColumnSource(new int[25]) };
        buffered.shredder.bind(sources, new Validity[1], Map.of(), Map.of(), Map.of());

        // A flush that does not reset would otherwise have the row target cut the group again
        // and again without the append ever advancing.
        RowGroupBuffer.Flush keepsRecords = () -> {
        };
        assertThatThrownBy(() -> buffered.buffer.append(buffered.shredder, sources, keepsRecords))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("A row group flush left 10 records buffered");
    }

    private static byte[][] distinctValues(int count, int length) {
        byte[][] values = new byte[count][];
        for (int i = 0; i < count; i++) {
            values[i] = new byte[length];
            Arrays.fill(values[i], (byte) i);
        }
        return values;
    }

    /// A [RowGroupBuffer] under a lowered store capacity, flushing each row group it cuts into a
    /// Parquet file in memory. Reading the values back flushes the last row group and completes
    /// the file, so a test reads them once, before it asserts on the row counts.
    private static final class Buffered {

        private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);

        private final FileSchema schema;
        private final RecordShredder shredder;
        private final RowGroupBuffer buffer;
        private final ByteBufferOutputFile out = new ByteBufferOutputFile();
        private final List<RowGroup> rowGroups = new ArrayList<>();

        Buffered(FileSchema schema, long targetRows, int storeCapacity) {
            this.schema = schema;
            this.shredder = new RecordShredder(schema);
            ColumnEncoding[] encodings = new ColumnEncoding[schema.getColumnCount()];
            Arrays.fill(encodings, ColumnEncoding.AUTO);
            this.buffer = new RowGroupBuffer(schema, 1 << 20, LARGE_TARGET_BYTES, targetRows, encodings, 64,
                    new CompressorFactory().getCompressor(CompressionCodec.UNCOMPRESSED),
                    CompressionCodec.UNCOMPRESSED, storeCapacity);
            out.create();
            out.write(ByteBuffer.wrap(MAGIC));
        }

        /// Appends one batch of a single-column schema, whose repeated layers take `listOffsets`.
        void append(Map<String, int[]> listOffsets, ColumnSource source) throws IOException {
            ColumnSource[] sources = { source };
            shredder.bind(sources, new Validity[1], Map.of(), Map.of(), listOffsets);
            buffer.append(shredder, sources, this::flush);
        }

        void flush() throws IOException {
            if (buffer.isEmpty()) {
                return;
            }
            rowGroups.add(buffer.flushTo(out));
            buffer.reset();
        }

        List<Long> rowCounts() {
            return rowGroups.stream().map(RowGroup::numRows).toList();
        }

        byte[][] readBinaries() throws IOException {
            List<byte[]> all = new ArrayList<>();
            try (ParquetFileReader reader = open(); ColumnReader column = reader.columnReader(0)) {
                while (column.nextBatch()) {
                    all.addAll(Arrays.asList(column.getBinaries()).subList(0, column.getValueCount()));
                }
            }
            return all.toArray(new byte[0][]);
        }

        int[] readInts() throws IOException {
            int[] all = new int[0];
            try (ParquetFileReader reader = open(); ColumnReader column = reader.columnReader(0)) {
                while (column.nextBatch()) {
                    int at = all.length;
                    all = Arrays.copyOf(all, at + column.getValueCount());
                    System.arraycopy(column.getInts(), 0, all, at, column.getValueCount());
                }
            }
            return all;
        }

        long[] readLongs() throws IOException {
            long[] all = new long[0];
            try (ParquetFileReader reader = open(); ColumnReader column = reader.columnReader(0)) {
                while (column.nextBatch()) {
                    int at = all.length;
                    all = Arrays.copyOf(all, at + column.getValueCount());
                    System.arraycopy(column.getLongs(), 0, all, at, column.getValueCount());
                }
            }
            return all;
        }

        /// Flushes what is still buffered, writes the footer and opens the file.
        private ParquetFileReader open() throws IOException {
            flush();
            long numRows = rowGroups.stream().mapToLong(RowGroup::numRows).sum();
            FileMetaData metaData = new FileMetaData(1, schema.toSchemaElements(), numRows, List.copyOf(rowGroups),
                    Map.of(), "test", Collections.nCopies(schema.getColumnCount(), ColumnOrder.TYPE_DEFINED_ORDER));
            ThriftCompactWriter footer = new ThriftCompactWriter();
            FileMetaDataWriter.write(footer, metaData);
            byte[] footerBytes = footer.toByteArray();
            out.write(ByteBuffer.wrap(footerBytes));
            out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footerBytes.length).flip());
            out.write(ByteBuffer.wrap(MAGIC));
            out.close();
            return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())));
        }
    }
}
